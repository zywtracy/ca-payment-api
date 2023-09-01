import nodeFetch from "node-fetch";
import {QryDB} from "../model/query";
import {ACHPayType, Queue, ReusableMethods} from "../resuable_component";
import fetch from "node-fetch";
import { AbortController } from 'abort-controller';
import path from "path";
const jwt = require('jsonwebtoken');
const https = require('https');
const fs = require('fs');

const log = require("../log");
const reusable = new ReusableMethods();
let {createPool} = require("../model/psqlAPM");

let qryDb = new QryDB();
let isDBConnected = false;
let isCheckDBRunning = false;
let branchServiceAuthKey: string;
let isGetBranchAuthKeyRunning = false;
let portalApiTwilioAuthKey:string;
let restapiKey:string;
let portalApiURL:string;
let restapiURL:string;
let branchWebhookDict = new Map(); //this is to avoid duplicate sms/email for an event already generated.
let achKeys ={ach_program_id_deliveries:'',ach_program_id_tips:'',ach_program_id_avs:'',ach_onboard_url:'', ach_webhook_crt_file:'', achpemfile_for_payment:'',achPayThreds:1,achPayRateLimitPerMin:100,jpm_ach_passphrase:'',ach_pfx_file:''};
let achPayQ = new Queue();
let intervalACHWeeklyService: NodeJS.Timer;
let weeklyThreadRunning:boolean[] = [];
// let delayTimeMs = Math.ceil(60*1000/achKeys.achPayRateLimitPerMin/achKeys.achPayThreds)
let achPFXFile:any;
let httpsAgent:any;
let payErrorCode = new Map();
let intervalSendToLogACHQStatus:any;
let achQEmpty = 0;
let weeklyPayoutRecd:number = 0;
let weeklyPayoutProcessed:number = 0;
let weeklyProcessedAmt:number = 0;
let weeklyTransactions: any[] = [];
let isJobRunning = false;
setInterval(()=>{
    //every week clear the dictionary of webhooks of branch
    log.logger("info",encodeURIComponent(`${branchWebhookDict.size} dictionary elements cleared`));
    branchWebhookDict.clear();
},7*24*60*60*1000)


export class QryController {
    constructor(){}

    async getSecretValue() {
        try {
            let param = {
                "secrets": ['database', 'port', 'user', 'password', 'host', 'ssl']
            }
            // let url = "http://localhost:5600/v2/getAllSecrets";
            let url = process.env.KEY_VAULT_URL + '/v2/getAllSecrets';
            let response = await fetch(url, {
                method: 'POST',
                body: JSON.stringify(param),
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': process.env.AZURE_AUTHORIZATION_TOKEN || ''
                }
            });
            if (response.status == 200) {
                let result = await response.json();
                if (result.success) {
                    result.result['max'] = 50;
                    result.result['idleTimeoutMillis'] = 300000;
                    await createPool(result.result);
                    log.logger("info", encodeURIComponent(`Azure Key vault service :: SUCCESS.`));
                    return this.checkDb();
                } else {
                    log.logger("error", encodeURIComponent(`Azure Key vault service :: Failed <> ${result.message}.`));
                    return false;
                }
            } else {
                log.logger("error", encodeURIComponent(`Azure Key vault service :: Failed for URL ${url} <> status: ${response.status} <> statusText: ${response.statusText}`));
                setTimeout(() => {
                    this.getSecretValue();
                }, 10000);
                return false;
            }
        } catch (error:any) {
            log.logger("error", encodeURIComponent(`getSecretValue(), Azure KeyValuts Error ${error.message}, trying to connect in 10 sec`));
            setTimeout(() => {
                this.getSecretValue();
            }, 10000);
            return false;
        }
    }

    async checkDb() {
        const result = await qryDb.checkDB();
        if (result.success) {
            isDBConnected = true;
            isCheckDBRunning = false;
            console.log("DBConnected", isDBConnected);
            log.logger("info",encodeURIComponent(`DB Connected ${new Date().toLocaleString()}`));
            return true;
        }
        else {
            isDBConnected = false;
            isCheckDBRunning = true;
            log.logger("error", encodeURIComponent(`checkDb(),new Date() PSQL Connection Error ${result.message}, trying to connect in 10 sec`));
            setTimeout(() => {
                this.checkDb();
            }, 10000);
            return false;
        }
    }

    isDBConnected() {
        if (!isDBConnected && !isCheckDBRunning) {
            this.checkDb();
        }
        return isDBConnected;
    }

    async getBranchKeys() {
        try {
            const result: any = await qryDb.getBranchKeys();
            if (result.success) {
                return { success: true, rowCount: result.rowCount, result: result.rows };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    async insBranchWebhook(param:any) {
        try {
            const result: any = await qryDb.insBranchWebhook(param);
            if (result.success) {
                return { success: true, rowCount: result.rowCount, result: result.rows };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    async insPartnerPayoutProcessor(param:any) {
        try {
            let unqID;
            if (param.event == "ORGANIZATION_INITIALIZED_ACCOUNT_CREATED" || param.event == "WALLET_CREATED"){
                param["data"]["status"] = "CREATED";
                param["data"]["is_active"] = false;
                unqID = param.data.employee_id+'CREATED';
            }
            else if (param.event == "WALLET_CLAIMED" || param.event == "ACCOUNT_VERIFIED_AS_WORKER"){
                param["data"]["status"] = "ACTIVE";
                param["data"]["is_active"] = true;
                unqID = param.data.employee_id+'ACTIVE';
                if (!branchWebhookDict.get(unqID)){
                    getSMSEmailTemplateBranchApproval(param);
                    // qryDb.insCandidateProgress(param.data.employee_id, "PAYMENT_AGREEMENT","COMPLETE");
                }
            }
            else if (param.event == "WALLET_ACTIVATED"){
                param["data"]["status"] = "UNCLAIMED";
                param["data"]["is_active"] = false;
                unqID = param.data.employee_id+'UNCLAIMED';
                if (!branchWebhookDict.get(unqID)){
                    // qryDb.insCandidateProgress(param.data.employee_id, "PAYMENT_AGREEMENT","COMPLETE");
                    getSMSEmailTemplateBranchWallet(param);
                }
            }
            else if (param.event == "ACCOUNT_REVIEW"){
                param["data"]["status"] = "REVIEW";
                param["data"]["is_active"] = false;
                unqID = param.data.employee_id+'REVIEW';
            }
            else if (param.event == "WALLET_DEACTIVATED"){
                param["data"]["status"] = "CLOSED";
                param["data"]["is_active"] = false;
                unqID = param.data.employee_id+'CLOSED';
                addWithdrawnStatus({ partnerID: param.data.employee_id });
                if (!branchWebhookDict.get(unqID)){
                    // qryDb.insCandidateProgress(param.data.employee_id, "PAYMENT_AGREEMENT","WITHDRAWN");
                    let payeeID = await qryDb.getPayeeId(param.data.employee_id);
                    if (payeeID.success){
                        if (payeeID.rowCount > 0 && payeeID.rows[0].externalID == undefined && (payeeID.rows[0].seq < 300 || payeeID.rows[0].seq > 330)){
                            log.logger("info", encodeURIComponent(`Payee id does not exist for partner ${param.data.employee_id} and in last status ${payeeID.rows[0].status_name} hence mail is sent for disqualified status ${param.data.status}`));
                            getSMSEmailTemplateDisqualified(param);
                        }
                        else {
                            if (payeeID.rowCount > 0){
                                log.logger("info", encodeURIComponent(`Partner ${param.data.employee_id} is having payee id or driver is in last status ${payeeID.rows[0].status_name} and hence mail not sent for disqualified status ${param.data.status} payee id - ${payeeID.rows[0].externalID}, last application status - ${ payeeID.rows[0].status_name}`));    
                            }
                            else {
                                log.logger("info", encodeURIComponent(`No clp data found for partner ${param.data.employee_id} and hence mail not sent for disqualified for status ${param.data.status}. Impossible event`));
                            }
                        }
                    }
                    else {
                        log.logger("info", encodeURIComponent(`Could not get the payee id hence not processing the email for partner ${param.data.employee_id} with status ${param.data.status}`));
                    }
                }
            }
            else {
                param["data"]["is_active"] = false;
                param["data"]["status"] = param.event;
                unqID = param.data.employee_id+param.event;
            }
            if (!branchWebhookDict.get(unqID)){
                branchWebhookDict.set(unqID,"updated");    
                const result: any = await qryDb.insParterPayoutProcessor(param.data);
                if (result.success) {
                    log.logger("info", encodeURIComponent(`Partner Payout Processor updated for partner ${param.data.employee_id} with status ${param.data.status}`));
                }
                else {
                    log.logger("error", encodeURIComponent(`Partner Payout Processor update failed with error ${result.messaage} for partner ${param.data.employee_id} with status ${param.data.status},${JSON.stringify(param)}`));
                }
            }
            else {
                log.logger("info", encodeURIComponent(`Duplicate webhook for partner ${param.data.employee_id} and event ${param.event}`))
            }
        }
        catch (e: any) {
            log.logger("error", encodeURIComponent(`Partner Payout Processor update failed with exception ${e.messaage} for partner ${param.data.employee_id} with status ${param.data.status}`));
            return { success: false, message: e.message };
        }
    }

    async getBranchServiceAuthKey() {
        let result = await qryDb.getBranchUserHash();
        if (result.success && result.rowCount > 0){
            console.log("Branch Keys loaded");
            branchServiceAuthKey = result.rows[0].phash;
            log.logger("info",encodeURIComponent(`Branch Service key loaded`));
            isGetBranchAuthKeyRunning = false;
        }
        else {
            log.logger("error",encodeURIComponent(`Branch Service Auth key not found, will retry every 10 Sec`));
            isGetBranchAuthKeyRunning = true;
            setTimeout(() => {
                this.getBranchServiceAuthKey();
            }, 10000);
        }
    };

    getBranchAuthKey() {
        if (!branchServiceAuthKey && !isGetBranchAuthKeyRunning) {
            this.getBranchServiceAuthKey();
        }
        return branchServiceAuthKey;
    }

    async getTwilioUserDetails() {
        try {
            const result: any = await qryDb.getTwilioUserDetails();
            if (result.success && result.rowCount > 0) {
                portalApiTwilioAuthKey = result.rows[0].phash;
                log.logger("info", encodeURIComponent(`Portal API Twilio key loaded`));
                console.log("Portal API Twilio key loaded.")
            }
            else {
                log.logger("info", encodeURIComponent(`Portal API Twilio Auth key not found, will retry in next 10 sec`))
                setTimeout(() => {
                    this.getTwilioUserDetails();
                }, 10000);
            }
        }
        catch (e: any) {
            log.logger("error", encodeURIComponent(`getTwilioUserDetails() failed with exception ${e.message} and ${JSON.stringify(e)}`));
        }
    }

    async getRestapiKey() {
        try {
            const result: any = await qryDb.getRestapiKey();
            if (result.success && result.rowCount > 0) {
                restapiKey = result.rows[0].phash;
                log.logger("info", encodeURIComponent(`Restapi API key loaded`));
                console.log("Restapi API key loaded.")
            }
            else {
                log.logger("info", encodeURIComponent(`Restapi API key not found, will retry in next 10 sec`))
                setTimeout(() => {
                    this.getRestapiKey();
                }, 10000);
            }
        }
        catch (e: any) {
            log.logger("error", encodeURIComponent(`getRestapiKey() failed with exception ${e.message} and ${JSON.stringify(e)}`));
        }
    }

    async getPortalApiURL(){
        try {
            const result: any = await qryDb.getPortalApiURL();
            if (result.success && result.rowCount > 0) {
                portalApiURL = result.rows[0].value;
                log.logger("info", encodeURIComponent(`Portal API URL loaded`));
                console.log("Portal API URL loaded.")
            }
            else {
                log.logger("info", encodeURIComponent(`Portal API URL not found, will retry in next 10 sec`))
                setTimeout(() => {
                    this.getPortalApiURL();
                }, 10000);
            }
        }
        catch (e: any) {
            log.logger("error", encodeURIComponent(`getPortalApiURL() failed with exception ${e.message} and ${JSON.stringify(e)}`));
        }
    }

    async getRestapiURL(){
        try {
            const result: any = await qryDb.getRestapiURL();
            if (result.success && result.rowCount > 0) {
                restapiURL = result.rows[0].value;
                log.logger("info", encodeURIComponent(`Restapi API URL loaded`));
                console.log("Restapi API URL loaded.")
            }
            else {
                log.logger("info", encodeURIComponent(`Restapi API URL not found, will retry in next 10 sec`))
                setTimeout(() => {
                    this.getRestapiURL();
                }, 10000);
            }
        }
        catch (e: any) {
            log.logger("error", encodeURIComponent(`getRestapiURL() failed with exception ${e.message} and ${JSON.stringify(e)}`));
        }
    }

    async getACHKeys(){
        try {
            const result: any = await qryDb.getACHKeys();
            if (result.success && result.rowCount > 0) {
                result.rows.map((row:any)=>{
                    if (row.key == 'ach_program_id_deliveries') achKeys.ach_program_id_deliveries = row.value;
                    else if (row.key == 'ach_program_id_tips') achKeys.ach_program_id_tips = row.value;
                    else if (row.key == 'ach_program_id_deliveries') achKeys.ach_program_id_avs = row.value;
                    //achpfxfile
                })
                log.logger("info", encodeURIComponent(`ACH Keys loaded`));
                console.log("ACH Keys loaded.");
            }
            else {
                log.logger("info", encodeURIComponent(`ACH Keys not found, will retry in next 10 sec`))
                setTimeout(() => {
                    this.getACHKeys();
                }, 10000);
            }
            const result1: any = await qryDb.getACHURL();
            if (result1.success && result1.rowCount > 0) {
                result1.rows.map((row:any)=>{
                    if (row.key == 'ACH_ONBOARD_URL') achKeys.ach_onboard_url = row.value;
                    else if (row.key == 'ach_webhook_crt_file') achKeys.ach_webhook_crt_file = row.value;
                    else if (row.key == 'achpemfile') achKeys.achpemfile_for_payment = row.value; 
                    else if (row.key == 'ACH_PAY_THREADS') achKeys.achPayThreds = row.value; 
                    else if (row.key == 'JPM_ACH_PASSPHRASE') achKeys.jpm_ach_passphrase = row.value;
                    else if (row.key == 'ACH_PAY_RATE_LIMIT_PER_MIN') achKeys.achPayRateLimitPerMin = row.value;
                    else if (row.key == 'achpfxfile') achKeys.ach_pfx_file = row.value;
                });
                httpsAgent = new https.Agent({
                    pfx: Buffer.from(achKeys.ach_pfx_file,'base64'),
                    passphrase: achKeys.jpm_ach_passphrase
                });
                log.logger("info", encodeURIComponent(`ACH Keys, certificates loaded`));
                console.log("ACH Keys & certificates loaded.");
            }
            else {
                log.logger("info", encodeURIComponent(`ACH Payment URL,certificate not found, will retry in next 10 sec`))
                setTimeout(() => {
                    this.getACHKeys();
                }, 10000);
            }
            const result2:any = await qryDb.getACHErrorCodes();
            if (result2.success){
                result2.rows.map((row:any)=>{
                    payErrorCode.set(row.error_code,row);
                });
                log.logger("info", encodeURIComponent(`ACH error code loaded.`));
            }
            else {
                log.logger("info", encodeURIComponent(`ACH Payment error codes not found, will retry in next 60 sec`));
                setTimeout(() => {
                    this.getACHKeys();
                }, 60000);
            }
        }
        catch (e: any) {
            log.logger("error", encodeURIComponent(`getACHKeys() failed with exception ${e.message} and ${JSON.stringify(e)}`));
        }
    }

    async getStartTimeDiffInms(){
        let result = await qryDb.getStartTimeDiffInms();
        if (result.success){
            return {success:true, delay_in_ms: result.rows[0].interval_in_sec}
        }
        else {
            return {success:false};
        }
    }

    async getWeeklyPayoutACH(payOutDate:string){
        try {
            const result: any = await qryDb.getWeeklyPayoutACH(payOutDate);
            if (result.success) {
                return { success: true, rowCount: result.rowCount, result: result.rows };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    async getWeeklyPayoutACHEarnings(payOutDate:string){
        try {
            const result: any = await qryDb.getWeeklyPayoutACHEarnings(payOutDate);
            if (result.success) {
                return { success: true, rowCount: result.rowCount, result: result.rows };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    async insWeeklyProcessLog(param:any){
        try {
            const result: any = await qryDb.insWeeklyProcessLog(param);
            if (result.success) {
                return { success: true, rowCount: result.rowCount, result: result.rows };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    setAndgetACHKeys(){
        return achKeys;
    }

    async sendToACHPayService(instantReq: string, transactionCreatedOn:string, payout?:string, logID?:string){
        let param:ACHPayType = JSON.parse(decodeURIComponent(instantReq));
        let payoutDet;
        if (payout) {
            payoutDet = JSON.parse(decodeURIComponent(payout));
            weeklyPayoutRecd ++;
        }
        achPayQ.enqueue({achData:param, tcn: transactionCreatedOn, payout:payoutDet, logID:logID});
        if (!intervalACHWeeklyService){
            sendToLogACHQStatus();
            startACHWeeklyPayment();
        }
    }

    async validateWebhook(payload:any){
        return validateWebhookToken(payload)
    }

    async insACHWebhookRaw(param:any){
        try {
            // '{"data": {"status": "Returned", "currency": "USD", "createdBy": null, "valueDate": "2023-07-17", "endToEndId": null, "creditOrDebit": "CREDIT", "effectiveDate": "2023-07-14", "paymentAmount": 56, "rejectDetails": null, "returnDetails": {"returnDate": null, "returnAmount": 56, "returnReasonCode": null, "returnReasonDescription": null}, "createDateTime": "2023-07-14T08:24:04", "externalAccount": {"card": null, "bankAccount": {"accountNumberLastFour": "4384"}, "externalAccountAliasId": "a519ae35001c4f05a65c73bb96fe2d61"}, "counterpartyUser": {"counterpartyUserName": "DefaultUser null", "counterpartyUserAliasId": "18d2eb37-c67d-48d4-a8de-932687d0e8d1", "counterpartyUserEmailAddress": "heenajasing@gmail.com"}, "confirmationNumber": "483278447045588", "counterpartyEntity": {"counterpartyEntityName": null, "counterpartyEntityAliasId": "18d2eb37-c67d-48d4-a8de-932687d0e8d1"}, "methodOfPaymentType": "ACH", "invoicePayableNumbers": ["49d7264f-f4be-418d-9909-89468e64f83b#5"], "transactionReferences": null, "methodOfPaymentSubType": "US_ACH_NEXT_DAY", "unregisteredExternalAccount": null}, "type": "payment.returned", "eventId": "3adfb730-ff16-4c8e-a02e-fe92a8f089ec", "programId": "5102566184831", "createdTime": "2023-07-17T22:16:11"}'
            let data ={
                req_encoded: param.body,
                req_decoded:JSON.stringify(param.decoded),
                driver_id: param.decoded.data?.counterpartyEntity?.counterpartyEntityAliasId,
                program_id:param.decoded.data?.programId,
                confirmation_number:param.decoded.data?.confirmationNumber,
                payment_status:param.decoded.data?.status,
                req_success:true,
                response_sent:JSON.stringify({success:true,message:'ok'}),
                payment_created_time:param.decoded?.data?.createDateTime,
                payable_number: param?.decoded?.data?.invoicePayableNumbers[0],
                return_reason_code: param.decoded?.data?.returnDetails?.returnReasonCode,
                return_reason_description:param.decoded?.data?.returnDetails?.returnReasonDescription,
                return_date: param.decoded?.data?.returnDetails?.returnDate,
                remarks: param.decoded?.data?.returnDetails?.returnReasonDescription
            }
            let result ={success:true}; //await qryDb.insACHWebhookRaw(data);
            if (result.success){
                log.logger("info", encodeURIComponent(`Processed successfully webhook to raw table for confirmation Number ${data.confirmation_number}`));
                let p = {
                    payment_status: data.payment_status,
                    payable_number: data.payable_number,
                    settlement_date: data.payment_created_time,
                    confirmation_number: data.confirmation_number,
                    return_reason_code: data.return_reason_code,
                    return_reason_description: data.return_reason_description,
                    return_date: data.return_date,
                    remarks: data.remarks
                }
                let msg = ''
                let resp = await qryDb.updACHPaidStatus(p);
                if (resp.success){
                    log.logger("info", encodeURIComponent(`Updated successfully payment status to paid transaction table for payable_number Number ${data.payable_number}`));
                    msg += 'ACH status updated to paid transactions table'
                }
                else {
                    log.logger("error", encodeURIComponent(`Update of payment status failed to update paid transaction table for payable_number ${data.payable_number} JSON.stringify(p)`));
                    msg += `ACH status update failed with message ${resp.message}`;
                }
                if (data.payment_status == 'Returned'){
                    // to update the hold status to all transactions in transaction table.
                    let updTrans = {
                        payable_number: data.payable_number,
                        remarks: p.return_reason_code+" "+p.return_reason_description+" "+p.return_date
                    }
                    let uhst = await qryDb.updHoldStatusToTransactions(updTrans)
                    if (uhst.success){
                        log.logger("info", encodeURIComponent(`Updated successfully hold status to transactions table for payable_number Number ${data.payable_number} and its transactions`));
                        msg += ' Status hold updated to transactions'
                    }
                    else {
                        log.logger("error", encodeURIComponent(`Update of payment status failed to update ach_paid_transactions table for payable_number ${data.payable_number} JSON.stringify(updTrans)`));
                        msg += ` Hold status update failed with message ${uhst.message}`;
                    }    
                    let updDpp = {
                        driver_id: data.driver_id
                    }
                    let updLfo = await qryDb.updLastFailedDate(updDpp);
                    if (updLfo.success){
                        log.logger("info", encodeURIComponent(`Updated successfully last failed on status to driver_payment_provider table for driver ${data.driver_id}`));
                        msg += ` last failed on updated to driver_payment_provider for driver ${data.driver_id}`
                    }
                    else {
                        log.logger("error", encodeURIComponent(`Update of last failed on status failed to update driver payment provider table for driver ${data.driver_id} emessage ${updLfo.message}`));
                        msg += ` failed to update last_failed_on to driver_payment_provider for driver ${data.driver_id}`;
                    }
                    return {success:true,message: msg};
                }
                else {
                    return {success:true, message: 'Successfully processed the webhook'};
                }
            }
            else {
                log.logger("error", encodeURIComponent(`Could not insert the webhook ${JSON.stringify(data)}`));
                //communication must go to devops and db admin and dev ahead.
                return {success:false, message:'Unknown service error, retry again after some time'};
            }    
        }
        catch (e:any) {
            log.logger("error", encodeURIComponent(`insACHWebhookRaw exception ${e.message} ${JSON.stringify(e.stack)} ${JSON.stringify(param.decoded)}`));
            return {success:false, message:'Unknown service error, retry again after some time'};
        }
    }

    resetWeeklyCnt(){
        weeklyPayoutProcessed = 0;
        weeklyPayoutRecd = 0;
        weeklyProcessedAmt = 0;
        weeklyTransactions = [];
    }

    getWeeklyPayRateLimit(){
        return achKeys.achPayRateLimitPerMin;
    }

    async getNxtInvRef(){
        try {
            const result: any = await qryDb.getNxtInvRef();
            if (result.success) {
                return { success: true, rowCount: result.rowCount, result: result.rows };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    async getCurrentPSTDate(){
        try {
            const result: any = await qryDb.getCurrentPSTDate();
            if (result.success && result.rowCount > 0) {
                return { success: true, rowCount: result.rowCount, curDate: result.rows[0].curr_date };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    async getCurrentPSTDateEarnings(){
        try {
            const result: any = await qryDb.getCurrentPSTDateEarnings();
            if (result.success && result.rowCount > 0) {
                return { success: true, rowCount: result.rowCount, curDate: result.rows[0].curr_date };
            }
            else {
                return { success: false, message: result.message };
            }
        }
        catch (e: any) {
            return { success: false, message: e.message };
        }
    }

    async insSchedulerLog(param:any){
        const result = await qryDb.insSchedulerLog(param);
        if (result.success){
            return {success:true, result:result.rows, rowCount:result.rowCount};
        }
        else {
            return {success:false, message:result.message};
        }
    
    }
    async insSchedulerLogDetails(param:any){
        const result = await qryDb.insSchedulerLogDetails(param);
        if (result.success){
            return {success:true, message:"Successfully inserted the schedulerLogDetails"};
        }
        else {
            return {success:false, message:result.message};
        }
    }
    
    async insertScheduleLogDetails(id:string,message:string){
        let p = {
            scheduler_log_id: id,
            message_details: message
        };
        let result = await this.insSchedulerLogDetails(p);
        if (!result.success){
            log.logger("error", encodeURIComponent(`Failed to insert scheduler log details with message ${result.message}`));
        }
    }
}

let qryCntrl = new QryController();

function startACHWeeklyPayment(){
    for (let i = 0; i<achKeys.achPayThreds; i++){
        weeklyThreadRunning.push(false);
    }
    if (!intervalACHWeeklyService){
        intervalACHWeeklyService = setInterval(()=>{
            drainACHPayQ();
        },10000);
    }
}

function drainACHPayQ(idx?:any){
    if (idx){
        if (!weeklyThreadRunning[idx] && achPayQ.getLength()>0) postACHWeeklyPayment(achPayQ.dequeue(),idx);
    }
    else {
        for (let i = 0; i< achKeys.achPayThreds; i++){
            if (!weeklyThreadRunning[i] && achPayQ.getLength()>0) postACHWeeklyPayment(achPayQ.dequeue(),i);
        }
    }
}

function sendToLogACHQStatus(){
    if (!intervalSendToLogACHQStatus) {
        intervalSendToLogACHQStatus = setInterval(()=>{
            log.qlog("info", encodeURIComponent(`Current ACH Weekly payment Queue length is ${achPayQ.getLength()}`));
            if (achPayQ.getLength() == 0){
                achQEmpty += 1;
                if (achQEmpty >= 10){
                    clearInterval(intervalSendToLogACHQStatus);
                }
            }
        }, 60*1000)
    }
}

async function postACHWeeklyPayment(dat:any, ix:number) {
    let param:ACHPayType = dat.achData;
    let tnc = dat.tcn;
    let pt = dat.payout;
    let logID = dat.logID;
    weeklyThreadRunning[ix] = true; 
    let url = achKeys.ach_onboard_url+'/payables';
    //for request timeout if no response for 30 sec from ACH
    const controller = new AbortController();
    const timeout = setTimeout(() => {
        controller.abort();
    }, 30000);
    try {
        const response = await nodeFetch(url,
            {
                method: "post",
                body: jwtSignPayment(param),
                headers: {
                    "Content-Type": "text/xml",
                },
                agent: httpsAgent,
                signal: controller.signal
            }
        );
        let data: any;
        log.logger("info",encodeURIComponent(`${param.payableNumber} response received with status ${response.status} & ${response.statusText}`));
        try{
            let resp = await response.json();
            data = {
                ach_weekly_process_log_id : logID,
                payable_number: param.payableNumber,
                driver_id: pt.partner_id,
                payable_key_value: param.payableReferences[0].payableKeyValue,
                payout_id: pt.payout_id,
                settlement_item_id: pt.settlementItemID,
                program_id: param.programId,
                mapping_key: pt.mappingKey,
                payable_amount: param.payableAmount,
                confirmation_number: resp?.data?.confirmationNumber,
                payment_created_time:resp.createdTime,
                req_content: param,
                resp: resp,
                resp_status_code:response.status,
                resp_status_text: response.statusText,
                resp_error_code:resp?.errors ? resp?.errors[0]?.errorCode:undefined,
                resp_error_msg: resp?.errors ? resp?.errors[0]?.errorMsg: undefined,
                remarks: undefined
            };
            let result = await qryDb.insACHPaymentRaw(data);
            if (result.success){
                log.logger("info", encodeURIComponent(`${data.payable_number} & ${result.rows[0].id}  is successfully paid and inserted into ach payment raw table`));
            }
            else {
                log.logger("error", encodeURIComponent(`${data.payableNumber} is successfully processed but failed to insert into ach payment raw table ${JSON.stringify(data)} `));
            }
            if (response.status == 201){
                //insert into paid transaction table && update transactions as paid
                let paidT = {
                    ach_weekly_process_log_id:logID,
                    ach_payment_raw_id: result.rows[0].id,
                    payable_number: param.payableNumber,
                    confirmation_number:resp?.data?.confirmationNumber,
                    transaction_created_on: tnc,
                    payment_status:'created',
                    driver_id:pt.partner_id,
                    payout_id:pt.payout_id,
                    settlement_item_id:pt.settlementItemID,
                    mapping_key:pt.mappingKey,
                    pay_type: pt.mappingKey == '3' ? 'Tips' : 'Earnings',
                    actual_pay_type:pt.name,
                    program_id:param.programId,
                    payable_amount: param.payableAmount,
                    settlement_date: resp.createdTime,
                    remarks: undefined,
                    ach_user_id: param.payableReferences[0].payableKeyValue
                };
                weeklyPayoutProcessed ++;
                weeklyProcessedAmt += Number(param.payableAmount);
                weeklyTransactions = weeklyTransactions.concat(pt.transaction_ids);
                let insPT = await qryDb.insACHPaidTransactions(paidT);
                if (insPT.success){
                    let p ={
                        ach_paid_transaction_id: insPT.rows[0].id,
                        transaction_ids: pt.transaction_ids
                    }
                    let ptd = await qryDb.insACHPayoutTransactions(p);
                    if (ptd.success) log.logger("info", encodeURIComponent(`Successfully inserted to ach paid transaction details table for payout ${paidT.payable_number}`));
                    else log.logger("error", encodeURIComponent(`failed to insert into ach_paid_transaction_details for payout ${paidT.payable_number}`));

                    let updDt = {
                        transaction_ids:pt.transaction_ids,
                        status:'PAID'
                    };
                    let updTrans = await qryDb.updTransactionPaid(updDt);
                    if (updTrans.success) log.logger("info", encodeURIComponent(`Successfully updated the paid status to transactions table for payout ${paidT.payable_number} and transactions ${JSON.stringify(pt.transaction_ids)}`));
                    else log.logger("error", encodeURIComponent(`failed to update the paid status to transactions table for payout ${paidT.payable_number} and transactions ${JSON.stringify(pt.transaction_ids)}`));
                }
                else {
                    log.logger("error", encodeURIComponent(`Failed to insert into ach_paid_transactions table for payout ${paidT.payable_number} to insert ${JSON.stringify(paidT)} error message ${insPT.message}`));
                }
            }
            else if (data.resp_error_code == 'R0210'){
                weeklyPayoutProcessed ++;
                weeklyProcessedAmt += Number(param.payableAmount);
                weeklyTransactions = weeklyTransactions.concat(pt.transaction_ids);
                // update transactions table with PAID status due to duplicate payment tried.
                let updDt = {
                    transaction_ids:pt.transaction_ids,
                    status:'PAID',
                    remarks: `${data.resp_error_msg}`
                };
                let updTrans = await qryDb.updTransactionPaidWithRemarks(updDt);
                if (updTrans.success) log.logger("info", encodeURIComponent(`Successfully updated to PAID status to the transactions table for payout ${pt.payout_id} and transactions ${JSON.stringify(pt.transaction_ids)}`));
                else log.logger("error", encodeURIComponent(`failed to update the paid status to the transactions table for payout ${pt.payout_id} and transactions ${JSON.stringify(pt.transaction_ids)}`));
            }
            else {
                let errDetails = payErrorCode.get(data.resp_error_code);
                if (errDetails.is_retriable){
                    setTimeout(() => {
                        log.logger("error", encodeURIComponent(`ACH payment failed for ${data.payout_id} with error ${errDetails.error_code} and ${errDetails.error_msg}, will be retried in next 60 sec.`));
                        achPayQ.enqueue(dat);
                    }, 60000);
                }
                else {
                    weeklyPayoutProcessed ++;
                    weeklyProcessedAmt += Number(param.payableAmount);
                    weeklyTransactions = weeklyTransactions.concat(pt.transaction_ids);
                    // update transactions table with hold status due to bad request, require user intervention to pay this request.
                    let updDt = {
                        transaction_ids:pt.transaction_ids,
                        status:'HOLD',
                        remarks: `ACH payment failed with error ${data.resp_error_msg}`
                    };
                    let updTrans = await qryDb.updTransactionPaidWithRemarks(updDt);
                    if (updTrans.success) log.logger("info", encodeURIComponent(`Successfully updated the HOLD status to transactions table for payout ${pt.payout_id} and transactions ${JSON.stringify(pt.transaction_ids)}`));
                    else log.logger("error", encodeURIComponent(`failed to update the hold status to transactions table for payout ${pt.payout_id} and transactions ${JSON.stringify(pt.transaction_ids)}`));
                }
            }
        }
        catch(e:any){
            log.logger("error", encodeURIComponent(`Exception while processing response ${e.message} ${JSON.stringify(e.stack)}`));
            // when extraction of JSON body from response fails (receives static text (502 - bad gate way error)) which cannot be converted by response.json();
            data = {
                ach_weekly_process_log_id : logID,
                payable_number: param.payableNumber,
                driver_id: pt.partner_id,
                payable_key_value: param.payableReferences[0].payableKeyValue,
                payout_id: pt.payout_id,
                settlement_item_id: pt.settlementItemID,
                program_id: param.programId,
                mapping_key: pt.mappingKey,
                payable_amount: param.payableAmount,
                req_content: param,
                resp_status_code:response.status,
                resp_status_text: response.statusText,
                remarks: 'Invalid Response Exception, will be re-tried'
            };
            let result = await qryDb.insACHPaymentRaw(data);
            if (result.success){
                log.logger("info", encodeURIComponent(`${data.payable_number} is failed and inserted into ach payment raw table`));
            }
            else {
                log.logger("error", encodeURIComponent(`${data.payable_number} is failed and failed to insert into ach payment raw table ${JSON.stringify(data)}`));
            }
            setTimeout(() => {
                achPayQ.enqueue(dat);
            }, 30000);
        }
    }
    catch (e: any) {
        let dt = {
            ach_weekly_process_log_id : logID,
            payable_number: param.payableNumber,
            driver_id: pt.partner_id,
            payable_key_value: param.payableReferences[0].payableKeyValue,
            payout_id: pt.payout_id,
            settlement_item_id: pt.settlementItemID,
            program_id: param.programId,
            mapping_key: pt.mappingKey,
            payable_amount: param.payableAmount,
            req_content: param,
            resp_status_text: e.message,
            remarks: 'Timeout Exception, will be re-tried'
        };
        let result = await qryDb.insACHPaymentRaw(dt);
        if (result.success){
            log.logger("info", encodeURIComponent(`${dt.payable_number} is failed and inserted into ach payment raw table`));
        }
        else {
            log.logger("error", encodeURIComponent(`${dt.payable_number} is failed and failed to insert into ach payment raw table ${JSON.stringify(dt)}`));
        }
        log.logger("error", encodeURIComponent(`postACHWeeklyPayment Exception ${param.payableNumber} ${e.message}, ${JSON.stringify(e.stack)} will be retried in next 60 sec`));
        setTimeout(() => {
            achPayQ.enqueue(dat);
        }, 60000);
    }
    clearTimeout(timeout);
    weeklyThreadRunning[ix] = false;
    drainACHPayQ(ix);
    if (weeklyPayoutProcessed == weeklyPayoutRecd){
        // process deposit summary, update is_ds_processed status to true in transactions table
        weeklyPayoutProcessed = 0;
        weeklyPayoutRecd = 0;
        weeklyProcessedAmt = 0;
        weeklyTransactions = [];
    }

}

async function getSMSEmailTemplateDisqualified(param:any){
    let result = await qryDb.getSMSEmailTemplateDisqualified();
    if (result.success && result.rowCount > 0){
        let personDetails = await qryDb.getPartnerDetails(param.data);
        if (personDetails.success && personDetails.rowCount > 0){
            let html = result.rows[0].richText;
            let smsText = result.rows[0].smsText;
            let subject = result.rows[0].subject;
            let phone = personDetails.rows[0].phone;
            let email = personDetails.rows[0].email;
            let name = personDetails.rows[0].firstName+" "+personDetails.rows[0].lastName;
            if (smsText && phone){
                let modifiedText = smsText.replace("{{name}}", name);
                sendSMSText({phone:phone, text:modifiedText, partnerID:personDetails.rows[0].partnerID,userID: personDetails.rows[0].userID});
            }
            else {
                log.logger("info", encodeURIComponent(`smsText & phone number does not exist for partner ${personDetails.rows[0].partnerID}, hence sms not sent. Template used is disqualified`));
            }
            if (html && email){
                let modifiedHtml = html.replace("{{name}}", name);
                let dat = {
                    to : email, 
                    from: 'Spark-Driver@email.wal-mart.com', 
                    subject: subject,
                    html: modifiedHtml,
                    partnerID: personDetails.rows[0].partnerID,
                    userID: personDetails.rows[0].userID,
                }
                sendEmail(dat);
            }    
            else {
                log.logger("info", encodeURIComponent(`html & email does not exist for partner ${personDetails.rows[0].partnerID}, hence email not sent. Template used is Branch Wallet`));
            }
        }
        else {
            log.logger("error",encodeURIComponent(`Partner details not available for partner ID ${JSON.stringify(param)}`))
        }
    }

}

async function getSMSEmailTemplateBranchWallet(param:any){
    let result = await qryDb.getSMSEmailTemplateBranchWallet();
    if (result.success && result.rowCount>0){
        let personDetails = await qryDb.getPartnerDetails(param.data);
        if (personDetails.success && personDetails.rowCount > 0){
            let html = result.rows[0].richText;
            let smsText = result.rows[0].smsText;
            let subject = result.rows[0].subject;
            let phone = personDetails.rows[0].phone;
            let email = personDetails.rows[0].email;
            let name = personDetails.rows[0].firstName+" "+personDetails.rows[0].lastName;
            if (smsText && phone){
                let modifiedText = smsText.replace("{{name}}", name).replace("{{magicLink}}",param.data.onboarding_link).replace('[INSERTCUSTOMLINK]', param.data.onboarding_link);
                sendSMSText({phone:phone, text:modifiedText, partnerID:personDetails.rows[0].partnerID,userID: personDetails.rows[0].userID});
            }
            else {
                log.logger("info", encodeURIComponent(`smsText & phone number does not exist for partner ${personDetails.rows[0].partnerID}, hence sms not sent. Template used is Branch Wallet`));
            }
            if (html && email){
                let modifiedHtml = html.replace("{{name}}", name).replace("{{magicLink}}",param.data.onboarding_link).replace('{{[INSERTCUSTOMLINK]}}', param.data.onboarding_link);
                let dat = {
                    to : email, 
                    from: 'Spark-Driver@email.wal-mart.com', 
                    subject: subject,
                    html: modifiedHtml,
                    partnerID: personDetails.rows[0].partnerID,
                    userID: personDetails.rows[0].userID,
                }
                sendEmail(dat);
            }    
            else {
                log.logger("info", encodeURIComponent(`html & email does not exist for partner ${personDetails.rows[0].partnerID}, hence email not sent. Template used is Branch Wallet`));
            }
        }
        else {
            log.logger("error",encodeURIComponent(`Partner details not available for partner ID ${param.data.employee_id}`))
        }
    }

}

async function getSMSEmailTemplateBranchApproval(param:any){
    let result = await qryDb.getSMSEmailTemplateBranchApproved();
    if (result.success && result.rowCount>0){
        let personDetails = await qryDb.getPartnerDetails(param.data);
        if (personDetails.success && personDetails.rowCount > 0){
            let html = result.rows[0].richText;
            let smsText = result.rows[0].smsText;
            let subject = result.rows[0].subject;
            let phone = personDetails.rows[0].phone;
            let email = personDetails.rows[0].email;
            let name = personDetails.rows[0].firstName+" "+personDetails.rows[0].lastName;
            if (smsText && phone){
                let modifiedText = smsText.replace("{{name}}", name).replace("{{magicLink}}",param.data.onboarding_link).replace('[INSERTCUSTOMLINK]', param.data.onboarding_link);
                sendSMSText({phone:phone, text:modifiedText, partnerID:personDetails.rows[0].partnerID,userID: personDetails.rows[0].userID});
            }
            else {
                log.logger("info", encodeURIComponent(`smsText & phone number does not exist for partner ${personDetails.rows[0].partnerID}, hence sms not sent. Template used is Branch Wallet`));
            }
            if (html && email){
                let modifiedHtml = html.replace("{{name}}", name).replace("{{magicLink}}",param.data.onboarding_link).replace('{{[INSERTCUSTOMLINK]}}', param.data.onboarding_link);
                let dat = {
                    to : email, 
                    from: 'Spark-Driver@email.wal-mart.com', 
                    subject: subject,
                    html: modifiedHtml,
                    partnerID: personDetails.rows[0].partnerID,
                    userID: personDetails.rows[0].userID,
                }
                sendEmail(dat);
            }    
            else {
                log.logger("info", encodeURIComponent(`html & email does not exist for partner ${personDetails.rows[0].partnerID}, hence email not sent. Template used is Branch Wallet`));
            }
        }
        else {
            log.logger("error",encodeURIComponent(`Partner details not available for partner ID ${param.data.employee_id}`))
        }
    }

}
async function sendSMSText(param:any){
    try{
        if (portalApiURL && portalApiTwilioAuthKey){
            let url = portalApiURL+"/twilio/sendSMSText";
            let headers = {
                "Content-Type": "application/json",
                "authorization": portalApiTwilioAuthKey,
            }
            log.logger("info",encodeURIComponent(`sendSMSText triggered at ${new Date().toISOString()} to url ${url} for partner param.partnerID`));
            let resp = await sendToExtURL(url,JSON.stringify(param), headers, 'POST');
            if (resp.success){
                log.logger("info",encodeURIComponent(`Successfully sent to Twilio service for partner ${param.partnerID}`));
            }
            else {
                log.logger("error", encodeURIComponent(`Failed to sent to Twilio service. JSON.stringify(resp) for partner ${param.partnerID}`));;
            }
        }
        else {
            log.logger("error", encodeURIComponent(`sendSMSText() failed due to non availability of keys/portal api url`));
        }
    }
    catch(e:any){
        log.logger("error", encodeURIComponent(`sendSMSText() failed with exception ${e.message} ${e.stack}`));
    }
}

async function addWithdrawnStatus(param:any){
    try{
        if (restapiURL && restapiKey){
            let url = restapiURL+"/addWithdrawnStatus";
            let headers = {
                "Content-Type": "application/json",
                "authorization": restapiKey,
            }
            log.logger("info",encodeURIComponent(`addWithdrawnStatus triggered at ${new Date().toISOString()} to url ${url} for partner param.partnerID`));
            let resp = await sendToExtURL(url, JSON.stringify(param), headers, 'POST');
            if (resp.success){
                log.logger("info",encodeURIComponent(`Successfully sent to Restapi service for partner ${param.partnerID}`));
            }
            else {
                log.logger("error", encodeURIComponent(`Failed to sent to Restapi service. JSON.stringify(resp) for partner ${param.partnerID}`));;
            }
        }
        else {
            log.logger("error", encodeURIComponent(`addWithdrawnStatus() failed due to non availability of keys/restapi api url`));
        }
    }
    catch(e:any){
        log.logger("error", encodeURIComponent(`addWithdrawnStatus() failed with exception ${e.message} ${e.stack}`));
    }
}

async function sendEmail(param:any){
    try{
        if (portalApiURL && portalApiTwilioAuthKey){
            let url = portalApiURL+"/twilio/sendEmail";
            let headers = {
                "Content-Type": "application/json",
                "authorization": portalApiTwilioAuthKey,
            }
            log.logger("info",encodeURIComponent(`sendEmail triggered at ${new Date().toISOString()} to url ${url}`));
            let resp = await sendToExtURL(url,JSON.stringify(param), headers, 'POST');
            if (resp.success){
                log.logger("info",encodeURIComponent(`Successfully sent to Twilio service for partner ${param.partnerID}`));
            }
            else {
                log.logger("error", encodeURIComponent(`Failed to sent to Twilio service. JSON.stringify(resp) for partner ${param.partnerID}`));;
            }
        }
        else {
            log.logger("error", encodeURIComponent(`sendEmail() failed for partner ${param.partnerID} due to non availability of keys/portal api url`));
        }
    }
    catch(e:any){
        log.logger("error", encodeURIComponent(`sendEmail() failed for partner ${param.partnerID} with exception ${e.message} ${e.stack}`));
    }
}

function jwtSignPayment(obj:any) {
    const pkey = achKeys.achpemfile_for_payment;
    return jwt.sign(obj, pkey, { algorithm: 'RS256' });
}

function validateWebhookToken(payload:any) {
    try {
        let decoded = jwt.verify(payload, achKeys.ach_webhook_crt_file, { algorithms: ['HS256'] });
        return { success: true, decoded: decoded };
    }
    catch(e:any) {
        log.logger("error", encodeURIComponent(`validateWebhookACHToken falied. payload received ${JSON.stringify(payload)}`));
        return {success:false, message:e.message};
    }
}

setInterval(()=>{
    //5 min schedule for the disbursement summary and payment audit jobs
    if (!isJobRunning) {
        isJobRunning = true;
        runJob();
    }
    else {
        log.logger("error", encodeURIComponent(`Previous job is not yet completed we will retry in next 5 min`));
    }
}, 5*60*1000)

async function runJob(){
    let param = {
        event_type: 'jobDisbursementSummary',
        service_name: 'weekly-api',
        event_name: 'jobDisbursementSummary',
        message: 'Running disbursement aggregation job',
        error: undefined
    }
    let sl = await qryCntrl.insSchedulerLog(param)
    let slID:any;
    if (sl.success && sl.rowCount > 0){
        slID = sl.result[0].id
    }
    else {
        log.logger("error", encodeURIComponent(`jobDisbursementSummary Scheduler log insert failed with message ${sl.message}`));
    }        
    let dt = new Date().getTime();
    await qryDb.jobDisbursementSummary();
    log.logger("info",encodeURIComponent(`Disbursement summary is completed in ${new Date().getTime() - dt} ms`));
    qryCntrl.insertScheduleLogDetails(slID,`jobDisbursementSummary completed in ${new Date().getTime()-dt} ms`);
    param = {
        event_type: 'jobPaymentAudit',
        service_name: 'weekly-api',
        event_name: 'jobPaymentAudit',
        message: 'Running payment audit job',
        error: undefined
    }
    let sl1 = await qryCntrl.insSchedulerLog(param)
    let sl1ID:any;
    if (sl1.success && sl1.rowCount > 0){
        sl1ID = sl1.result[0].id
    }
    else {
        log.logger("error", encodeURIComponent(`jobPaymentAudit Scheduler log insert failed with message ${sl.message}`));
    }   
    dt = new Date().getTime();
    await qryDb.jobPaymentAudit();
    qryCntrl.insertScheduleLogDetails(sl1ID,`jobDisbursementSummary completed in ${new Date().getTime()-dt} ms`);
    isJobRunning = false;
    log.logger("info",encodeURIComponent(`Payment audit summary is completed in ${new Date().getTime() - dt} ms`));
}

async function sendToExtURL(url:string, body:string, headers:any, method:string){
    const controller = new AbortController();
    const timeout = setTimeout(() => {
        controller.abort();
    }, 30000);
    try {
        const response = await nodeFetch(url,
            {
                method: method,
                body: body,
                headers: headers,
                signal: controller.signal
            }
        );
        clearTimeout(timeout);
        try {
            let resp = await response.json();
            log.logger("info", encodeURIComponent(`${url}/${method} is sent with response ${response.status} & ${response.statusText}`))
            return{success:true, result:resp}
        }
        catch(e:any){
            log.logger("error", encodeURIComponent(`${url}/${method} is sent with response ${response.status} & ${response.statusText}`))
            return {success:false, status:response.status, statusText: response.statusText, message:e.message}
        }
    }
    catch(e:any){
        log.logger("error", encodeURIComponent(`${url}/${method} is returned with exception ${e.message} ${e.stack}`));
        return {success:false, message:e.message};
    }
}
