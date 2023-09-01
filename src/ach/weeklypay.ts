/*
# For settlement Item (Mapping key) set as weekly, this service will enable the payment to ACH. 
# for weekly transaction, payoutid of each of the partner of that week is consolidated and payment is initiated
# once payment is confirmed transaction table is updated with paid status for each of the payout id (all transaction that belongs to the payout id)
# process
# get consolidated transactions that are not paid from transactions table for each of the payout id
# prepare the pay statement for each of the payout and send to the payment service. 
# weekly payment is identified with the additional parameter payoutid. 
# payment is triggered every week UTC 2PM ( 6am PST/7am PDT)
*/

import {QryController} from "../controller/query_controller";
import { Queue } from "../resuable_component";
const log = require("../log");

let qryCntrl = new QryController();
let limit = 2000;
let weeklyPayQ = new Queue();
let weeklyLogId = "";
let weeklyLogIdEarnings = "";
let processedCnt = 0;
let totalCnt = 0;

export async function startWeeklyService(){
    let dt = new Date();
    //starting the service LOS ANGELES 6AM hours the following day
    let dateDiff = await getStartTimeDiffInms();
    if (dateDiff){
        //since our process time 6 AM los angeles time, if time diff is more than 18 hours, we wait only of remaining time from 6 AM. otherwise time will midnight + 6 hours. 
        dateDiff = dateDiff > 18*60*60*1000 ? (24*60*60*1000) - dateDiff : dateDiff + (6*60*60*1000); 
        log.logger("info",encodeURIComponent(`ACH Weekly service started and first weekly pay check will happen after ${dateDiff/(1000*60*60)} hours. payment will happen only on Tuesdays`));
        setTimeout(() => {
            //start if the current day is Tuesday
            if (new Date().getDay() == 2) startPayService();
            setInterval(()=>{
                if (new Date().getDay() == 2) {
                    startPayService();
                }
                else {
                    let param = {
                        event_type: 'weeklypay',
                        service_name: 'weekly-api',
                        event_name: 'startWeeklyService',
                        message: 'Getting Weekly payment service for ACH',
                        error: 'Today is not Tuesday hence skipping this schedule.'
                    }
                    qryCntrl.insSchedulerLog(param)
                }
            }, 24*60*60*1000);
        },dateDiff);    
    }
    else {
        log.logger("error",encodeURIComponent(`Could get the waittime info for los angeles and will be retried in next 10 min`));
        setTimeout(() => {
            startWeeklyService();
        }, 10*60*1000);
    }
}

export async function startPayService(){
    await qryCntrl.getACHKeys();
    let d = new Date();
    let curDate = new Date(d.getFullYear(), d.getMonth(),d.getDate()-1);
    let payOutDate = curDate.getFullYear()+"-"+(curDate.getMonth()+1).toString().padStart(2,"0")+"-"+(curDate.getDate()).toString().padStart(2,"0");
    log.logger("info",encodeURIComponent(`Getting weekly payout for payout date ${payOutDate}`));
    getWeeklyPayout(payOutDate);
}

async function sendToPayservice(payout:any){
    let achKeys = await qryCntrl.setAndgetACHKeys();
    let unq = payout.suffix;
    let weeklyLogId = payout.logID;
    let unqKey = unq ? unq.length > 0 ? "#"+unq : '':'';
    let dt = new Date(new Date().getTime()+5*24*60*60*1000); //adding 5 days from today as expiry date. need to watch of any trns that did not receive confirmation request within this period.
    let expDt = dt.getFullYear()+'-'+(dt.getMonth()+1).toString().padStart(2,'0')+'-'+dt.getDate().toString().padStart(2,'0');
    qryCntrl.resetWeeklyCnt();
    let weeklyReq = {
        "programId": payout.mappingKey == 3 ? achKeys.ach_program_id_tips : achKeys.ach_program_id_deliveries,
        "counterpartyEntityAliasId": payout.partner_id,
        "payableNumber": payout.payout_id+'#'+payout.mappingKey+unqKey,
        "payableAmount": payout.amount.toString(),
        "payableCurrency": "USD",
        "payableStatus": "OPEN",
        "payableExpirationDate": expDt,
        "payableReferences": [
            {
            "payableKeyId": "accountToken",
            "payableKeyValue": payout.ach_user_id
            }
        ]
    }
    qryCntrl.sendToACHPayService(encodeURIComponent(JSON.stringify(weeklyReq)), new Date().toISOString(),encodeURIComponent(JSON.stringify(payout)),weeklyLogId)
}

// async function sendToPayservice(payoutArr:any,weeklyLogId:string,unq?:string){
//     let achKeys = qryCntrl.setAndgetACHKeys();
//     let unqKey = unq ? unq.length > 0 ? "#"+unq : '':'';
//     let dt = new Date(new Date().getTime()+10*24*60*60*1000); //adding 10 days from today as expiry date
//     let expDt = dt.getFullYear()+'-'+(dt.getMonth()+1).toString().padStart(2,'0')+'-'+dt.getDate().toString().padStart(2,'0');
//     qryCntrl.resetWeeklyCnt();
//     payoutArr.map((payout:any)=>{
//         let weeklyReq = {
//             "programId": payout.mappingKey == 3 ? achKeys.ach_program_id_tips : achKeys.ach_program_id_deliveries,
//             "counterpartyEntityAliasId": payout.partner_id,
//             "payableNumber": payout.payout_id+'#'+payout.mappingKey+unqKey,
//             "payableAmount": payout.amount.toString(),
//             "payableCurrency": "USD",
//             "payableStatus": "OPEN",
//             "payableExpirationDate": expDt,
//             "payableReferences": [
//                 {
//                 "payableKeyId": "accountToken",
//                 "payableKeyValue": payout.ach_user_id
//                 }
//             ]
//         }
//         qryCntrl.sendToACHPayService(encodeURIComponent(JSON.stringify(weeklyReq)), new Date().toISOString(),encodeURIComponent(JSON.stringify(payout)),weeklyLogId)
//     })
// }

export async function getWeeklyPayout(payOutDate:string,suffix?:string){
    let param = {
        event_type: 'weeklypay',
        service_name: 'weekly-api',
        event_name: 'startWeeklyService',
        message: 'Getting Weekly payment service for ACH',
        error: undefined
    }
    let sl = await qryCntrl.insSchedulerLog(param)
    let slID:any;
    if (sl.success && sl.rowCount > 0){
        slID = sl.result[0].id
    }
    else {
        log.logger("error", encodeURIComponent(`sendToDMSView Scheduler log insert failed with message ${sl.message}`));
    }        
    limit = qryCntrl.getWeeklyPayRateLimit() ? qryCntrl.getWeeklyPayRateLimit() : limit;
    let result = await qryCntrl.getWeeklyPayoutACH(payOutDate);
    if (result.success && result.rowCount > 0){
        let lastInvRecord = await qryCntrl.getNxtInvRef();
        let lastInvRef = '';
        if (lastInvRecord.success && lastInvRecord.rowCount == 0){
            let curD = await qryCntrl.getCurrentPSTDate();
            lastInvRef = curD.curDate+'_1';
        }
        else if (lastInvRecord.success) {
            let curD = await qryCntrl.getCurrentPSTDate();
            lastInvRef = curD.curDate+'_'+lastInvRecord.result[0].seq
        }
        else {
            log.logger('error', encodeURIComponent(`Failed to the inv next reference`));
        }
        let param = {
            records_processed: result.rowCount,
            remarks: result.rowCount == 0 ? 'No data to process' : undefined,
            invoice_ref : lastInvRef
        }
        let weeklyLogId = await insWeeklyProcessLog(param);
        log.logger("info",encodeURIComponent(`There are around ${result.rowCount} records for weekly payment for tips and getting processed`));
        qryCntrl.insertScheduleLogDetails(slID,`${result.rowCount} records of weekly payments for tips are getting processed for payout date ${payOutDate}`);
        result.result.map((row:any) => {
            row["logID"] = weeklyLogId;
            row["suffix"] = suffix;
            weeklyPayQ.enqueue(row);
        });
        totalCnt = weeklyPayQ.getLength();
        log.logger("info", encodeURIComponent(`WeeklyOneQ is having around ${weeklyPayQ.getLength()} records for tips, it is getting processed @ ${limit}/min`));
        // if (weeklyPayQ.getLength()>0){
        //     drainWeeklyPayQWithOffsetLimit();
        // }
    }
    else {
        log.logger("error",encodeURIComponent(`Failed to get the weekly payout for tips error ${result.message}`));
    }
    result = await qryCntrl.getWeeklyPayoutACHEarnings(payOutDate);
    if (result.success && result.rowCount > 0){
        let lastInvRecord = await qryCntrl.getNxtInvRef();
        let lastInvRefEarnings = '';
        if (lastInvRecord.success && lastInvRecord.rowCount == 0){
            let curDE = await qryCntrl.getCurrentPSTDateEarnings();
            lastInvRefEarnings = curDE.curDate+'_1';
        }
        else if (lastInvRecord.success) {
            let curDE = await qryCntrl.getCurrentPSTDateEarnings();
            lastInvRefEarnings = curDE.curDate+'_'+lastInvRecord.result[0].seq
        }
        else {
            log.logger('error', encodeURIComponent(`Failed to the inv next reference`));
        }
        let param = {
            records_processed: result.rowCount,
            remarks: result.rowCount == 0 ? 'No data to process' : undefined,
            invoice_ref : lastInvRefEarnings
        }
        weeklyLogIdEarnings = await insWeeklyProcessLog(param);
        log.logger("info",encodeURIComponent(`There are around ${result.rowCount} records for weekly payment for Earnings and are getting processed`));
        qryCntrl.insertScheduleLogDetails(slID,`${result.rowCount} records of weekly payments for Earnings are getting processed for payout date ${payOutDate}`);
        result.result.map((row:any) => {
            row["logID"] = weeklyLogIdEarnings;
            row["suffix"] = suffix;
            weeklyPayQ.enqueue(row);
        });
        totalCnt = weeklyPayQ.getLength();
        log.logger("info", encodeURIComponent(`WeeklyOneQ is having around ${weeklyPayQ.getLength()} records for Earnings, it is getting processed @ ${limit}/min`));
    }
    else {
        log.logger("error",encodeURIComponent(`Failed to get the weekly payout for tips error ${result.message}`));
    }
    if (weeklyPayQ.getLength()>0){
        drainWeeklyPayQWithOffsetLimit();
    }
}

async function drainWeeklyPayQWithOffsetLimit(){
    let loopCnt = weeklyPayQ.getLength() > limit ? limit : weeklyPayQ.getLength();
    for (let i = 0; i < loopCnt; i++){
        sendToPayservice(weeklyPayQ.dequeue());
        processedCnt ++;
    }
    log.logger("info", encodeURIComponent(`Processed ${processedCnt}/${totalCnt}, Batch of ${limit} will be processed in next min`));
    if (weeklyPayQ.getLength() > 0){
        setTimeout(() => {
            drainWeeklyPayQWithOffsetLimit();
        }, 60000);
    }
    else {
        log.logger("info", encodeURIComponent(`All records processed for the weekly payment of ONE`));
    }
}
async function insWeeklyProcessLog(param:any){
    let result = await qryCntrl.insWeeklyProcessLog(param);
    if (result.success){
        log.logger("info", encodeURIComponent(`Weekly processing log details inserted`))
        return result.result[0].id;
    }
    else {
        log.logger("info", encodeURIComponent(`Failed to insert Weekly processing log details`))
        return undefined;
    }
}

async function getStartTimeDiffInms(){
    let result = await qryCntrl.getStartTimeDiffInms();
    if (result.success){
        log.logger("info", encodeURIComponent(`Received waiting time for the service as ${result.delay_in_ms} in ms`))
        return result.delay_in_ms;
    }
    else {
        log.logger("info", encodeURIComponent(`Failed to get the waiting time`))
        return undefined;
    }
}