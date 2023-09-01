import {ReusableMethods} from "../resuable_component";
const log = require('../log');
let {fnDbQuery} = require("../model/psqlAPM");

let reusable = new ReusableMethods();

export class QryDB {
    constructor(){ }

    async checkDB() {
        const queryText = `SELECT now()`;
        let sqt = reusable.sanitizeStr(queryText);
        return await fnDbQuery('checkDB', sqt);
    }

    async getBranchKeys(){
        const queryText = `SELECT key, value FROM secrets WHERE key IN ('BASE64AESKEY')`;
        return await fnDbQuery('getBranchKeys', reusable.sanitizeStr(queryText)); 
    }

    async insBranchWebhook(param:any){
        try{
            const queryText = `INSERT INTO ddi_branch.branch_webhook(event, client_type, client_id, data, employee_id,account_number, routing_number, reason_code, reason, webhook_payload, status, error_msg) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
            const queryParam = [param.event, param.client_type, param.client_id, param.data, param?.decrypt?.employee_id,param?.decrypt?.account_number, param?.decrypt?.routing_number, param?.decrypt?.reason_code, param?.decrypt?.reason, param, param?.status,param?.err_msg];
            return await fnDbQuery('insBranchWebhook', reusable.sanitizeStr(queryText), reusable.sanitizeJSON(queryParam));     
        }
        catch(e:any){
            log.error("error", `insBranchWebhook psql script exception ${e.message}`)
            return {success:false, message:e.message};
        }
    }

    async insParterPayoutProcessor(param:any){
        const queryText  =`INSERT INTO public."partnerPaymentProcessor" ("partnerID","magicLink",status,"paymentProcessor", is_active) VALUES ($1, $2, $3, $4, $5)  ON CONFLICT ON CONSTRAINT "partnerPaymentProcessor_partnerID_paymentProcessor_key" DO UPDATE set "magicLink"= $2, status = $3, updated_on= now(), is_active=$5`;
        const queryParam = [param.employee_id, param.onboarding_link, param.status, param.paymentProcessor, param.is_active];
        return await fnDbQuery('insParterPayoutProcessor', reusable.sanitizeStr(queryText), reusable.sanitizeJSON(queryParam));     
    }

    async getBranchUserHash(){
        const queryText = `SELECT phash FROM "clientCredentials" where username = 'branchservice'`;
        return await fnDbQuery('getBranchUserDetails', reusable.sanitizeStr(queryText));  
    }

    async getPortalApiURL(){
        const queryText = `SELECT key, value FROM secrets WHERE key IN ('internal_api_base_url')`;
        return await fnDbQuery('getPortalApiURL', reusable.sanitizeStr(queryText));  
    }

    async getTwilioUserDetails(){
        const queryText = `SELECT phash FROM "clientCredentials" where username = 'twilioservice'`;
        return await fnDbQuery('getTwilioUserDetails', reusable.sanitizeStr(queryText));  
    }

    async getRestapiKey(){
        const queryText = `SELECT phash FROM "clientCredentials" where username = 'restapi'`;
        return await fnDbQuery('getRestapiKey', reusable.sanitizeStr(queryText));  
    }

    async getSMSEmailTemplateBranchWallet(){
        const queryText = `SELECT "richText","smsText","subject" FROM "communicationTemplates" where name = 'Branch Wallet'`;
        return await fnDbQuery('getSMSEmailTemplateBranchWallet', reusable.sanitizeStr(queryText));  
    }

    async getSMSEmailTemplateBranchApproved(){
        const queryText = `SELECT "richText","smsText","subject" FROM "communicationTemplates" where name = 'branch_approved'`;
        return await fnDbQuery('getSMSEmailTemplateBranchApproved', reusable.sanitizeStr(queryText));  
    }

    async getPartnerDetails(param:any){
        const queryText = `select "firstName", "lastName", email, phone, id as "partnerID", "userID" from partners p where id = $1`;
        const queryParam = [param.employee_id];
        return await fnDbQuery('getTwilioUserDetails', reusable.sanitizeStr(queryText), reusable.sanitizeJSON(queryParam));          
    }

    async getSMSEmailTemplateDisqualified(){
        const queryText = `SELECT "richText","smsText","subject" FROM "communicationTemplates" where name = 'disqualified'`;
        return await fnDbQuery('getSMSEmailTemplate', reusable.sanitizeStr(queryText));  
    }

    // async insCandidateProgress(param: any) {
    //     const queryText = `INSERT INTO ddi_partner.driver_step_status (driver_id, stage_id, step_id,status_id,assigned_os, stage_name, step_name, status_name) SELECT $1,stage_id, step_id, status_id,$2,stage_name, step_name, status_name from ddi_partner.vw_step_status where status_name = $3 ON CONFLICT ON CONSTRAINT driver_step_status_driver_id_status_id_key DO NOTHING  `
    //     const queryParam = [param.partnerID, param.assignedOS, param.stepStatus];
    //     return await fnDbQuery('addApplicationStatus',  reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    // }

    /* Not in use after July 15 2023 release */
    // async insCandidateProgress(partnerID:string,candidateStep:string, candidateStatus:string){
    //     const queryText = `INSERT INTO "candidateProgress" ("candidateID", "candidateStep", "candidateStatus",date,"assignedOS","createdBy")
    //     select clp.id,$2,$3,now(), e."firstName" ||' '|| e."lastName" as name, e."firstName" ||' '|| e."lastName" from "clientLocationPartners" clp join "clientLocations" cl on cl.id = clp."clientLocationID" join "employeeLocations" el on el."clientLocationID" = cl.id join employees e on e.id = el."employeeID" where clp."partnerID" = $1 limit 1;`;
    //     const queryParam = [partnerID,candidateStep, candidateStatus ];
    //     return await fnDbQuery('insCandidateProgress', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));  
    // }

    async getACHKeys(){
        const queryText = `select key, value from "systemConfig" where key in ('ach_program_id_deliveries','ach_program_id_tips','ach_program_id_avs')`;
        return await fnDbQuery('getACHKeys', reusable.sanitizeStr(queryText));          
    }

    async getACHURL(){
        const queryText = `select key, value from secrets where key in ('ACH_ONBOARD_URL','ach_webhook_crt_file','achpemfile','ACH_PAY_THREADS','ACH_PAY_RATE_LIMIT_PER_MIN','JPM_ACH_PASSPHRASE','achpfxfile')`;
        return await fnDbQuery('getACHURL', reusable.sanitizeStr(queryText));
    };

    async getWeeklyPayoutACH(payOutDate:string){
        const queryText = `SELECT t."payoutID" as payout_id, t."partnerID" as partner_id,t.payee_id, t."settlementItemID", si."mappingKey", si."name", achp.token as ach_user_id, sum(amount) as amount, ARRAY_AGG(t."externalID") as transaction_ids from transactions t JOIN "settlementItems" si on si.id = t."settlementItemID" JOIN ach.partner achp on achp.partner_id = t."partnerID" WHERE ("createdDate" at time zone 'america/los_angeles') < $1 AND t.payment_mode ='ACH' AND t.payment_type ='WEEKLY' AND t.status = 'CREATED' AND t.pay_type = 'Tips' GROUP BY 1,2,3,4,5,6,7;`;
        const queryParam = [payOutDate];
        return await fnDbQuery('getWeeklyPayoutACH', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    };

    async getWeeklyPayoutACHEarnings(payOutDate:string){
        const queryText = `SELECT t."payoutID" as payout_id, t."partnerID" as partner_id,t.payee_id, t."settlementItemID", si."mappingKey", si."name", achp.token as ach_user_id, sum(amount) as amount, ARRAY_AGG(t."externalID") as transaction_ids from transactions t JOIN "settlementItems" si on si.id = t."settlementItemID" JOIN ach.partner achp on achp.partner_id = t."partnerID" WHERE ("createdDate" at time zone 'america/los_angeles') < $1 AND t.payment_mode ='ACH' AND t.payment_type ='WEEKLY' AND t.status = 'CREATED' AND t.pay_type = 'Earnings' GROUP BY 1,2,3,4,5,6,7;`;
        const queryParam = [payOutDate];
        return await fnDbQuery('getWeeklyPayoutACH', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }


    async getStartTimeDiffInms(){
        const queryText = `select ROUND(extract(epoch from (((now() at time zone 'america/los_angeles')::date  + interval '1 day') - (now() at time zone 'america/los_angeles')))*1000) as interval_in_sec`;
        return await fnDbQuery('getStartTimeDiffInms', reusable.sanitizeStr(queryText));
    }

    async insWeeklyProcessLog(param:any){
        const queryText = `INSERT INTO ach.weekly_process_log (records_processed, remarks, invoice_ref) VALUES ($1,$2,$3) RETURNING id`;
        const queryParam = [param.records_processed, param.remarks, param.invoice_ref];
        return await fnDbQuery('insWeeklyProcessLog', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async insACHPaymentRaw(param:any){
        const queryText = `INSERT INTO ach.ach_payment_raw(ach_weekly_process_log_id, payable_number, driver_id, payable_key_value, payout_id, settlement_item_id, program_id, mapping_key, payable_amount, confirmation_number, payment_created_time, req_content, resp, resp_status_code, resp_status_text, resp_error_code, resp_error_msg, remarks) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) returning id`;
        const queryParam = [param.ach_weekly_process_log_id, param.payable_number, param.driver_id, param.payable_key_value,  param.payout_id, param.settlement_item_id, param.program_id, param.mapping_key, param.payable_amount, param.confirmation_number, param.payment_created_time, param.req_content, param.resp, param.resp_status_code, param.resp_status_text, param.resp_error_code, param.resp_error_msg, param.remarks];
        return await fnDbQuery('insACHPaymentRaw', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async getACHErrorCodes(){
        const queryText = `SELECT * from ach.ach_payment_error_code`;
        return await fnDbQuery('getACHErrorCodes', reusable.sanitizeStr(queryText));
    }

    async updTransactionPaid(param:any){
        const queryText = `UPDATE transactions set status=$2 where "externalID" = ANY($1::varchar[])`;
        const queryParam = [param.transaction_ids,param.status];
        return await fnDbQuery('updTransactionPaid', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }
    
    async updTransactionPaidWithRemarks(param:any){
        const queryText = `UPDATE transactions set status=$2, memo=memo||' '||$3 where "externalID" = ANY($1)`;
        const queryParam = [param.transaction_ids,param.status, param.remarks];
        return await fnDbQuery('updTransactionPaid', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }
    async insACHPaidTransactions(param:any){
        const queryText = `INSERT INTO ach.ach_paid_transactions(ach_weekly_process_log_id, payable_number, ach_payment_raw_id, confirmation_number, payment_status, driver_id, payout_id, settlement_item_id, mapping_key, pay_type, actual_pay_type, program_id, payable_amount, settlement_date, remarks, ach_user_id,transaction_created_on) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,$16, $17) returning id`;
        const queryParam =[param.ach_weekly_process_log_id, param.payable_number, param.ach_payment_raw_id, param.confirmation_number, param.payment_status, param.driver_id, param.payout_id, param.settlement_item_id, param.mapping_key, param.pay_type, param.actual_pay_type, param.program_id, param.payable_amount, param.settlement_date, param.remarks, param.ach_user_id, param.transaction_created_on];
        return await fnDbQuery('insACHPaidTransactions', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async insACHPayoutTransactions(param:any){
        const queryText = `insert into ach.ach_paid_transactions_details (ach_paid_transaction_id, transaction_id) select $1, unnest($2::varchar[])`;
        const queryParam = [param.ach_paid_transaction_id, param.transaction_ids];
        return await fnDbQuery('insACHPaidTransactions', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async insACHWebhookRaw(param:any){
        const queryText = `insert into ach.ach_payment_webhook_raw(req_encoded, req_decoded, program_id, confirmation_number, payment_status, req_success, response_sent, payment_created_time, payable_number) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) returning id`;
        const queryParam = [param.req_encoded, param.req_decoded, param.program_id, param.confirmation_number, param.payment_status, param.req_success, param.response_sent, param.payment_created_time, param.payable_number];
        return await fnDbQuery('insACHWebhookRaw', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    };

    async updACHPaidStatus(param:any){
        const queryText = `update ach.ach_paid_transactions set payment_status = $1, settlement_date = $3, confirmation_number = $4, audit_processed_status = 'Pending',return_reason_code = $5, return_reason_description = $6, return_date = $7, remarks = $8  where payable_number = $2;`;
        const queryParam = [param.payment_status, param.payable_number, param.settlement_date, param.confirmation_number,param.return_reason_code, param.return_reason_description, param.return_date, param.remarks];
        return await fnDbQuery('updACHPaidStatus', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async updWeeklyDepositSummary(param:any){
        const queryText = `update ddi_pay_audit.disbursement_summary set disbursed_volume = disbursed_volume + $1, disbursed_amount = disbursed_amount + $2 where date(disbursement_date at time zone 'america/los_angeles') = date(now() at time zone 'america/los_angeles');`
        const queryParam = [param.weeklyvol, param.weeklyamt];
        return await fnDbQuery('updWeeklyDepositSummary', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async updTransactionIsDbProcessed(param:any){
        const queryText = `UPDATE transactions set is_ds_processed = true WHERE "externalID" = ANY($1::varchar[])`;
        const queryParam = [param.transaction_ids];
        return await fnDbQuery('updTransactionIsDbProcessed', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }
    async jobDisbursementSummary(){
        const queryText = `select * from ddi_pay_audit.job_disbursement_summary();`
        return await fnDbQuery('jobDisbursementSummary', reusable.sanitizeStr(queryText));
    }
    async jobPaymentAudit(){
        const queryText = `select * from ddi_pay_audit.job_ins_driver_payment_audit();`
        return await fnDbQuery('jobDisbursementSummary', reusable.sanitizeStr(queryText));
    }
    
    async updHoldStatusToTransactions(param:any){
        const queryText = `update transactions t set status ='HOLD', memo = memo || ' ' || $2 where "externalID" in (select aptd.transaction_id from ach.ach_paid_transactions_details aptd join ach.ach_paid_transactions apt on apt.id = aptd.ach_paid_transaction_id where apt.payable_number = $1);`
        const queryParam = [param.payable_number, param.remarks];
        return await fnDbQuery('updHoldStatusToTransactions', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async updLastFailedDate(param:any){
        const queryText = `update ddi_partner.driver_payment_provider set last_failed_on = now() where driver_id = $1`;
        const queryParam = [param.driver_id];
        return await fnDbQuery('updLastFailedDate', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async getNxtInvRef(){
        const queryText = `select invoice_ref, SUBSTRING(REVERSE(invoice_ref) FROM 1 FOR POSITION('_' IN REVERSE(invoice_ref)) - 1)::int +1 as seq from ach.weekly_process_log wpl2 where wpl2.triggerd_on::date = date(now() at time zone 'america/los_angeles') order by wpl2.triggerd_on desc limit 1`
        return await fnDbQuery('getNxtInvRef', reusable.sanitizeStr(queryText));
    }
    async getCurrentPSTDate(){
        const queryText = `select 'ach_tips_'||replace(to_char(date(now() at time zone 'america/los_angeles'),'YYYY-MM-DD'),'-','')||'_1' as curr_date;`
        return await fnDbQuery('getCurrentPSTDate', reusable.sanitizeStr(queryText));
    }

    async getCurrentPSTDateEarnings(){
        const queryText = `select 'ach_earnings_'||replace(to_char(date(now() at time zone 'america/los_angeles'),'YYYY-MM-DD'),'-','')||'_1' as curr_date;`
        return await fnDbQuery('getCurrentPSTDate', reusable.sanitizeStr(queryText));
    }

    async insSchedulerLog(param:any){
        const queryText = `insert into ddi_audit.scheduler_log(event_type, service_name, event_name, triggered_at, message, error) values ($1, $2, $3, now(), $4, $5) returning id;`;
        const queryParam = [param.event_type, param.service_name, param.event_name, param.message, param.error]
        return await fnDbQuery('insSchedulerLog', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    };
    
    async insSchedulerLogDetails(param:any){
        const queryText = `insert into ddi_audit.scheduler_log_details(scheduler_log_id, message_details) values ($1, $2) returning id;`
        const queryParam = [param.scheduler_log_id, param.message_details];
        return await fnDbQuery('insSchedulerLogDetails', reusable.sanitizeStr(queryText),reusable.sanitizeJSON(queryParam));
    }

    async getRestapiURL(){
        const queryText = `SELECT key, value FROM secrets WHERE key IN ('REST_API_URL')`;
        return await fnDbQuery('getPortalApiURL', reusable.sanitizeStr(queryText));  
    }

    async getPayeeId(partnerID:string){
        const queryText =`select clp."externalID", ls.status_name, ls.seq from "clientLocationPartners" clp left join ddi_partner.vw_get_partner_last_status ls on ls.driver_id = clp."partnerID" where clp."partnerID" = $1`;
        const queryParam = [partnerID];
        return await fnDbQuery('getPayeeId', reusable.sanitizeStr(queryText), reusable.sanitizeJSON(queryParam));
    }
}
