/* 
    validate the decrypt logic if failed send the status authentication failure 
    if decrypt is success this will redirect and return the response code 200 OK
*/
import Router from "express";
import {QryController} from "../controller/query_controller";
import { QryDB } from "../model/query";
import {BranchStatusResponse,ReusableMethods} from "../resuable_component"
import { startWeeklyService } from "../ach/weeklypay";
const log = require("../log");

const router = Router();
module.exports = router;

let qryCntrl = new QryController();
let reusable = new ReusableMethods();

initializeSecretValue();
// initialize();
async function initializeSecretValue(){
    try {
        let result:any = await qryCntrl.getSecretValue();
        if(result){
            initialize();
        }
        else{
            recallIntializeProcess();
        }
    } catch (error) {
        log.logger("error", encodeURIComponent(`initializeSecretValue - exception: ${error}`));
    }
}

async function recallIntializeProcess() {
    try {
        if(!qryCntrl.isDBConnected()){
            log.logger("error", encodeURIComponent(`recallIntializeProcess(), trying to connect in 5 sec`));
            setTimeout(() => {
                recallIntializeProcess();
            }, 5000);
        }
        else{
            initialize();
        }
    } catch (error) {
        log.logger("error", encodeURIComponent(`recallIntializeProcess - exception: ${error}`));
    }
}

async function initialize(){
    await qryCntrl.checkDb();
    await qryCntrl.getBranchServiceAuthKey();
    await qryCntrl.getTwilioUserDetails();
    await qryCntrl.getRestapiKey();
    await qryCntrl.getPortalApiURL();
    await qryCntrl.getRestapiURL();
    await qryCntrl.getACHKeys();
    startWeeklyService();

}

//getBranchAuthKey
router.use(async (req: any, res, next) => {
    if (!qryCntrl.isDBConnected()) {
        res.json({ success: false, message: 'DB Connection Failure, please try after some time', result: [] });
    }
    else {
        const auth = req.headers['authorization'];
        if (!auth) {
            res.json({ success: false, message: 'No token provided', result: [] });
        } else {
            try {
                if (auth == qryCntrl.getBranchAuthKey()){
                    next();
                }
                else {
                    //on failure reloading the api key for a possible change
                    await qryCntrl.getBranchServiceAuthKey();
                    if (qryCntrl.getBranchAuthKey() == auth){
                        next();
                    }
                    else {
                        log.logger("error",encodeURIComponent(`Branch API Authentication Failed`));
                        res.json({success:false, message:"Branch Service authentication failed"});
                    }
                }
            }
            catch (e: any) {
                res.json({ success: false, message: e.message, result: [] });
            }
        }
    }
});

router.post("/walletStatus", async (req: any, res) => {
    try {
        let dt = new Date().getTime();
        // let param = JSON.parse(req.body);
        let param = JSON.parse(Buffer.from(req.body.param,'base64').toString());
        param["data"]["paymentProcessor"] ='BRANCH';
        qryCntrl.insBranchWebhook(param);
        qryCntrl.insPartnerPayoutProcessor(param);
        reusable.accessLog(req, new Date().getTime() - dt, true);
        res.json({success:true})
    } catch (e: any) {
        log.logger("error", encodeURIComponent(`walletStatus Exception ${e.message}, ${e.stack}`));
        res.json({ success: false, error: true, message: e.message });
    }
});

