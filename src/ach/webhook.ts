/* 
    decrypt body payload using HS256 algorithm.
    validate the decrypt logic if failed send the status authentication failure 
    if decrypt is success this will redirect and return the response code 200 OK
*/
import Router from "express";
import {QryController} from "../controller/query_controller";
import {ReusableMethods} from "../resuable_component";
import { getWeeklyPayout } from "../ach/weeklypay"
const log = require("../log");

const router = Router();
module.exports = router;

let qryCntrl = new QryController();
let reusable = new ReusableMethods();

router.use(async (req: any, res, next) => {
    if (!qryCntrl.isDBConnected()) {
        res.json({ success: false, message: 'DB Connection Failure, please try after some time', result: [] });
    }
    else {
        if (req.headers["x-mock-achweeklypay"]){
            if (qryCntrl.getBranchAuthKey()==req.headers["x-mock-achweeklypay"]){
                next();
            }
            else {
                res.status(400).json({success:false, message:"Invalid mock ach weeklypay key"});
            }
        }
        else if (req.headers["authorization"]){
            if (qryCntrl.getBranchAuthKey() == req.headers["authorization"]){
                next();
            }
        }
        else {
            res.status(400).json({success:false, message:"Invalid payload"})
        }
    }
});

router.post("/achpaymentstatusupdate", async (req: any, res) => {
    let dt = new Date().getTime();
    try {
        let dat = JSON.parse(Buffer.from(req.body.param,'base64').toString());
        let param = {
            body: dat.body,
            decoded: dat.decoded ,
        }
        let result = await qryCntrl.insACHWebhookRaw(param);
        reusable.accessLog(req, new Date().getTime() - dt, result.success);
        res.json(result);
    } catch (e: any) {
        log.logger("error", encodeURIComponent(`achpaymentstatusupdate Exception ${e.message}, ${e.stack}`));
        reusable.accessLog(req, new Date().getTime() - dt, false);
        res.json({ success: false, error: true, message: e.message });
    }
});

router.post("/initiateWeeklyPay", async (req: any, res) => {
	res.sendStatus(200);
	let payoutDate = req.body.payoutDate;
    let suffix = req.body.payable_number_suffix;
	getWeeklyPayout(payoutDate, suffix);
})

