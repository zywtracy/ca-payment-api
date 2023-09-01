import express from "express";
import path from "path";
import cors from "cors";
import compression from "compression";

/* Integrate Api Calls */
const branchRoutes = require("./branch/webhook");
const achRoutes = require("./ach/webhook");

const log = require("./log");
log.logger("info",encodeURIComponent("Log Service Started"));
log.dblog("info",encodeURIComponent("Db Log Service Started"));

const app = express();
app.use(compression());
app.use(cors({
    origin: ['http://localhost:4200', 'http://localhost:5443','http://localhost:3000']
}));

app.use(function (req, res, next) {
    res.setHeader('Access-Control-Allow-Orgin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Orgin, X-Requested-With, Content-Type, Accept');
    res.setHeader('Access-Control-Allow-Methods', ['GET', 'POST']);
    res.setHeader("Access-Control-Allow-Credentials", "true");
    next();
})

app.use(express.json({limit: '50mb'}));
app.use(express.urlencoded({
    limit: '5mb',
    extended: true
}));
app.use("/branchservice", branchRoutes);
app.use("/achservice", achRoutes);

app.use(function applyXFrame(req, res, next) {
    res.set('X-Frame-Options', 'SAMEORIGIN');
    next();
});

app.get('/ready', async (req: any, res) => {
    res.sendStatus(200);
});
var httpPort = 6000;
app.listen(httpPort, () => console.log('Weekly Pay API Node Server listening on port ' + httpPort + '!'));

process.on('unhandledRejection', (err:Error) => {
    log.logger('error',encodeURIComponent(`unhandledRejection ${err.message} in process ${process.pid}`));
})

process.on('uncaughtException', (err:Error) => {
    log.logger('error',encodeURIComponent(`uncaughtException ${err.message} in process ${process.pid}`));
})
