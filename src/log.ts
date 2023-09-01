const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');

let accessLog = createLogger({
  format: format.combine(
    format.errors({ stack: true }),
    format.splat(),
    format.simple()
  ), 
  transports: [
    new (transports.DailyRotateFile)({
        filename: './log/info/access-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'info',
    })
  ]
});

let logger = createLogger({
  format: format.combine(
    format.errors({ stack: true }),
    format.splat(),
    format.simple()
  ), 
  transports: [
    new (transports.DailyRotateFile)({
        filename: './log/error/error-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'error',
    }),
    new (transports.DailyRotateFile)({
        filename: './log/info/info-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'info',
    })
  ]
});

let dbServiceLog = createLogger({
  format: format.combine(
    format.errors({ stack: true }),
    format.splat(),
    format.simple()
  ), 
  transports: [
    new (transports.DailyRotateFile)({
        filename: './log/error/errordb-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'error',
    }),
    new (transports.DailyRotateFile)({
        filename: './log/info/infodb-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'info',
    })
  ]
});

let payQLog = createLogger({
  format: format.combine(
    format.errors({ stack: true }),
    format.splat(),
    format.simple()
  ), 
  transports: [
    new (transports.DailyRotateFile)({
        filename: './log/onepayq/onepayq-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'error',
    }),
    new (transports.DailyRotateFile)({
        filename: './log/info/infodb-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '20m',
        maxFiles: '14d',
        level: 'info',
    })
  ]
});

function fnAccessLog (level:string, message:any):void{
  let dt = new Date();
  let date = dt.getFullYear()+"-"+(dt.getMonth()+1).toString().padStart(2,"0")+"-"+dt.getDate().toString().padStart(2,"0")+"T"+dt.getHours().toString().padStart(2,"0")+":"+dt.getMinutes().toString().padStart(2,"0")+":"+dt.getSeconds().toString().padStart(2,"0");
  accessLog.log(level, decodeURIComponent(`${date}, ${message}`));
}

function wlogger (level:string, message:any):void{
  let dt = new Date();
  let date = dt.getFullYear()+"-"+(dt.getMonth()+1).toString().padStart(2,"0")+"-"+dt.getDate().toString().padStart(2,"0")+"T"+dt.getHours().toString().padStart(2,"0")+":"+dt.getMinutes().toString().padStart(2,"0")+":"+dt.getSeconds().toString().padStart(2,"0");
  logger.log(level, decodeURIComponent(`${date}, ${message}`));
}

function dblogger (level:string, message:any):void{
  let dt = new Date();
  let date = dt.getFullYear()+"-"+(dt.getMonth()+1).toString().padStart(2,"0")+"-"+dt.getDate().toString().padStart(2,"0")+"T"+dt.getHours().toString().padStart(2,"0")+":"+dt.getMinutes().toString().padStart(2,"0")+":"+dt.getSeconds().toString().padStart(2,"0");
  dbServiceLog.log(level, decodeURIComponent(`${date}, ${message}`));
}

function qlogger (level:string, message:any):void{
  let dt = new Date();
  let date = dt.getFullYear()+"-"+(dt.getMonth()+1).toString().padStart(2,"0")+"-"+dt.getDate().toString().padStart(2,"0")+"T"+dt.getHours().toString().padStart(2,"0")+":"+dt.getMinutes().toString().padStart(2,"0")+":"+dt.getSeconds().toString().padStart(2,"0");
  payQLog.log(level, decodeURIComponent(`${date}, ${message}`));
}
export = {logger: wlogger, dblog: dblogger, accesslog: fnAccessLog,qlog:qlogger};

// if (process.env.NODE_ENV !== 'production') {
//   logger.add(new transports.Console({
//     format: format.simple()
//   }));
// }
