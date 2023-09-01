import { Pool, PoolClient } from "pg";
import { settings } from "../resuable_component"
let pool = new Pool (settings.pgDbConfig);
const log = require('../log');

const config = require('../resuable_component');


 async function fnDbQuery(methodName:string,sStr:string, sParam?:any) {
  let client:PoolClient ;
  let start;
  try {
    start = Date.now();
    let qText = Buffer.from(sStr,'base64').toString();
    let qParam = [];
    if(sParam){
      qParam = JSON.parse(Buffer.from(sParam,'base64').toString())
    }
    client = await pool.connect();
    try {
      //this is to avoid multiple statement introduced due to injection
      qText = qText.replace(/;/g,'');
      const qResult = await client.query(qText, qParam);
      const duration = Date.now() - start;
      let result:any = qResult;
      result["success"] = true;
      result.error = false;
      log.dblog("info",encodeURIComponent(`${process.pid}, PSQL, ${methodName}, ${duration} ms, ${pool.idleCount} idle, ${pool.waitingCount} queue, ${pool.totalCount} total`));
      return result;
    } catch (e:any) {
        log.dblog("error",encodeURIComponent(`${process.pid}, PSQLQueryError, ${methodName}, ${e.message}`));
        return {success:false, qry_error: true, message: e.message};
    } finally {
      client.release();
    }
  } catch (e:any){
    log.dblog("error",encodeURIComponent(`${process.pid}, PSQL, ${methodName}, ${e.message}`));
    return {success:false, connection_error: true, message: e.message};
  } 
}

pool.on('error', (err:Error) => {
  log.dblog("error",`${process.pid}, PSQL Pool error, ${err.message}`);
  console.error('Connection error experienced',err.message);
});

async function createPool(newVal:any) {
  try {
    config.settings.pgDbConfig = newVal;
    await pool.end();
    pool = new Pool(config.settings.pgDbConfig);
  } catch (error:any) {
    log.logger("error", encodeURIComponent(`createPool(), Pool connection ${error.message}, trying to connect in 10 sec`));
  }
}

export = { fnDbQuery, createPool};