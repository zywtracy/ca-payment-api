import {createDecipheriv,randomBytes, createCipheriv, createHash} from 'crypto';

const log = require ("./log");

export interface BranchStatus {
    employee_id: string;
    account_number: string;
    routing_number: string;
    onboarding_link: string;
    reason_code: string;
    reason: string;
}

export interface BranchStatusResponse {
    data: string;
    event: string;
    clientId: string;
    clientType: string;
}

export interface ACHPayType {
        "programId": string,
        "counterpartyEntityAliasId": string,
        "payableNumber": string,
        "payableAmount": string,
        "payableCurrency": "USD",
        "payableStatus": "OPEN",
        "payableExpirationDate": string
        "payableReferences": [
            {
            "payableKeyId": "accountToken",
            "payableKeyValue": string
            }
        ]    
}
export const settings = {
    pgDbConfig: {
        user: 'ddidevdb',
        host: 'ddidevpostgres.postgres.database.azure.com',
        password: 'ETu7ESqVZMQAV3dev',
        database: 'hasura',
        port: 5432,
        max: 50, // max number of clients in the pool
        idleTimeoutMillis: 300000,
        ssl: true
    }
}

export class ReusableMethods {
    constructor(){}
    sanitizeStr = (str:string) =>{
        return Buffer.from(str).toString('base64');
    }

    sanitizeJSON = (str:any)=>{
        return Buffer.from(JSON.stringify(str)).toString('base64');
    }

    /**
     * Decrypts a user provided encrypted value with a user provided key
     *
     * @param base64EncryptedValue is the user provided base64 encrypted value
     * @param base64Key            is the base64 encoded key string
     * @return UTF8 encoded value string
    */
    decrypt = (base64EncryptedValue: String, base64Key: String) => {
        try {
            const encryptedByteValue = Buffer.from(base64EncryptedValue, "base64");
            const key = Buffer.from(base64Key, "base64");

            let decryptValue = this.decryptCipher(encryptedByteValue, key);
            if (decryptValue.success){
                return {success:true, result: decryptValue.val};
            }
            else {
                return {success:false, message: decryptValue.message}
            }
        }
        catch (e:any){
            log.logger("error",encodeURIComponent(`decryptCipher Exception ${e.message} input Data ${base64EncryptedValue}`))
            return {success:false, message:e.message}
        }
    };


    private readonly decryptCipher = (value: Buffer, key: Buffer): any => {
        try {
            const iv = Uint8Array.prototype.slice.call(value).slice(0, 16);
            const data = Uint8Array.prototype.slice.call(value).slice(16, value.length);
            const decipher = createDecipheriv("aes-256-cbc", key, iv);
            return {success:true, val:decipher.update(data, undefined, "utf8") + decipher.final("utf8")};
        }
        catch (e:any){
            log.logger("error",encodeURIComponent(`decryptCipher Exception ${e.message}`));
            return {success:false, message:e.message}
        }
    };

    encrypt = (text:string,base64Key:string) => {
        let iv = randomBytes(16);
        let key = Buffer.from(base64Key, 'base64');
        let cipher = createCipheriv('aes-256-cbc', key, iv);
        let encrypted = cipher.update(text);
        encrypted = Buffer.concat([encrypted, cipher.final()]);
        let finalVal = Buffer.concat([iv,encrypted]);
        this.decrypt(finalVal.toString('base64'),base64Key);
        return {success:true, data:finalVal.toString('base64')};
    }

    accessLog = (req: any, duration: number, status: boolean) => {
        log.accesslog("info", encodeURIComponent(`${this.getRequestIP(req)}, ${req.url}, ${status}, ${duration}`));
    }
    
    getRequestIP = (req: any) => {
        let ip;
        if (req.connection && req.connection.remoteAddress) {
            ip = req.connection.remoteAddress;
        } else if (req.headers['x-forwarded-for']) {
            ip = req.headers['x-forwarded-for'].split(",")[0];
        } else {
            ip = req.ip;
        }
        return ip;
    }

    sha256Hash(msgBuffer: Buffer) {
        return createHash('sha256').update(msgBuffer).digest("hex");
    }

    genRandBytes(length: number) {
        return randomBytes(length);
    }
}

export class Queue {
    constructor (){}
    a:any[] = [];
    b = 0;
    getLength = () =>{
        return this.a.length - this.b;
    }
    isEmpty = () => {
        return 0 == this.a.length;
    }
    enqueue = (dat:any) =>{
        this.a.push(dat);
    }
    dequeue = () => {
        if (0!=this.a.length){
            let c = this.a[this.b];
            2*++this.b>=this.a.length && (this.a=this.a.slice(this.b),this.b=0);
            return c;
        }
    }
    peek = () => {
        return 0<this.a.length?this.a[this.b]:void 0;
    }
}

