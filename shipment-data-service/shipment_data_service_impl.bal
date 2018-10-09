import ballerina/io;
import ballerina/http;
import ballerina/config;
import ballerina/log;
import ballerina/sql;
import ballerina/mime;
import wso2/redis;
import ballerina/crypto;

type shipmentBatchType string|int|float;

endpoint redis:Client redisEp {
    host: "localhost",
    password: "",
    options: { connectionPooling: true, isClusterConnection: false, ssl: false,
        startTls: false, verifyPeer: false, connectionTimeout: 500 }
};

endpoint mysql:Client shipmentDB {
    host: config:getAsString("shipment.db.host"),
    port: config:getAsInt("shipment.db.port"),
    name: config:getAsString("shipment.db.name"),
    username: config:getAsString("shipment.db.username"),
    password: config:getAsString("shipment.db.password"),
    poolOptions: { maximumPoolSize: 5 },
    dbOptions: { useSSL: false, serverTimezone:"UTC" }
};

public function addShipment (http:Request req, model:Shipment shipment) returns http:Response {

    string sqlString = "INSERT INTO CUSTOMER_SHIPMENT_DETAILS(SHIP_TO_EMAIL,SHIP_TO_CUSTOMER_NAME,SHIP_TO_ADDRESS_LINE_1,
        SHIP_TO_ADDRESS_LINE_2,SHIP_TO_ADDRESS_LINE_3,SHIP_TO_CONTACT_NUMBER,SHIP_TO_ADDRESS_LINE_4,
        SHIP_TO_CITY, SHIP_TO_STATE, SHIP_TO_COUNTRY, SHIP_TO_ZIP, SHIP_TO_COUNTY, SHIP_TO_PROVINCE, 
        BILL_TO_ADDRESS_LINE_1, BILL_TO_ADDRESS_LINE_2, BILL_TO_ADDRESS_LINE_3, BILL_TO_ADDRESS_LINE_4,
        BILL_TO_CONTACT_NUMBER, BILL_TO_CITY, BILL_TO_STATE, BILL_TO_COUNTRY, BILL_TO_ZIP, BILL_TO_COUNTY,
        BILL_TO_PROVINCE, ORDER_NUMBER, LINE_NUMBER, CONTEXT_ID) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE 
        SHIP_TO_EMAIL=?,SHIP_TO_CUSTOMER_NAME=?,SHIP_TO_ADDRESS_LINE_1=?,SHIP_TO_ADDRESS_LINE_2=?,
        SHIP_TO_ADDRESS_LINE_3=?,SHIP_TO_CONTACT_NUMBER=?,SHIP_TO_ADDRESS_LINE_4=?,
        SHIP_TO_CITY=?, SHIP_TO_STATE=?, SHIP_TO_COUNTRY=?, SHIP_TO_ZIP=?, SHIP_TO_COUNTY=?, SHIP_TO_PROVINCE=?, 
        BILL_TO_ADDRESS_LINE_1=?, BILL_TO_ADDRESS_LINE_2=?, BILL_TO_ADDRESS_LINE_3=?, BILL_TO_ADDRESS_LINE_4=?,
        BILL_TO_CONTACT_NUMBER=?, BILL_TO_CITY=?, BILL_TO_STATE=?, BILL_TO_COUNTRY=?, BILL_TO_ZIP=?, BILL_TO_COUNTY=?,
        BILL_TO_PROVINCE=?, ORDER_NUMBER=?, LINE_NUMBER=?, CONTEXT_ID=?";

    log:printInfo("Calling shipmentDB->insert for OrderNo=" + shipment.orderNo);

    boolean isSuccessful;
    transaction with retries = 5, oncommit = onCommitFunction, onabort = onAbortFunction {                              
  
        var ret = shipmentDB->update(sqlString,shipment.shipToEmail,shipment.shipToCustomerName,shipment.shipToAddressLine1,
            shipment.shipToAddressLine2,shipment.shipToAddressLine3,shipment.shipToContactNumber,
            shipment.shipToAddressLine4,shipment.shipToCity,shipment.shipToState,shipment.shipToCountry,
            shipment.shipToZip,shipment.shipToCounty,shipment.shipToProvince,shipment.billToAddressLine1,
            shipment.billToAddressLine2,shipment.billToAddressLine3,shipment.billToAddressLine4,
            shipment.billToContactNumber,shipment.billToCity,shipment.billToState,shipment.billToCountry,
            shipment.billToZip,shipment.billToCounty,shipment.billToProvince,shipment.orderNo,
            shipment.lineNumber,shipment.contextId,
            shipment.shipToEmail,shipment.shipToCustomerName,shipment.shipToAddressLine1,
            shipment.shipToAddressLine2,shipment.shipToAddressLine3,shipment.shipToContactNumber,
            shipment.shipToAddressLine4,shipment.shipToCity,shipment.shipToState,shipment.shipToCountry,
            shipment.shipToZip,shipment.shipToCounty,shipment.shipToProvince,shipment.billToAddressLine1,
            shipment.billToAddressLine2,shipment.billToAddressLine3,shipment.billToAddressLine4,
            shipment.billToContactNumber,shipment.billToCity,shipment.billToState,shipment.billToCountry,
            shipment.billToZip,shipment.billToCounty,shipment.billToProvince,shipment.orderNo,
            shipment.lineNumber,shipment.contextId);

        match ret {
            int insertedRows => {
                if (insertedRows < 1) {
                    log:printError("Calling shipmentDB->insert for OrderNo=" + shipment.orderNo 
                        + " failed", err = ());
                    isSuccessful = false;
                    abort;
                } else {
                    log:printInfo("Calling shipmentDB->insert OrderNo=" + shipment.orderNo + " succeeded");
                    isSuccessful = true;
                }
            }
            error err => {
                log:printError("Calling shipmentDB->insert for OrderNo=" + shipment.orderNo 
                    + " failed", err = err);
                retry;
            }
        }        
    }  

    json resJson;
    int statusCode;
    if (isSuccessful) {
        statusCode = http:OK_200;
        resJson = { "Status": "Shipment is inserted to the staging database for order : " 
                    + shipment.orderNo };
    } else {
        statusCode = http:INTERNAL_SERVER_ERROR_500;
        resJson = { "Status": "Failed to insert shipment to the staging database for order : " 
                    + shipment.orderNo };
    }
    
    http:Response res = new;
    res.setJsonPayload(resJson);
    res.statusCode = statusCode;
    return res;
}

public function addShipments (http:Request req, model:Shipments shipments)
                    returns http:Response {

    string uniqueString;
    shipmentBatchType[][] shipmentBatches;
    foreach i, shipment in shipments.shipments {
        shipmentBatchType[] rec = [shipment.shipToEmail,shipment.shipToCustomerName,shipment.shipToAddressLine1,
            shipment.shipToAddressLine2,shipment.shipToAddressLine3,shipment.shipToContactNumber,
            shipment.shipToAddressLine4,shipment.shipToCity,shipment.shipToState,shipment.shipToCountry,
            shipment.shipToZip,shipment.shipToCounty,shipment.shipToProvince,shipment.billToAddressLine1,
            shipment.billToAddressLine2,shipment.billToAddressLine3,shipment.billToAddressLine4,
            shipment.billToContactNumber,shipment.billToCity,shipment.billToState,shipment.billToCountry,
            shipment.billToZip,shipment.billToCounty,shipment.billToProvince,shipment.orderNo,
            shipment.lineNumber,shipment.contextId,
            shipment.shipToEmail,shipment.shipToCustomerName,shipment.shipToAddressLine1,
            shipment.shipToAddressLine2,shipment.shipToAddressLine3,shipment.shipToContactNumber,
            shipment.shipToAddressLine4,shipment.shipToCity,shipment.shipToState,shipment.shipToCountry,
            shipment.shipToZip,shipment.shipToCounty,shipment.shipToProvince,shipment.billToAddressLine1,
            shipment.billToAddressLine2,shipment.billToAddressLine3,shipment.billToAddressLine4,
            shipment.billToContactNumber,shipment.billToCity,shipment.billToState,shipment.billToCountry,
            shipment.billToZip,shipment.billToCounty,shipment.billToProvince,shipment.orderNo,
            shipment.lineNumber,shipment.contextId];
        shipmentBatches[i] = rec;
        uniqueString = uniqueString + "," + shipment.orderNo;        
    }
    
    string sqlString = "INSERT INTO CUSTOMER_SHIPMENT_DETAILS(SHIP_TO_EMAIL,SHIP_TO_CUSTOMER_NAME,SHIP_TO_ADDRESS_LINE_1,
        SHIP_TO_ADDRESS_LINE_2,SHIP_TO_ADDRESS_LINE_3,SHIP_TO_CONTACT_NUMBER,SHIP_TO_ADDRESS_LINE_4,
        SHIP_TO_CITY, SHIP_TO_STATE, SHIP_TO_COUNTRY, SHIP_TO_ZIP, SHIP_TO_COUNTY, SHIP_TO_PROVINCE, 
        BILL_TO_ADDRESS_LINE_1, BILL_TO_ADDRESS_LINE_2, BILL_TO_ADDRESS_LINE_3, BILL_TO_ADDRESS_LINE_4,
        BILL_TO_CONTACT_NUMBER, BILL_TO_CITY, BILL_TO_STATE, BILL_TO_COUNTRY, BILL_TO_ZIP, BILL_TO_COUNTY,
        BILL_TO_PROVINCE, ORDER_NUMBER, LINE_NUMBER, CONTEXT_ID) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE 
        SHIP_TO_EMAIL=?,SHIP_TO_CUSTOMER_NAME=?,SHIP_TO_ADDRESS_LINE_1=?,SHIP_TO_ADDRESS_LINE_2=?,
        SHIP_TO_ADDRESS_LINE_3=?,SHIP_TO_CONTACT_NUMBER=?,SHIP_TO_ADDRESS_LINE_4=?,
        SHIP_TO_CITY=?, SHIP_TO_STATE=?, SHIP_TO_COUNTRY=?, SHIP_TO_ZIP=?, SHIP_TO_COUNTY=?, SHIP_TO_PROVINCE=?, 
        BILL_TO_ADDRESS_LINE_1=?, BILL_TO_ADDRESS_LINE_2=?, BILL_TO_ADDRESS_LINE_3=?, BILL_TO_ADDRESS_LINE_4=?,
        BILL_TO_CONTACT_NUMBER=?, BILL_TO_CITY=?, BILL_TO_STATE=?, BILL_TO_COUNTRY=?, BILL_TO_ZIP=?, BILL_TO_COUNTY=?,
        BILL_TO_PROVINCE=?, ORDER_NUMBER=?, LINE_NUMBER=?, CONTEXT_ID=?";

    log:printInfo("Calling shipmentDB->batchUpdate for orders : " + uniqueString);

    boolean isSuccessful;
    transaction with retries = 5, oncommit = onCommitFunction, onabort = onAbortFunction {  
        var retBatch = shipmentDB->batchUpdate(sqlString, ...shipmentBatches); 
        io:println(retBatch);
        match retBatch {
            int[] counts => {
                foreach count in counts {
                    if (count < 1) {
                        log:printError("Calling shipmentDB->batchUpdate for orders : " + uniqueString 
                            + " failed", err = ());
                        isSuccessful = false;
                        abort;
                    } else {
                        log:printInfo("Calling shipmentDB->batchUpdate orders : " + uniqueString + " succeeded");
                        isSuccessful = true;
                    }
                }
            }
            error err => {
                log:printError("Calling shipmentDB->batchUpdate for orders : " + uniqueString 
                    + " failed", err = err);
                retry;
            }
        }
    }        

    json resJson;
    int statusCode;
    if (isSuccessful) {
        statusCode = http:OK_200;
        resJson = { "Status": "Shipments are inserted to the staging database for orders : " 
            + uniqueString};
    } else {
        statusCode = http:INTERNAL_SERVER_ERROR_500;
        resJson = { "Status": "Failed to insert shipments to the staging database for orders : " 
            + uniqueString };
    }

    http:Response res = new;
    res.setJsonPayload(resJson);
    res.statusCode = statusCode;
    return res;
}

public function getShipment (http:Request req, string orderNo)
                    returns http:Response {
    
    string cachedRes;
    boolean found; 
    (found, cachedRes) = readFromRedis(orderNo);
    json retJson;
    int code;
    if (found) {
        io:StringReader sr = new(cachedRes);
        retJson = check sr.readJson();
        io:println(retJson.shipToEmail);
        code = http:OK_200;
    } else {
        string sqlString = "select * from CUSTOMER_SHIPMENT_DETAILS where ORDER_NUMBER=? limit 1";
        var ret = shipmentDB->select(sqlString, model:Shipment, loadToMemory = true, orderNo);

        match ret {
            table<model:Shipment> tableShipment => {
                if (tableShipment.count()== 0) {
                    retJson = { "Status": "Not Found"};
                    code = http:NOT_FOUND_404;
                } else {
                    json shipmentJsonArray = check <json> tableShipment;
                    retJson = shipmentJsonArray[0];
                    code = http:OK_200;
                    // write in redis
                    writeInRedis(orderNo, retJson.toString());
                }
            }
            error err => {
                retJson = { "Status": "Internal Server Error", "Error": err.message };
                code = http:INTERNAL_SERVER_ERROR_500;
            }
        }
    }

    http:Response resp = new;
    resp.setJsonPayload(untaint retJson);
    resp.statusCode = code;
    return resp;
}

function writeInRedis(string key, string payload) {
    log:printInfo("Storing " + key + " in redis");
    var stringSetresult = redisEp->setVal(key, payload);

    match stringSetresult {
        string res => log:printInfo("Stored " + key + " in redis. " + res);
        error err => log:printError("Error occurred while storing " + key + " in redis", err=err);
    }
}

function readFromRedis(string key) returns (boolean, string) {
    log:printInfo("Reading " + key + " from redis");
    var value = redisEp->get(key);

    boolean found;
    string resPayload;
    match value {
        string res => {
            log:printInfo("Read " + key + " from redis. " + res);
            found = true;
            resPayload = res;
        }
        () => log:printInfo("Key " + key + " does not exist in redis. ");
        error err => log:printError("Error occurred while reading " + key + " from redis", err=err);
    }

    return (found, resPayload);
}

function onCommitFunction(string transactionId) {
    log:printInfo("Transaction: " + transactionId + " committed");
}

function onAbortFunction(string transactionId) {
    log:printInfo("Transaction: " + transactionId + " aborted");
}