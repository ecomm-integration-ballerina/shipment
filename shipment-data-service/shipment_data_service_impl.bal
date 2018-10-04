import ballerina/io;
import ballerina/http;
import ballerina/config;
import ballerina/log;
import ballerina/sql;
import ballerina/mime;

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

public function getShipment (http:Request req)
                    returns http:Response {

    // string orderNo = "";
    // string sqlString = "select * from CUSTOMER_SHIPMENT_DETAILS where ORDER_NUMBER=? limit 1";

    // var ret = orderDB->select(sqlString, model:Shipment, orderNo);

    http:Response resp = new;
    // json[] jsonReturnValue;
    // match ret {
    //     table<model:OrderDAO> tableOrderDAO => {
    //         foreach orderRec in tableOrderDAO {
    //             io:StringReader sr = new(check mime:base64DecodeString(orderRec.request.toString()));
    //             json requestJson = check sr.readJson();
    //             orderRec.request = requestJson;
    //             jsonReturnValue[lengthof jsonReturnValue] = check <json> orderRec;
    //         }
    //         io:println(jsonReturnValue);
    //         resp.setJsonPayload(untaint jsonReturnValue);
    //         resp.statusCode = http:OK_200;
    //     }
    //     error err => {
    //         json respPayload = { "Status": "Internal Server Error", "Error": err.message };
    //         resp.setJsonPayload(untaint respPayload);
    //         resp.statusCode = http:INTERNAL_SERVER_ERROR_500;
    //     }
    // }

    

    return resp;
}

function onCommitFunction(string transactionId) {
    log:printInfo("Transaction: " + transactionId + " committed");
}

function onAbortFunction(string transactionId) {
    log:printInfo("Transaction: " + transactionId + " aborted");
}