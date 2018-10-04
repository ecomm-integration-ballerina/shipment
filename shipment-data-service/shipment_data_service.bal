import ballerina/http;
import ballerina/log;
import ballerina/mysql;
import raj/shipment.model as model;

endpoint http:Listener orderListener {
    port: 8290
};

@http:ServiceConfig {
    basePath: "/shipment"
}
service<http:Service> shipmentDataService bind orderListener {

    @http:ResourceConfig {
        methods:["POST"],
        path: "/",
        body: "shipment"
    }
    addShipment (endpoint outboundEp, http:Request req, model:Shipment shipment) {
        http:Response res = addShipment(req, untaint shipment);
        outboundEp->respond(res) but { error e => log:printError("Error while responding", err = e) };
    }

    @http:ResourceConfig {
        methods:["GET"],
        path: "/"
    }
    getShipment (endpoint outboundEp, http:Request req) {
        http:Response res = getShipment(req);
        outboundEp->respond(res) but { error e => log:printError("Error while responding", err = e) };
    }      
}