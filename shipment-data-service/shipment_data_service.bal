import ballerina/http;
import ballerina/log;
import ballerina/mysql;
import ballerinax/kubernetes;
import raj/shipment.model as model;

@kubernetes:Service {
    name: "shipment-data-service-service",
    serviceType:"LoadBalancer"
}
endpoint http:Listener shipmentDataServiceListener {
    port: 8280
};

@kubernetes:Deployment {
    name: "shipment-data-service-deployment",
    namespace: "default",
    labels: {
        "integration": "shipment"
    },
    replicas: 1,
    annotations: {
        "prometheus.io/scrape": "true",
        "prometheus.io/path": "/metrics",
        "prometheus.io/port": "9797"
    },
    additionalPorts: {
        "prometheus": 9797
    },
    buildImage: true,
    push: true,
    image: "index.docker.io/$env{DOCKER_USERNAME}/shipment-data-service:0.5.0",
    username:"$env{DOCKER_USERNAME}",
    password:"$env{DOCKER_PASSWORD}",
    imagePullPolicy: "Always",
    env: {
        shipment_db_host: "staging-db-headless-service.default.svc.cluster.local",
        shipment_db_port: "3306",
        shipment_db_name: "WSO2_STAGING",
        shipment_db_username: {
            secretKeyRef: {
                name: "staging-db-secret",
                key: "username"
            }
        },
        shipment_db_password: {
            secretKeyRef: {
                name: "staging-db-secret",
                key: "password"
            }
        },
        shipment_redis_host: "redis-service.default.svc.cluster.local",     
        b7a_observability_tracing_jaeger_reporter_hostname: "jaeger-udp-service.default.svc.cluster.local"
    },
    copyFiles: [
        { 
            source: "./shipment-data-service/conf/ballerina.conf", 
            target: "/home/ballerina/ballerina.conf", isBallerinaConf: true 
        },
        {
            source: "./shipment-data-service/dependencies/packages/dependencies/",
            target: "/ballerina/runtime/bre/lib/"
        },
        {
            source: "./shipment-data-service/dependencies/packages/balo/",
            target: "/ballerina/runtime/lib/repo/"
        }
    ]
}
@http:ServiceConfig {
    basePath: "/data/shipment"
}
service<http:Service> shipmentDataService bind shipmentDataServiceListener {

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
        methods:["POST"],
        path: "/batch",
        body: "shipments"
    }
    addShipments (endpoint outboundEp, http:Request req, model:Shipments shipments) {
        http:Response res = addShipments(req, untaint shipments);
        outboundEp->respond(res) but { error e => log:printError("Error while responding", err = e) };
    }

    @http:ResourceConfig {
        methods:["GET"],
        path: "/{orderNo}"
    }
    getShipment (endpoint outboundEp, http:Request req, string orderNo) {
        http:Response res = getShipment(req, orderNo);
        outboundEp->respond(res) but { error e => log:printError("Error while responding", err = e) };
    }      
}