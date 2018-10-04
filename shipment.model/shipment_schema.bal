public type Shipment record {
    string shipToEmail,
    string shipToCustomerName,
    string shipToAddressLine1,
    string shipToAddressLine2,
    string shipToAddressLine3,
    string shipToContactNumber,
    string shipToAddressLine4,
    string shipToCity, 
    string shipToState, 
    string shipToCountry, 
    string shipToZip, 
    string shipToCounty, 
    string shipToProvince, 
    string billToAddressLine1, 
    string billToAddressLine2, 
    string billToAddressLine3, 
    string billToAddressLine4,
    string billToContactNumber, 
    string billToCity, 
    string billToState, 
    string billToCountry, 
    string billToZip, 
    string billToCounty,
    string billToProvince, 
    string orderNo, 
    string lineNumber, 
    string contextId,
};

public type Shipments record {
    Shipment[] shipments,
};

public function shipmentToString(Shipment s) returns string {
    json shipmentJson = check <json> s;
    return shipmentJson.toString();
}