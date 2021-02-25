import json
import boto3
import requests
import database_helper

# -------------------------------------------------------------------------------
# Author: JBoba, NMunoz, YHu
# This function receives the distance, fuel type and ,if user knows, fuel consumption
# and an JSON-Object with the fields Origin, Destination and the Region in which they are (ideally both of them).
# It returns co2 emission as a float
# All the route calculation logic is done by Google Directions API
# Example for test: 'Origin'= 'Frankfurt','Destination'='Berlin','Region'='DE','Vehicle_type'='SmallDieselCar' -> CO2_fin
# -------------------------------------------------------------------------------

dynamodb = boto3.resource('dynamodb')
client = boto3.client('lambda')


def lambda_handler(event, context):
    
    #Check if Origin is given
    if "Origin" not in event:
        return {
            'statusCode': 400,
            'body': "Origin is not defined!" 
        }
        
    #Check if Destination is given
    if "Destination" not in event:
        return {
            'statusCode': 400,
            'body': "Destination is not defined!" 
        }
    
    # invoke-parameters for the function getSpeedValuesBetweenTwoWaypoints
    origin = event["Origin"]
    destination = event["Destination"]
    inputForInvoker = {'Origin': origin, 'Destination': destination}

    # call getSpeedValuesBetweenTwoWaypoints-Function
    response = client.invoke(
        FunctionName='arn:aws:lambda:eu-central-1:663325156950:function:getSpeedValuesBetweenTwoWaypoints',
        InvocationType='RequestResponse',
        Payload=json.dumps(inputForInvoker),
    )

    # receive the Payload file and transfer it to Json format
    responseJson = json.load(response['Payload'])
    body_json = responseJson['body']
    statusCode = responseJson['statusCode']

    if (statusCode == 200):
        # transfer the Distance_in_m into km and calculate the co2 emission
        distance = body_json["distance"]
        distanceKmCity = body_json["distanceKmCity"]
        distanceKmHighway = body_json["distanceKmHighway"]

    else:
        # TODO: All kind of error codes
        return {
            'statusCode': 400,
            'body': "Some kind of Error in GoogleDirectionsAPI occured: " + str(statusCode)
        }

    fuelconsumption = event["fuel consumption"]
    fuel = event["fuel"]
    
    table = dynamodb.Table("EmissionFactorsAfterDIN16258")
    response = table.get_item(
        Key={
            'fuel_type': fuel  # returns the item of the chosen fuel type in Table EmissionsFactorsAfterDIN16258
        }
    )
    item = response['Item']
    tank_to_wheel = float(item["THG_emissionfactor_TTW_kgCo2e/l"]) / 1000  # convert to ton

    if (fuelconsumption == "-1"):
        VClass=event["VClass"]
        productionYear=event["productionYear"]

        inputForInvoker = {'fuel': fuel, 'VClass': VClass, 'productionYear': productionYear}
        response = client.invoke(
            FunctionName='arn:aws:lambda:eu-central-1:663325156950:function:getCarFuelConsumptionAverage',
            InvocationType='RequestResponse',
            Payload=json.dumps(inputForInvoker),
        )
        responseJson = json.load(response['Payload'])
        body_json = responseJson['body']
        if (fuel == "Electricity"):
            fuelInLPer100KmCity = body_json["electricityInKWHPer100KmCity"]
            fuelInLPer100KmHighway = body_json["electricityInKWHPer100KmHighway"]
            fuelInLPer100KmComb = body_json["electricityInKWHPer100KmComb"]
            fuelconsumption = fuelInLPer100KmComb /100
        else :
            fuelInLPer100KmCity = body_json["fuelInLPer100KmCity"]
            fuelInLPer100KmHighway = body_json["fuelInLPer100KmHighway"]
            fuelInLPer100KmComb = body_json["fuelInLPer100KmComb"]
            fuelconsumption = fuelInLPer100KmComb /100

        fuelInLPerKmCity = fuelInLPer100KmCity / 100  # convert from l/100km to l/km
        fuelInLPerKmHighway = fuelInLPer100KmHighway / 100  # convert from l/100km to l/km
        CO2_äquivalent_city = float(distanceKmCity) * fuelInLPerKmCity * tank_to_wheel  # multiplies distance, fuelconsumption(l/km), co2equivalent of fueltype
        CO2_äquivalent_highway = float(distanceKmHighway) * fuelInLPerKmHighway * tank_to_wheel  # multiplies distance, fuelconsumption(l/km), co2equivalent of fueltype
        CO2_äquivalent = CO2_äquivalent_city + CO2_äquivalent_highway
        if (fuel == "Diesel"):
            CO2_fin_city = float(distanceKmCity) * fuelInLPerKmCity * 2.64/1000  # multiplies distance, fuelconsumption(l/km), co2 of fueltype(in t)
            CO2_fin_highway = float(distanceKmHighway) * fuelInLPerKmHighway * 2.64/1000  # multiplies distance, fuelconsumption(l/km), co2 of fueltype (in t)
            CO2_fin = CO2_fin_city + CO2_fin_highway
            
        if (fuel == "Petrol"):
            CO2_fin_city = float(distanceKmCity) * fuelInLPerKmCity * 2.33/1000  # multiplies distance, fuelconsumption(l/km), co2 of fueltype(in t)
            CO2_fin_highway = float(distanceKmHighway) * fuelInLPerKmHighway * 2.33/1000  # multiplies distance, fuelconsumption(l/km), co2 of fueltype(in t)
            CO2_fin = CO2_fin_city + CO2_äquivalent_highway
    else:
        fuelconsumption = float(fuelconsumption) / 100  # convert from l/100km to l/km
        CO2_äquivalent = float(distance / 1000) * fuelconsumption * tank_to_wheel  # multiplies distance, fuelconsumption(l/km), co2equivalent of fueltype
        if (fuel == "Diesel"):
            CO2_fin = float(distance / 1000) * fuelconsumption * 2.64  # multiplies distance, fuelconsumption(l/km), co2equivalent of fueltype
        if (fuel == "Petrol"):
            CO2_fin = float(distance / 1000) * fuelconsumption * 2.33  # multiplies distance, fuelconsumption(l/km), co2equivalent of fueltype

    #Access to the Database helper function for the specific fuel type
    emission_data = json.loads(database_helper.search_in_database(fuel))
    
    #scope3 ----------------------------------------------------------------
    tableWTT = dynamodb.Table("EmissionFactorsFuel_WTT")
    responseWTT = tableWTT.get_item(
        Key={
            'fuel_type': fuel  # returns the item of the chosen fuel type in Table EmissionsFactorsAfterDIN16258
        }
    )
    itemWTT = responseWTT['Item']
    
    if(fuel == 'Diesel' or fuel == 'Petrol' or fuel == 'Biodiesel' or fuel == 'Ethanol'):
     wtt_Co2 = float(itemWTT["THG_emissionfactor_WTT_kgCo2e/l"]) / 1000  # in kgCo2e/l -> /1000 to get TCo2e 
     wtt_result = wtt_Co2  * float(fuelconsumption) * float(distance / 1000)
     
    if(fuel == 'LPG' or fuel == 'CNG' or fuel == 'LNG'):
     wtt_Co2 = float(itemWTT["THG_emissionfactor_WTT_kgCo2e/kg"])   # in kgCo2e/kg -> /1000 to get TCo2e
     wtt_result = wttCo2 * float(fuelconsumption) * float(distance / 1000)
     
    if(fuel == 'Electricity'): #still to do
     wtt_result = "no Well to Tank Emissions!"
    #-------------------------------------------------------
    
    #Stores the Co2ePerLiter
    Co2ePerLiter = float(emission_data['fuel_values'][0]['THG_emissionfactor_TTW_kgCo2e/l'])
    if (fuel == "Diesel" or fueltype == "Petrol"):
        # call getAllGreenHouseGasOfCO2-Function
        inputForInvoker = {'CO2': CO2_fin, 'fuel': fuel}
        response = client.invoke(
            FunctionName='arn:aws:lambda:eu-central-1:663325156950:function:getAllGreenHouseGasOfCO2',
            InvocationType='RequestResponse',
            Payload=json.dumps(inputForInvoker),
        )
        
        # receive the Payload file and transfer it to Json format
        responseJson = json.load(response['Payload'])
        body_json = responseJson['body']
        # Scope1-TankToWheel-Diesel
        CO = body_json["CO"]
        NOx = body_json["NOx"]
        HC = body_json["HC"]
        json_reply = \
            {
                "scope1": {
                    "CO2_äquivalent": CO2_äquivalent,
                    "CO": CO,
                    "NOx": NOx,
                    "HC": HC
                },
                "scope3": {
                    "WTT": wtt_result
                }
            }
    else:
        json_reply = \
            {"CO2_äquivalent": CO2_äquivalent
             }
    # The Data is stored as a real JSON
    json_dump = json.dumps(json_reply)
    return {
        'statusCode': 200,
        'body': json.loads(json_dump)  # returns co2 in t emission as a float (NOT A JSON!!)
    }