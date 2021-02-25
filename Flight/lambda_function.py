import json
import boto3
import requests
import database_helper

#-------------------------------------------------------------------------------
# Author: JBoba, NMunoz, YHu
# This function receives an JSON-Object with the Fields fromIATA and toIATA 
# (An IATA is a 3 Digit abbreviation of an Airport) and number of passengers
# It returns co2 emission in tons as a float
# Example for test: {"airplane_name": "A380", "fromIATA": "BER", "toIATA": "FRA","passenger": 2} -> CO2_in_t
#-------------------------------------------------------------------------------

dynamodb = boto3.resource('dynamodb')
client = boto3.client('lambda')

def lambda_handler(event, context):
    #Check if airplane is given
    if "airplane_name" not in event:
        return {
            'statusCode': 400,
            'body': "airplane name is not defined!" 
        }
        
    #Check if passenger is given
    if "passenger" not in event:
        passenger = 1
    else:
        passenger = event["passenger"]
        
    airplane_name = event["airplane_name"]

    # invoke the function calculateDistanceAirports
    response = client.invoke(
        FunctionName='arn:aws:lambda:eu-central-1:663325156950:function:calculateDistanceBetweenAirports',
        InvocationType='RequestResponse',
        Payload=json.dumps(event),
    )
    # receive the Payload file and transfer it to Json format
    responseJson = json.load(response['Payload'])
    body_json = responseJson['body']
    distance = body_json['distanceInKM']
    
    inputForInvoker = {'distance': distance, 'airplane_name': airplane_name}
    
    # invoke the function getAirplaneEmissionsByAirplaneIdentifier
    response = client.invoke(
        FunctionName='arn:aws:lambda:eu-central-1:663325156950:function:getAirplaneEmissionsByAirplaneIdentifier',
        InvocationType='RequestResponse',
        Payload=json.dumps(inputForInvoker),
    )
    # receive the Payload file and transfer it to Json format
    responseJson = json.load(response['Payload'])
    body_json = responseJson['body']
    Flight_Emission_json = body_json['Flight_Emission']
    body_json_LTO = body_json['LTO_Emission']
    statusCode = responseJson['statusCode']
    
    table = dynamodb.Table("AircraftCapacity")
    response = table.get_item(
        Key={
            'Aircraft': airplane_name  # returns the item of the chosen aircraft
        }
    )
    item = response['Item']
    seatsofplane = item["StandardSeating"] 

    if (statusCode == 200):
        Co2_kg = Flight_Emission_json['Co2_kg'] * float(passenger) / float(seatsofplane)
        NOx_kg = Flight_Emission_json['NOx_kg'] * float(passenger) / float(seatsofplane)
        SOx_kg = Flight_Emission_json['SOx_kg'] * float(passenger) / float(seatsofplane)
        H2O_kg = Flight_Emission_json['H2O_kg'] * float(passenger) / float(seatsofplane)
        CO_kg = Flight_Emission_json['CO_kg']   * float(passenger) / float(seatsofplane)
        HC_kg = Flight_Emission_json['HC_kg']   * float(passenger) / float(seatsofplane)
        PM_kg = Flight_Emission_json['PM_Total_kg']   * float(passenger) / float(seatsofplane)
        
        Co2_kg_LTO = body_json_LTO['Co2_kg'] * float(passenger) / float(seatsofplane)
        NOx_kg_LTO = body_json_LTO['NOx_kg'] * float(passenger) / float(seatsofplane)
        SOx_kg_LTO = body_json_LTO['SOx_kg'] * float(passenger) / float(seatsofplane)
        H2O_kg_LTO = body_json_LTO['H2O_kg'] * float(passenger) / float(seatsofplane)
        CO_kg_LTO = body_json_LTO['CO_kg']   * float(passenger) / float(seatsofplane)
        HC_kg_LTO = body_json_LTO['HC_kg']   * float(passenger) / float(seatsofplane)
        PM_kg_LTO = body_json_LTO['PM_Total_kg']   * float(passenger) / float(seatsofplane)
        
        json_Flight_Emission_reply = \
            {"Co2_kg": Co2_kg,
            "NOx_kg": NOx_kg,
            "SOx_kg": SOx_kg,
            "H2O_kg": H2O_kg,
            "HC_kg": HC_kg,
            "PM_kg": PM_kg
            }
             
        json_LTO_reply = \
            {"Co2_kg": Co2_kg_LTO,
            "NOx_kg": NOx_kg_LTO,
            "SOx_kg": SOx_kg_LTO,
            "H2O_kg": H2O_kg_LTO,
            "HC_kg": HC_kg_LTO,
            "PM_kg": PM_kg_LTO
            }
        
        # The Data is stored as JSON format
        json_dump = json.dumps(json_Flight_Emission_reply)
        json_dump_LTO = json.dumps(json_LTO_reply)
        return {
            'statusCode': 200,
            'Flight_Emission': json.loads(json_dump),
            'LTO_Emission': json.loads(json_dump_LTO)
             }
    else:
        # TODO: All kind of error codes
        return{
            'statusCode': 400,
            'body' : "Wrong abbreviation of airport: " + str(statusCode)  
        }




