import json
import boto3

# -------------------------------------------------------------------------------
# Author: NMunoz, YHu
# This function receives the distance, fuel type (diesel or electricity)  
# and an JSON-Object with the fields Origin, Destination
# It returns co2, NOx and PM emission as a float
# All the route calculation logic is done by Google Directions API
# Example for test: "Origin": "Frankfurt","Destination": "Berlin", "Fuel": "1" -> CO2_fin, NOx_fin, PM_fin
# -------------------------------------------------------------------------------

client = boto3.client('lambda')

def lambda_handler(event, context):
    
  try:
      
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
        
    #Check if fuel is given
    if "fuel" not in event:
        return {
            'statusCode': 400,
            'body': "fuel is not defined!\nPlease enter 1 for diesel or -1 for electricity" 
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
    input_json = responseJson['input']
    body_json = responseJson['body']
    statusCode = responseJson['statusCode']

    if (statusCode == 200):
        distance = body_json["distance"]
        departure_country = input_json["departure_country"]
    else:
        return{
            'statusCode': 400,
            'errorMsg': "FunctionCall of getSpeedValuesBetweenTwoWaypoints did not work!"
        }

    # set parameter fueltype to calculate the emission
    fuel = event['fuel']

    # If fuel = 1 -> calculation for a diesel bus
    if(fuel == "1"):
        try:
            # All the value in grams per person-kilometer
            busCo2Emission = 33
            busNOxEmission = 0.21
            busPMEmission = 0.0044
            
            # Calculete the total emission and transfer gram to ton
            Co2EmissionFinal =  busCo2Emission*distance/1000000
            NOxEmissionFinal = busNOxEmission*distance/1000000
            PMEmissionFinal = busPMEmission*distance/1000000
            
            # response if everything was correctly calcuated
            return{
                'statusCode':200,
                'input':{
                    'Origin': origin,
                    'Destination': destination,
                    'Fuel': "diesel"
                },
                'body':{
                    'Co2EmissionFinal': Co2EmissionFinal,
                    'NOxEmissionFinal': NOxEmissionFinal,
                    'PMEmissionFinal': PMEmissionFinal
                }
            }

        except Exception:
            return{
                 'statusCode':400,
                 'errorMsg': "No valid key found for diesel-calculation!"
            }
    
    # If fuel = -1 -> calculation for an electric bus
    if(fuel == "-1"):
        try:
            # All values in kwh/km
            busConsumption = 1.296
            totalConsumption = busConsumption*distance
            
            inputForInvoker2 = {'country': departure_country, 'kWh': totalConsumption, "green_electricity": 0}
    
            # call calculateEmissionsForElectricityByCountry -Function
            response2 = client.invoke(
                FunctionName='arn:aws:lambda:eu-central-1:663325156950:function:calculateEmissionsForElectricityByCountry',
                InvocationType='RequestResponse',
                Payload=json.dumps(inputForInvoker2),
            )
            
            # receive the Payload file and transfer it to Json format
            responseJson2 = json.load(response2['Payload'])
            body_json2 = responseJson2['body']
            statusCode2 = responseJson2['statusCode']

            
            if(statusCode2 != 200):
                return{
                    "statusCode": 400,
                    "errorMsg": "Error occurred while calling the the LambdaFunction-calculateEmissionsForElectricityByCountry"
                }

            directCO2min = body_json2["directCO2min"]
            directCO2med = body_json2["directCO2med"]
            directCO2max = body_json2["directCO2max"]        
            methaneCO2e = body_json2["methaneCO2e"]
            biogenicCO2e = body_json2["biogenicCO2e"]
            waterConsumptionInL = body_json2["waterConsumptionInL"]


            return{
                'statusCode':200,
                'input':{
                    'Origin': origin,
                    'Destination': destination,
                    'Fuel': "electricity"
                },
                'body':{
                    'directCO2min': directCO2min,
                    'directCO2med': directCO2med,
                    'directCO2max': directCO2max,
                    'methaneCO2e': methaneCO2e,
                    'biogenicCO2e': biogenicCO2e,
                    'waterConsumptionInL': waterConsumptionInL
                }
            }

        except Exception:
            return{
                'statusCode':400,
                'errorMsg': "No valid key found for electricity-calculation!"
            }
    else:
        return{
          'statusCode':400,
          'errorMsg': "Given fuelType is not valid!\nPlease enter 1 for diesel or -1 for electricity"
        }
            



  except KeyError:
        return{
          'statusCode':400,
          'errorMsg': "The given parameters are NOT correct!\nPlease enter a valid Origin or Destination!"
       }

  except Exception:
        return{
           'statusCode': 400,
           'errorMsg': "Unkown Error!"
       }

  
