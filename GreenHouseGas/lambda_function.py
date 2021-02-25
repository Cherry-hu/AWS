import json
import boto3

def lambda_handler(event, context):
    fuel = event["fuel"]
    CO2 = event["CO2"]
    if (fuel =='Diesel' or fuel == 'Petrol'):
        if (fuel == 'Diesel'):
            # Scope1-TankToWheel-Diesel
            CO2 = CO2 / 12
            CO = CO2 * 0.8
            NOx = CO2 * 0.12
            HC = CO2 * 0.08
            json_reply = \
                {
                    "CO": CO,
                    "NOx": NOx,
                    "HC": HC
                    
                }
        if (fuel == 'Petrol'):
            # Scope1-TankToWheel-Benzin
            CO2 = CO2 / 8
            CO2 = CO2 / 5
            CO = CO2 * 0.2
            NOx = CO2 * 0.55
            HC = CO2 * 0.1
            json_reply = \
                {
                    "CO": CO,
                    "NOx": NOx,
                    "HC": HC
                    
                }
    else : 
        return {
        'statusCode': 400,
        'body': fuel + "is not allowed"
    }
    
    # The Data is stored as a real JSON
    json_dump = json.dumps(json_reply)
    return {
        'statusCode': 200,
        'body': json.loads(json_dump)  # returns co2 in t emission as a float (NOT A JSON!!)
    }