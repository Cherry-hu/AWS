import json
from datetime import datetime
import boto3

from boto3.dynamodb.conditions import Key, Attr

#-------------------------------------------------------------------------------
#Author: SVincenti, ABusch
#Template for accessing the database more efficiently.
#Accessess the S3 Bucket for the fuel types, which are available faster, therefore
#we minimize the DDB calls which are expensive and slow(er).
#You can set the intervall in which the database should be checked for updates. 
#-------------------------------------------------------------------------------

#Set the ressources and database
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("EmissionFactorsAfterDIN16258")

#Sets the intervall in which the database should be checked
database_check_intervall = 15 
database_check_unit = 60 # 60 is Minutes, 3600 for hours, 86400 for days


def search_in_database(fuel_type):
    #Set the file name as variable. 
    fuel_file_name = fuel_type + '.json'
    
    #Source: https://stackoverflow.com/questions/40336918/how-to-write-a-file-or-data-to-an-s3-object-using-boto3
    s3 = boto3.resource("s3").Bucket("emissionfactorsbucket")
    json.load_s3 = lambda f: json.load(s3.Object(key=f).get()["Body"])
    json.dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=json.dumps(obj))
    
    
    #Database search function, which is called later, to get the data
    #directly from DynamoDB
    def get_data_from_database():
        #Try to find the fuel Type in the Database...
            try:
                fuelQuery = table.query(KeyConditionExpression=Key('fuel_type').eq(fuel_type))
                fuelResponse = fuelQuery['Items']
                #...If it is not found, throw an Error 
                file_content = {
                    "timestamp" : datetime.now(),
                    "fuel_values" : fuelResponse
                }
                return json.dumps(file_content, default=str)
            #If the value is not found in the database
            except IndexError:
                        return {
                            'statusCode': 400,
                            'body' : { 
                                'error': "fuel type not found in Database",
                                'wrongFuelType': fuel_type 
                            }
                        }
                
        
    
     
   
    #Try to access the data in the Bucket, in case the file does not exist yet,
    #an Exception will be thrown.
    try: 
        print("File found.")
        data = json.load_s3(fuel_file_name)
        
        #Checking the timestamp in the available data and comparing it to the
        #actual time, to see how much time has passed.
        timestamp_data = datetime.strptime(json.loads(data)["timestamp"], '%Y-%m-%d %H:%M:%S.%f')
        timestamp_now = datetime.now()
        difference = timestamp_now - timestamp_data
        difference = divmod(difference.total_seconds(), database_check_unit)[0]
        
        #If the time passed is above our treshhold, we raise an Exception to go
        #directly to the Exception block, in which the database is called and 
        #saved. 
        if (difference > database_check_intervall):
            print("Time is passed.")
            raise Exception
            
        #If data is not too old and we can find it in the bucket, we return it. 
        return data
        
    #The Exception block is called in case the value is not stored in S3 already
    #or if we jump to it when the Time is passed. 
    except:
        #If it cant be accessed, it is not there yet or it's to old, therefore we create it. 
        print("File not found or too old.")
        data = get_data_from_database()
        json.dump_s3(data, fuel_file_name)
        #Return the data directly from the database. 
        return data
    
    
    
    
    
    
    
    
    
    
    

