import json
import csv
import boto3

def dynamodb_push(reader,region):
    #using SD
    dynamodb=boto3.client('dynamodb',region_name=region)
    
    #To reduce complexity we will use list comprehension and other method also
    for row in reader:
        add_to_db=dynamodb.put_item(
            TableName='s3_table',
            Item={
                'id':{'N':str(row[0])},
                'company':{'S':str(row[1])},
                'location':{'S':str(row[2])},
                'profit':{'N':str(row[3])}
            }
            )
    print("Data Write Successfully !")

def lambda_handler(event, context):
    region='us-east-2'
    record_list=list()
    try:
        #using SDK
        s3=boto3.client('s3')
        
        #getting Bucket details and File_name from event
        bucket=event['Records'][0]['s3']['bucket']['name']
        key=event['Records'][0]['s3']['object']['key']
        
        #start Reading CSV File
        record_list=s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8').split('\n')
        
        csv_reader=csv.reader(record_list,delimiter=',',quotechar='"')
        
        # calling Data Push function
        
        dynamodb_push(csv_reader,region)
        
    except Exception as error:
        print("Error :",error)
        
    return {
        'statusCode': 200,
        'body': json.dumps('dyanomodb push successfully')
    }
