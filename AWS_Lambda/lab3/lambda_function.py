import boto3
import datetime
import json
import os

boto3_client = boto3.client('dynamodb')
table = os.environ['TABLE']

def lambda_handler(event, context):
    #Logging the original event that triggers this lambda.
    print(json.dumps(event))
    for record in event['Records']:
        if record['eventName'] != 'INSERT':
            return None
        net = int(record['dynamodb']['NewImage']['gross']['N']) - int(record['dynamodb']['NewImage']['costs']['N'])
        timestamp = str(datetime.datetime.now().astimezone().isoformat())
        print('DynamoDB record recieved, adding timestamp: ', timestamp, ' and net:', str(net))
        #This section took wayy too long than I care to admit due to silly typos.
        #There should be better error handling, but this isn't the place for it.
        try:
            data_write = boto3_client.put_item(
                TableName=table,
                Item={
                    "txid": {'S': record['dynamodb']['Keys']['txid']['S']},
                    "costs": {'N': record['dynamodb']['NewImage']['costs']['N']}, 
                    "gross": {'N': record['dynamodb']['NewImage']['gross']['N']}, 
                    "net": {'N': str(net)}, 
                    "timestamp": {'S': str(timestamp)}
                }
            )
            print('DynamoDB write succeeded with: ', data_write)
        #Let's print any exception at all
        except Exception as e:
            print(e)
