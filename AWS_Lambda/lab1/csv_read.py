import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    srcBucket = event['Records'][0]['s3']['bucket']['name']
    srcKey = event['Records'][0]['s3']['object']['key']
    if srcKey[-3:] != 'csv':
        print(srcKey[-3:])
        error_msg = srcKey + ' Object is not a CSV.'
        return error_msg
    csv_data = s3_client.get_object(Bucket=srcBucket, Key=srcKey)
    raw_data = csv_data['Body'].read().decode('utf-8')
    print(raw_data)
    return {
        'statusCode': 200
    }