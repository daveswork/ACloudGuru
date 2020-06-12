import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    srcBucket = event['Records'][0]['s3']['bucket']['name']
    srcKey = event['Records'][0]['s3']['object']['key']
    #Verify that the file is csv, by checking the extension
    if srcKey[-3:] != 'csv':
        print(srcKey[-3:])
        error_msg = srcKey + ' Object is not a CSV.'
        return error_msg
    csv_data = s3_client.get_object(Bucket=srcBucket, Key=srcKey)
    #Read the CSV file and parse the records.
    raw_data = csv_data['Body'].read().decode('utf-8')
    lines = raw_data.split('\n')
    total = 0
    for line in lines:
        record = line.split(',')
        try:
            total += int(record[2]) - int(record[3])
        except:
            continue
    #Output the net profits
    return {
        'net_profits': total
    }