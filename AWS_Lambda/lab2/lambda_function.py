import base64
import boto3

def lambda_handler(event, context):
    net_profit = 0
    for record in event['Records']:
        #This is similar to lab1, with the addition of a base64 decode step.
        content = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        print('Decoded paylod: ', content)
        line = content.split(',')
        try:
            net_profit += int(line[2]) - int(line[3])
        except:
            continue
    print("net_profit: ", str(net_profit))
    return {"net_profit": net_profit}