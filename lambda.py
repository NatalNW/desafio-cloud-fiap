import boto3
import urllib
import re

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('dados')

event = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2021-10-05T22:06:22.249Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "AWS:AROAUEEI57RFQI7NEGZIM:user=email@email.com.br"
            },
            "requestParameters": {
                "sourceIPAddress": "189.18.28.22"
            },
            "responseElements": {
                "x-amz-request-id": "5VC05DT432GKZVYA",
                "x-amz-id-2": "4XEfSqPPGS4+iRuX5ScgzLmrV1S1uONWIs1R4RZWPHCpTzOfvTyY5XnnPQZtru5J0GSvW1BimS9SsP/tYJQYG1sSwy+4RY+6"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "9b775336-554d-48f9-a851-d4bfb858d7e0",
                "bucket": {
                    "name": "bucket-cloudcomputing-final-3456",
                    "ownerIdentity": {
                        "principalId": "AV38ZIPLUSX15"
                    },
                    "arn": "arn:aws:s3:::bucket-cloudcomputing-final-3456"
                },
                "object": {
                    "key": "entrada-dados/teste2.json",
                    "size": 87,
                    "eTag": "bcea1abe0941cc217ffb0abf97566170",
                    "sequencer": "00615CCC6154690551"
                }
            }
        }
    ]
}


def handler(event, context):
    event_name = event['Records'][0]['eventName']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file_name = re.sub('entrada-dados/', '', key)

    if 'ObjectCreated' in event_name:
        create(file_name)
    else:
        update(file_name)


def create(item_name):
    try:
        rep = table.put_item(Item={'nomearquivo': item_name, 'ativo': True})

        return rep
    except Exception as e:
        print(e)
        raise e


def update(item_name):
    try:
        rep = table.update_item(Key={
            'nomearquivo': item_name
        },
            UpdateExpression='SET ativo = :val1',
            ExpressionAttributeValues={
                ':val1': False
            })
        
        return rep
    except Exception as e:
        print(e)
        raise e


def show(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])

        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))

        raise e
