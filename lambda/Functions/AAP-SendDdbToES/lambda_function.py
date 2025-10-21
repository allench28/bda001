import os
import json
import boto3
import requests
import decimal
from requests_aws4auth import AWS4Auth
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.types import TypeDeserializer

ES_DOMAIN_ENDPOINT = os.environ.get('ES_DOMAIN_ENDPOINT')

# Get AWS Credentials
CREDENTIALS = boto3.Session().get_credentials()
ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-5', 'es', session_token=CREDENTIALS.token)
HEADERS = { "Content-Type": "application/json" }
RETRIES = 3

logger = Logger()
tracer = Tracer()

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o) if o % 1 else int(o)
        return super(DecimalEncoder, self).default(o)

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        for record in event['Records']:
            # Get DynamoDB Table Name
            tableName = record['eventSourceARN'].split('/')[1]
            if "-" in tableName:
                tableName = tableName.split('-')[1]
                
            tablePartitionKey = tableName[0].lower() + tableName[1:] + 'Id'
            url = 'https://{}/{}/_doc/'.format(ES_DOMAIN_ENDPOINT, tableName.lower())
            
            if tableName == 'MerchantSignUp':
                tablePartitionKey = 'phoneNumber'
            
            # Get Record Id and Deserialized Record
            recordId = record['dynamodb']['Keys'][tablePartitionKey]['S']
            
            # Remove/Insert/Update Data into Elasticsearch
            if record['eventName'] == 'REMOVE':
                for _ in range(RETRIES):
                    response = requests.delete(url + recordId, auth=AWSAUTH)
                    if response.status_code == 200 or response.status_code == 201:
                        break
                    else:
                        logger.exception({"message": str(response.text)})
            else:
                deserializedRecord = deserializeDdbRecord(record['dynamodb']['NewImage'])
                logger.info(f"Deserialized records: {deserializedRecord}")

                if 'boundingBoxes' in deserializedRecord:
                    deserializedRecord['boundingBoxes'] = json.dumps(deserializedRecord['boundingBoxes'], cls=DecimalEncoder)

                for _ in range(RETRIES):
                    response = requests.put(url + recordId, auth=AWSAUTH, json=deserializedRecord, headers=HEADERS)
                    if response.status_code == 200 or response.status_code == 201:
                        break
                    else:
                        logger.exception({"message": str(response.text)})
        
        return True
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}

@tracer.capture_method
def deserializeDdbRecord(record, type_deserializer=TypeDeserializer()):
    return type_deserializer.deserialize({"M": record})