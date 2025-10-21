import boto3
import json
import os
import uuid
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key, Attr
from decimal import *
from custom_exceptions import BadRequestError, NotFoundError

DDB_RESOURCE = boto3.resource("dynamodb")

logger = Logger()
tracer = Tracer()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, Decimal):
            return str(obj)
        #️ otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)
        
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        payloadList = []
        for record in event['Records']:
            if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
                deserializedRecord = deserializeDdbRecord(record.get('dynamodb').get('NewImage'))
                
                for key, value in deserializedRecord.items():
                    if type(value) == list or type(value) == dict:
                        deserializedRecord[key] = json.dumps(value, cls=DecimalEncoder)
                    elif type(value) == Decimal:
                        deserializedRecord[key] = float(value) if abs(value.as_tuple().exponent) > 0 else int(value)
                            
                payloadList.append(deserializedRecord)
        
        if payloadList:
            deliveryStreamName = event['Records'][0]['eventSourceARN'].split('/')[1] + 'Delivery'
            dropToFirehose(payloadList, deliveryStreamName)
            tableName = event['Records'][0]['eventSourceARN'].split('/')[1]
            
        else:
            raise BadRequestError('Out Of Scope')

        return {
            'status': True, 
            'message': 'Success'
        }

    except (BadRequestError, NotFoundError) as ex:
        return {'status': False, 'message': str(ex)}
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}

@tracer.capture_method
def dropToFirehose(records, stream, recordType = ""):
    client = boto3.client('firehose')
    try:
        for record in records:
            payload = {}
            payload['Data'] = "{}\n".format(json.dumps(record))
            client.put_record(
                Record = payload,
                DeliveryStreamName = stream
            )

        return True
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")  
        tracer.put_annotation("lambda_name", "dropToFirehose")
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return None
        
@tracer.capture_method
def deserializeDdbRecord(record, type_deserializer=TypeDeserializer()):
    return type_deserializer.deserialize({"M": record})

    