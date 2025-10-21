import os
import boto3
import json
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger, Tracer
from cacheHelper import getCacheValue, setCacheValue
from identityHelper import getUser
from custom_exceptions import BadRequestError, NotFoundError


DDB_RESOURCE = boto3.resource('dynamodb')


logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:

        return {
            'statusCode': 200,
            'body': json.dumps({'status': True, 'message': 'Hello from Lambda!'})
        }

    except (BadRequestError, NotFoundError) as ex:
        return {
            'statusCode': 400,
            'body': json.dumps({'status': False, 'message': str(ex)})
        }
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {
            'statusCode': 500,
            'body': json.dumps({'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."})
        }