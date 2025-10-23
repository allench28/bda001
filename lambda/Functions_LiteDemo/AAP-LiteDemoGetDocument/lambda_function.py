import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
from decimal import Decimal
from boto3.dynamodb.conditions import Key

DOCUMENT_TABLE = os.environ.get('DOCUMENTS_TABLE_NAME')

DDB_RESOURCE = boto3.resource('dynamodb')

DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_TABLE)

logger = Logger()
tracer = Tracer()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        logger.info(event)
        parameters = event.get('queryStringParameters', {})

        if parameters and parameters.get('documentId'):
            extracted_document_id = parameters.get('documentId')
                
            extracted_document = get_document(extracted_document_id)
            return create_response(200, "Success", extracted_document)

        else:
            raise Exception("ID is required")
                    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return create_response(
            500, 
            "The server encountered an unexpected condition that prevented it from fulfilling your request."
        )
    

@tracer.capture_method
def get_document(document_id):
    response = DOCUMENT_DDB_TABLE.get_item(
        Key={'documentId': document_id}
    )

    existing_document = response.get('Item')
    return existing_document

    
@tracer.capture_method
def create_response(status_code, message, payload=None):
    if not payload:
        payload = {}
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Content-Security-Policy': "default-src 'self'; script-src 'self'",
            'X-Content-Type-Options': 'nosniff',
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
            'Cache-control': 'no-store',
            'Pragma': 'no-cache',
            'X-Frame-Options':'SAMEORIGIN'
        },
        'body': json.dumps({"statusCode": status_code, "message": message, **payload}, cls=DecimalEncoder)
    }