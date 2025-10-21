import json
import os
import boto3
from decimal import Decimal
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user_group, get_user
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException
from boto3.dynamodb.conditions import Key

MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

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
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_group = get_user_group(user.get('userGroupId')).get('userGroupName')
        
        parameters = event.get('queryStringParameters', {})
        
        if parameters and parameters.get('merchantId'):
            has_permission(user_group, Permission.GET_SPECIFIC_CUSTOMER.value)
            customer_id = parameters.get('merchantId')
            
            response = MERCHANT_DDB_TABLE.get_item(
                Key={
                    'merchantId': customer_id
                },
            )

            if not response['Item']:
                raise Exception("No merchant with ID")
        
            payload = {
                "items": [response['Item']]
            }
            
        else:
            has_permission(user_group, Permission.GET_ALL_CUSTOMERS.value)
            response = MERCHANT_DDB_TABLE.scan()

            payload = {
                "items": response['Items'],
                'count': response['Count']
            }

        return create_response(200, "Success", payload)

    except (AuthenticationException, AuthorizationException, BadRequestException) as e:
        logger.error(f'Custom error: {str(e)}')
        return create_response(400, e.message)
    
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