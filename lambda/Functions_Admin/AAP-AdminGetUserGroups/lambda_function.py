import os
import boto3
import json
from decimal import Decimal
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException
from boto3.dynamodb.conditions import Key

USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

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
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.GET_ROLES.value)
        merchant_id = user.get('merchantId')

        payload = get_user_groups(merchant_id)

        return create_response(200, "Success", payload)
    
    except (AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException, ApiException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(400, e.message)
    
    except Exception as e:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(e))
        logger.exception({"message": str(e)})
        return create_response(500, "The server encountered an unexpected condition that prevented it from fulfilling your request.")

@tracer.capture_method
def get_user_groups(merchant_id):
    formatted_items = []
    
    response = USER_GROUP_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchant_id)
    )

    items = response['Items']
    for item in items:
        formatted_items.append({
            'userGroupId': item['userGroupId'],
            'merchantId': item['merchantId'],
            'name': item['userGroupName'],
            'members': item['totalUser'],
            'createdAt': item['createdAt'],
            'updatedAt': item['updatedAt']
        })

    payload = {
        'items': formatted_items,
        'count': response['Count']
    }
    

    return payload

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
        'body': json.dumps({
            "statusCode": status_code, 
            "message": message, 
            **payload
        }, cls=DecimalEncoder)
    }
