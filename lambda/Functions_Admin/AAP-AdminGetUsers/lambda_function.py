import json
import os
import boto3
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException
from boto3.dynamodb.conditions import Attr, Key

USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        current_user = get_user(sub)
        merchant_id = current_user.get('merchantId')
        current_user_group_name = get_user_group(current_user.get('userGroupId')).get('userGroupName')
        role_mapping = fetch_merchant_role_mappings(merchant_id)

        parameters = event.get('queryStringParameters', {})

        if parameters and parameters.get('userId'):
            target_user_id = parameters.get('userId')            
            has_permission(current_user_group_name, Permission.GET_SPECIFIC_USER.value)
            user_details = fetch_single_user_details(target_user_id, merchant_id, role_mapping)

            payload = {
                'Items': [user_details]
            }
            
        else:
            has_permission(current_user_group_name, Permission.GET_ALL_USERS.value)
            all_users = fetch_merchant_users(merchant_id, role_mapping)

            payload = {
                "items": all_users
            }

        return create_response(200, "Success", payload)

    except (AuthorizationException, AuthenticationException, ResourceNotFoundException, BadRequestException) as e:
        logger.error(f'Custom error: {str(e)}')
        return create_response(400, e.message)

    except Exception as e:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(e))
        logger.exception({"message": str(e)})
        return create_response(500, "The server encountered an unexpected condition that prevented it from fulfilling your request.")

@tracer.capture_method
def fetch_merchant_role_mappings(merchant_id):
    role_id_to_name_map = {}
    response = USER_GROUP_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchant_id)
    )

    for role in response['Items']:
        role_id_to_name_map[role['userGroupId']] = role['userGroupName']
    
    return role_id_to_name_map


@tracer.capture_method
def fetch_single_user_details(user_id, merchant_id, user_groups_name_dict):
    response = USER_DDB_TABLE.get_item(
        Key={
            'userId': user_id
        },
    )

    if 'Item' not in response:
        raise ResourceNotFoundException("Agent", user_id)
    
    user_data = response['Item']

    if user_data.get('merchantId') != merchant_id:
        raise AuthorizationException("Not authorized to access this user")    
    
    return {
        'userId': user_data['userId'],
        'merchantId': user_data['merchantId'],
        'email': user_data['email'],
        'role': user_groups_name_dict.get(user_data['userGroupId']),
        'name': user_data['name'],
        'createdAt': user_data['createdAt'],
        'updatedAt': user_data['updatedAt']
    }

@tracer.capture_method
def fetch_merchant_users(merchant_id, role_mapping):
    formatted_items = []
    
    contents = USER_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchant_id)
    )
    users = contents['Items']

    for user_data in users:
        formatted_items.append({
            'userId': user_data['userId'],
            'merchantId': user_data['merchantId'],
            'email': user_data['email'],
            'role': role_mapping.get(user_data['userGroupId']),
            'name': user_data['name'],
            'createdAt': user_data['createdAt'],
            'updatedAt': user_data['updatedAt']
        })

    return formatted_items

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
        })
    }
