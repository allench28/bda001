import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import is_authenticated, Permission, has_permission, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException

USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')
COGNITO_USER_POOL = os.environ.get('COGNITO_USER_POOL')

DDB_RESOURCE = boto3.resource('dynamodb')
COGNITO_CLIENT = boto3.client('cognito-idp')

USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_id = user.get('userId')
        merchant_id = user.get('merchantId')
        user_group = get_user_group(user.get('userGroupId')).get('userGroupName')

        has_permission(user_group, Permission.DELETE_USER.value)
        user = get_user(sub)
        

        request_body = json.loads(event.get('body', '{}'))
        user_list = request_body.get('userIds')

        if not user_list:
            BadRequestException('ids not found in request body')

        if user_id in user_list:
            raise BadRequestException("User does not have permission.")
        cognito_username_list, user_group_changes = delete_users(user_list, merchant_id)
        delete_cognito_user(cognito_username_list)
        updateUserGroup(user_group_changes)
        
        return create_response(200, "User(s) deleted successfully.")
    
    except (BadRequestException, AuthorizationException, AuthenticationException) as ex:
        logger.error(f"Custom error: {str(ex)}")
        return create_response(400, ex.message)
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
def delete_users(user_id_list, merchant_id):
    cognito_username_list = []
    user_group_changes = {}
    for userId in user_id_list:
        user = USER_DDB_TABLE.get_item(Key={'userId': userId}).get('Item')
        if user:
            if user.get('merchantId') != merchant_id:
                raise AuthorizationException("You are not authorized to delete this user")
            
            cognito_username_list.append(user.get('email'))
            USER_DDB_TABLE.delete_item(
                Key={'userId': userId}
            )
            if user.get('userGroupId') in user_group_changes:
                user_group_changes[user.get('userGroupId')] += 1
            else:
                user_group_changes[user.get('userGroupId')] = 1
        else:
            raise BadRequestException('User not found!')

    return cognito_username_list, user_group_changes

@tracer.capture_method
def delete_cognito_user(cognito_username_list):
    for cognitoUsername in cognito_username_list:
        COGNITO_CLIENT.admin_delete_user(
            UserPoolId=COGNITO_USER_POOL,
            Username=cognitoUsername
        )
        
@tracer.capture_method
def get_user_group(user_group_id):
    userGroup = USER_GROUP_DDB_TABLE.get_item(Key={'userGroupId': user_group_id}).get('Item')
    if not userGroup:
        raise BadRequestException('User Group not found!')
        
    return userGroup
        
@tracer.capture_method
def updateUserGroup(user_group_changes):
    for user_group_id, total_user_deleted in user_group_changes.items():
        user_group = get_user_group(user_group_id)
        USER_GROUP_DDB_TABLE.update_item(
            Key={'userGroupId':user_group_id},
            UpdateExpression='Set totalUser=:totalUser',
            ExpressionAttributeValues={':totalUser':user_group.get('totalUser')-total_user_deleted}
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
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }