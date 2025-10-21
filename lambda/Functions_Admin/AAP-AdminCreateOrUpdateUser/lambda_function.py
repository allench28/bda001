import os
import uuid
import boto3
import json
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException, ResourceNotFoundException

COGNITO_USER_POOL = os.environ.get('COGNITO_USER_POOL') 
USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
COGNITO_CLIENT = boto3.client('cognito-idp')

USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, email, _ = is_authenticated(event)
        user = get_user(sub)
        merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        body = json.loads(event.get('body', '{}'))
        user_id = body.get('userId')
        user_group_id = body.get('userGroupId')
        role = body.get('role')

        if not user_id:
            # Create user flow
            has_permission(user_group_name, Permission.CREATE_USER.value)
            cognito_username = create_cognito_user(body)
            # add_cognito_user_to_group(cognito_username, role)

            create_user(body, cognito_username, user_group_id, merchant_id, email, now)
            update_user_group(None, user_group_id, True)
        else:
            # Update user flow
            has_permission(user_group_name, Permission.UPDATE_USER.value)
            user = get_user_resp(user_id)
            if user.get('cognitoUsername') == sub:
                raise BadRequestException('Unable to update your own account!')
            
            old_user_group_id = user.get('userGroupId')
            old_user_group_name = get_user_group(old_user_group_id).get('userGroupName')
            
            update_user(body, user, email, now)
            update_user_group(old_user_group_id, user_group_id, False)
            # update_cognito_user_group(user.get('cognitoUsername'), old_user_group_name, role)

        return create_response(200, 'Successfully created/updated user!')
    
    except (BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException) as ex:
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
def create_user(request_body, cognito_username, user_group_id, merchant_id, email, now):
    payload = {
        'userId': str(uuid.uuid4()),
        'cognitoUsername': cognito_username,
        'userGroupId': user_group_id,
        'mobileNo': "",
        'merchantId': merchant_id,
        'email': request_body.get('email'),
        'name': request_body.get('name'),
        'isDisabled': False,
        'createdAt': now,
        'createdBy': email,
        'updatedAt': now,
        'updatedBy': email,
    }
    USER_DDB_TABLE.put_item(Item=payload)

@tracer.capture_method
def update_user(request_body, user, merchant_username, now):
    userAttr = [
        {
            'Name': 'email',
            'Value':request_body.get('email')
        },
        {
            'Name': 'email_verified',
            'Value': 'True'
        }
    ]

    response = COGNITO_CLIENT.admin_update_user_attributes(
        UserPoolId = COGNITO_USER_POOL,
        Username = user.get('cognitoUsername'),
        UserAttributes = userAttr,
    )

    new_role_id = request_body.get('userGroupId')

    payload = {
        'name': request_body.get('name'),
        'email': request_body.get('email'),
        'userGroupId': new_role_id,
        'updatedAt': now,
        'updatedBy': merchant_username,
    }
    updateExpression = "Set "
    expressionAttributesNames = {}
    expressionAttributesValues = {}

    for key, value in payload.items():
        updateExpression += ", "+ "#" + str(key) + " = :"+str(key) if updateExpression != "Set " else "#" + str(key)+" = :"+str(key)
        expressionAttributesNames['#'+str(key)] = str(key)
        expressionAttributesValues[':'+str(key)] = value

    USER_DDB_TABLE.update_item(
        Key={
            'userId': request_body.get('userId')
        },
        UpdateExpression=updateExpression,
        ExpressionAttributeNames=expressionAttributesNames,
        ExpressionAttributeValues=expressionAttributesValues
    )

@tracer.capture_method
def create_cognito_user(request_body):
    userAttr = [
        {
            'Name': 'email',
            'Value':request_body.get('email')
        },
        {
            'Name': 'email_verified',
            'Value': 'True'
        },
    ]

    checkUserExists(request_body.get('email'))

    response = COGNITO_CLIENT.admin_create_user(
        UserPoolId=COGNITO_USER_POOL,
        Username = request_body.get('email'),
        UserAttributes=userAttr,
        ForceAliasCreation=False,
        DesiredDeliveryMediums=['EMAIL'],
    ).get('User')

    if not response:
        raise BadRequestException('Failed to create cognito user')

    return response.get('Username')

# @tracer.capture_method
# def add_cognito_user_to_group(cognito_username, user_group_name):
#     COGNITO_CLIENT.admin_add_user_to_group(
#         UserPoolId=COGNITO_USER_POOL,
#         Username=cognito_username,
#         GroupName=user_group_name
#     )

# @tracer.capture_method
# def update_cognito_user_group(cognito_username, old_user_group_name, new_user_group_name):
#     COGNITO_CLIENT.admin_remove_user_from_group(
#         UserPoolId=COGNITO_USER_POOL,
#         Username=cognito_username,
#         GroupName=old_user_group_name
#     )

#     COGNITO_CLIENT.admin_add_user_to_group(
#         UserPoolId=COGNITO_USER_POOL,
#         Username=cognito_username,
#         GroupName=new_user_group_name
#     )

@tracer.capture_method
def get_user_resp(user_id):
    user = USER_DDB_TABLE.get_item(Key={'userId': user_id}).get('Item')
    if not user:
        raise BadRequestException('User not found!')
        
    return user

@tracer.capture_method
def checkUserExists(email):
    try:
        cognitoResp = COGNITO_CLIENT.admin_get_user(
            UserPoolId = COGNITO_USER_POOL,
            Username=email
        )
        
    except:
        cognitoResp = None
    
    if cognitoResp:
        raise BadRequestException('User with this Email Exists!')

@tracer.capture_method

@tracer.capture_method
def update_user_group(old_user_group_id, new_user_group_id, is_create):
    if not is_create and old_user_group_id != new_user_group_id:
        oldUserGroupResp = get_user_group(old_user_group_id)
        USER_GROUP_DDB_TABLE.update_item(
            Key={'userGroupId':old_user_group_id},
            UpdateExpression='Set totalUser=:totalUser',
            ExpressionAttributeValues={':totalUser':oldUserGroupResp.get('totalUser')-1}
            )
    
        newUserGroupResp = get_user_group(new_user_group_id)
        USER_GROUP_DDB_TABLE.update_item(
            Key={'userGroupId':new_user_group_id},
            UpdateExpression='Set totalUser=:totalUser',
            ExpressionAttributeValues={':totalUser':newUserGroupResp.get('totalUser')+1}
            )
            
    elif is_create:
        newUserGroupResp = get_user_group(new_user_group_id)
        USER_GROUP_DDB_TABLE.update_item(
            Key={'userGroupId':new_user_group_id},
            UpdateExpression='Set totalUser=:totalUser',
            ExpressionAttributeValues={':totalUser':newUserGroupResp.get('totalUser')+1}
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