import json
import boto3
import uuid
from datetime import datetime
import os
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import BadRequestException

MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')
USER_MATRIX_TABLE = os.environ.get('USER_MATRIX_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')
COGNITO_USER_POOL = os.environ.get('COGNITO_USER_POOL')

DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-5')
COGNITO_CLIENT = boto3.client('cognito-idp', region_name='ap-southeast-5')

MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)
USER_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(USER_MATRIX_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)


USER_GROUP_NAMES = ["Checker", "Approver", "Admin", "Axrail Admin"]
MODULES = [{
    "module": "3 Way Matching Agent",
    "subModules": ["Document Extraction", "Matching Result"]
}, {
    "module": "Agents",
    "subModules": ["Agent List"]
}, {
    "module": "AP Invoice Processing",
    "subModules": ["Uploaded Documents", "Extraction Result", "Audit Trails"]
}, {
    "module": "Customers",
    "subModules": ["All Customers"]
}, {
    "module": "User Matrix",
    "subModules": ["Roles", "All Users"]
}]

"""
Sample Event: 
{
  "merchant_name": "AI Agent Platform",
  "user": [{
    "user_group": "Admin",
    "email": "aliaarina@axrail.com",
    "name": "Alia Arina"
  },{
    "user_group": "Admin",
    "email": "yhchia@axrail.com",
    "name": "Nigel"
  }, {
    "user_group": "Checker",
    "email": "aliefdany@axrail.com",
    "name": "Alief"
  }, {
    "user_group": "Approver",
    "email": "kevinlim@axrail.com",
    "name": "Kevin Lim"
  }]
}
"""

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    merchant_name = event.get('merchant_name')
    receiver_email = event.get('receiver_email')
    user_input = event.get('user')
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ') 
    
    userGroupMap = {}
    merchantId = createMerchant(merchant_name, receiver_email, now)
    for userGroup in USER_GROUP_NAMES:
        userGroupId = createUserGroup(merchantId, userGroup, now)
        userGroupMap[userGroup] = userGroupId

    for userGroup in USER_GROUP_NAMES:
        userGroupId = userGroupMap.get(userGroup)
        createUserMatrix(merchantId, userGroupId, userGroup, MODULES, now)

    for user in user_input:
        cognitoUsername = createCognitoUser(user)
        addCognitoUserToGroup(cognitoUsername, user.get('user_group'))
        createUser(user, merchantId, userGroupMap, cognitoUsername, now)
    
    return {
        "statusCode":
        200,
        "body":
        json.dumps("Merchant, UserGroup, UserMatrix and User records created successfully.")
    }

@tracer.capture_method
def createMerchant(merchant_name, receiver_email, now):
    merchant_id = str(uuid.uuid4())
    
    merchant_record = {
        "merchantId": merchant_id,
        "email": receiver_email,
        "createdAt": now,
        "createdBy": "System",
        "name": merchant_name,
        "updatedAt": now,
        "updatedBy": "System"
    }
    
    MERCHANT_DDB_TABLE.put_item(Item=merchant_record)
    return merchant_id

@tracer.capture_method
def createUserGroup(merchantId, userGroupName, now):
    userGroupId = str(uuid.uuid4())
    
    user_group_record = {
        "userGroupId": userGroupId,
        "createdAt": now,
        "createdBy": "System",
        "merchantId": merchantId,
        "totalUser": 1,
        "updatedAt": now,
        "updatedBy": "System",
        "userGroupName": userGroupName
    }
    
    USER_GROUP_DDB_TABLE.put_item(Item=user_group_record)
    return userGroupId

@tracer.capture_method
def createUserMatrix(merchantId, userGroupId, userGroupName, modules, now):
    
    for module in modules:
        if userGroupName == "AxrailAdmin" and module.get('module') != "Customers":
            continue
        if userGroupName == "Admin" and module.get('module') == "Customers":
            continue
        if userGroupName in ["Checker", "Approver"] and module.get('module') not in ["AP Invoice Processing", "3 Way Matching Agent"]:
            continue
        
        for subModule in module.get('subModules'):
            user_matrix_record = {
                "userMatrixId": str(uuid.uuid4()),
                "canAdd": True,
                "canDelete": True,
                "canEdit": True,
                "canList": True,
                "canView": True,
                "createdAt": now,
                "createdBy": "System",
                "merchantId": merchantId,
                "module": module.get('module'),
                "parentUserMatrixId": None,
                "subModule": subModule,
                "updatedAt": now,
                "updatedBy": "System",
                "userGroupId": userGroupId
            }
            USER_MATRIX_DDB_TABLE.put_item(Item=user_matrix_record)


@tracer.capture_method
def createUser(user, merchantId, userGroupMap, cognitoUsername, now):
    userId = str(uuid.uuid4())
    userGroupId = userGroupMap.get(user.get('user_group'))
    
    user_record = {
        "userId": userId,
        "cognitoUsername": cognitoUsername,
        "createdAt": now,
        "createdBy": "System",
        "email": user.get('email'),
        "isDisabled": False,
        "merchantId": merchantId,
        "mobileNo": "",
        "name": user.get('name'),
        "updatedAt": now,
        "updatedBy": "System",
        "userGroupId": userGroupId
    }
    
    USER_DDB_TABLE.put_item(Item=user_record)
    
    return user

@tracer.capture_method
def createCognitoUser(request_body):
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
def addCognitoUserToGroup(cognitoUsername, user_group_name):
    COGNITO_CLIENT.admin_add_user_to_group(
        UserPoolId=COGNITO_USER_POOL,
        Username=cognitoUsername,
        GroupName=user_group_name
    )