import os
import boto3
from enum import Enum
from custom_exceptions import AuthorizationException, AuthenticationException
from boto3.dynamodb.conditions import Key

USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

class Permission(Enum):
    GET_ALL_AGENTS = "get_all_agents"
    GET_SPECIFIC_AGENT = "get_specific_agent"
    CREATE_AGENT = "create_agent"
    UPDATE_AGENT = "update_agent"
    DELETE_AGENT = "delete_agent"
    
    GET_ALL_DOCUMENTS = "get_all_documents"
    GET_SPECIFIC_DOCUMENT = "get_specific_document"
    DELETE_DOCUMENT = "delete_document"
    UPDATE_DOCUMENT = "update_document"
    APPROVE_DOCUMENT = "approve_document"

    GET_ALL_USERS = "get_all_users"
    GET_SPECIFIC_USER = "get_specific_user"
    CREATE_USER = "create_user"
    UPDATE_USER = "update_user"
    DELETE_USER = "delete_user"

    GET_ALL_CUSTOMERS = "get_all_customers" 
    GET_SPECIFIC_CUSTOMER = "get_specific_customer"
    CREATE_CUSTOMER = "create_customer"
    UPDATE_CUSTOMER = "update_customer"
    DELETE_CUSTOMER = "delete_customer"

    GET_ROLES = "get_roles"
    GET_SERVICES = "get_services"
    GET_PROMPTS = "get_prompts"
    GENERATE_PRESIGNED_URLS = "generate_presigned_urls"

    GET_RECONCILIATION_DOCUMENTS = "get_reconciliation_documents"
    GET_RECONCILIATION_RESULTS = "get_reconciliation_results"

    GET_THREE_WAY_MATCHING_RESULTS = "get_three_way_matching_results"
    GET_THREE_WAY_MATCHING_DOCUMENTS = "get_three_way_matching_documents"



ROLE_PERMISSIONS = {
    "Axrail Admin": [
        Permission.GET_SERVICES.value,
        Permission.GET_PROMPTS.value,
        Permission.GENERATE_PRESIGNED_URLS.value,
        Permission.GET_ALL_AGENTS.value,
        Permission.GET_SPECIFIC_AGENT.value,
        Permission.CREATE_AGENT.value,
        Permission.UPDATE_AGENT.value,
        Permission.DELETE_AGENT.value,
        Permission.GET_ALL_CUSTOMERS.value,
        Permission.GET_SPECIFIC_CUSTOMER.value,
        Permission.CREATE_CUSTOMER.value,
        Permission.UPDATE_CUSTOMER.value,
        Permission.DELETE_CUSTOMER.value,
        Permission.GENERATE_PRESIGNED_URLS.value,
        Permission.GET_ALL_USERS.value,
        Permission.GET_SPECIFIC_USER.value,
        Permission.CREATE_USER.value,
        Permission.UPDATE_USER.value,
        Permission.DELETE_USER.value,
        Permission.GET_ROLES.value
    ],
    "Admin": [
        Permission.GET_ALL_USERS.value,
        Permission.GET_SPECIFIC_USER.value,
        Permission.CREATE_USER.value,
        Permission.UPDATE_USER.value,
        Permission.DELETE_USER.value,
        Permission.GET_ROLES.value
    ],
    "Checker": [
        Permission.GET_ALL_DOCUMENTS.value,
        Permission.GET_SPECIFIC_DOCUMENT.value,
        Permission.DELETE_DOCUMENT.value,
        Permission.UPDATE_DOCUMENT.value,
        Permission.APPROVE_DOCUMENT.value,
        Permission.GENERATE_PRESIGNED_URLS.value,
        Permission.GET_RECONCILIATION_DOCUMENTS.value,
        Permission.GET_RECONCILIATION_RESULTS.value,
        Permission.GET_THREE_WAY_MATCHING_RESULTS.value,
        Permission.GET_THREE_WAY_MATCHING_DOCUMENTS.value,
    ],
    "Maker": [
        Permission.GET_ALL_DOCUMENTS.value,
        Permission.GET_SPECIFIC_DOCUMENT.value,
        Permission.DELETE_DOCUMENT.value,
        Permission.UPDATE_DOCUMENT.value,
        Permission.APPROVE_DOCUMENT.value,
        Permission.GENERATE_PRESIGNED_URLS.value,
        Permission.GET_RECONCILIATION_DOCUMENTS.value,
        Permission.GET_RECONCILIATION_RESULTS.value,
        Permission.GET_THREE_WAY_MATCHING_RESULTS.value,
        Permission.GET_THREE_WAY_MATCHING_DOCUMENTS.value,
    ]
}

def has_permission(user_group, required_permission):
    if required_permission in ROLE_PERMISSIONS[user_group]:
        return True
    raise AuthorizationException(f"You don't have permission to {required_permission.replace('_', ' ')}")

def is_authenticated(event):
    requestContext = event.get('requestContext', {})
    authorizer = requestContext.get('authorizer', {})
    claims = authorizer.get('claims', {})
    
    sub = claims.get('sub', '')
    email = claims.get('email', '')
    
    groups_str = claims.get('cognito:groups', [])
    user_groups = [g.strip() for g in groups_str.split(',')] if groups_str else []
            
    if not sub:
        raise AuthenticationException()
    
    return sub, email, user_groups

def get_user(sub):
    response = USER_DDB_TABLE.query(
        IndexName='gsi-cognitoUsername',
        KeyConditionExpression='cognitoUsername = :cognitoUsername',
        ExpressionAttributeValues={':cognitoUsername': sub}
    )

    if not response.get('Items'):
        raise AuthenticationException("User not found")
    
    existing_user = response.get('Items')[0]

    return existing_user

def get_user_group(user_group_id):
    response = USER_GROUP_DDB_TABLE.get_item(
        Key={
            'userGroupId': user_group_id
        }
    )

    if not response.get('Item'):
        raise AuthorizationException("User group not found")

    return response['Item']
