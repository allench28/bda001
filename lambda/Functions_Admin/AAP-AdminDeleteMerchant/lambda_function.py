import os
import json
import boto3
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
USER_POOL_ID = os.environ.get('USER_POOL_ID')

DDB_RESOURCE = boto3.resource('dynamodb')
COGNITO_CLIENT = boto3.client('cognito-idp')

MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_group = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group, Permission.DELETE_CUSTOMER.value)

        # parameters = event.get('queryStringParameters')

        # if not (parameters and parameters.get('customerId')):
        #     raise Exception('customerId not found in parameters')

        # customer_id = parameters.get('customerId')

        # existing_customer = CUSTOMER_DDB_TABLE.get_item(
        #     Key={'customerId': customer_id}
        # )
        # logger.info(existing_customer)

        # existing_customer = existing_customer['Item']

        # existing_customer_cognito_id = existing_customer.get('cognitoId')
        # delete_cognito_user(existing_customer_cognito_id)
        # delete_ddb_item(customer_id)

        return create_response(200, "Customer successfully deleted (dummy)")
    
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        return create_response(500, f"error: {str(e)}")
    
@tracer.capture_method
def delete_cognito_user(cognito_id):
    COGNITO_CLIENT.admin_delete_user(
        UserPoolId=USER_POOL_ID,
        Username=cognito_id
    )

@tracer.capture_method
def delete_ddb_item(customer_id):
    MERCHANT_DDB_TABLE.delete_item(
        Key={'customerId': customer_id}
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