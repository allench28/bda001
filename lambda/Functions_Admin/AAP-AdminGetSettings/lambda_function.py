import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.GET_SERVICES.value)

        ## TODO: changed to merchant specific

        service_attributes = [
            {
                "actions": [
                    "Invoice Extraction",
                    "3 Way Matching"
                ],
                "type": "Account Payable"
            },
            {
                "actions": [
                    "PO2SO"
                ],
                "type": "Account Receivable"
            }
        ]

        payload = {
            'agentConfigurationId': "Service Settings",
            'createdAt': "2025-03-29T15:36:41.450683Z",
            'createdBy': "2025-03-29T15:36:41.450683Z",
            'serviceAttributes': service_attributes,
        }

        return create_response(200, "Succes", payload)
    
    except (AuthorizationException, AuthenticationException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(400, e.message)
        
    except Exception as e:
        error_id = context.aws_request_id
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(e))
        logger.exception({"message": str(e)})
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
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }
