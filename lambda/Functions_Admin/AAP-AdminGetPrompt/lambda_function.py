import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN = os.environ.get('DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN', "dummy")
AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
BEDROCK_CLIENT = boto3.client('bedrock-agent')

SERVICE_ACTION_PROMPT_MAPPING = {
    'Invoice Extraction': DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN,
    # Add more mappings as needed
}

DEFAULT_PROMPT = """
You are an advanced document analysis assistant specializing in invoice data extraction.

OBJECTIVE:
Extract structured information from invoice documents while maintaining data integrity and accuracy.

PROCESSING GUIDELINES:
- Identify and extract all standard invoice fields (invoice number, date, total amount, tax, etc.)
- Recognize vendor-specific formatting and adapt extraction accordingly
- Maintain original numerical precision and currency formatting
- Flag any inconsistencies between calculated totals and stated totals
- Preserve hierarchical relationships between line items
- Identify and extract payment terms and due dates

QUALITY ASSURANCE:
For each extracted field, assess confidence level and flag uncertain extractions for human review.

OUTPUT FORMAT:
Return data in a consistent structured format optimized for database integration and downstream processing.
"""

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.GET_PROMPTS.value)

        parameters = event.get('queryStringParameters', {})

        if not (parameters and parameters.get('action')):
            raise BadRequestException('action not found in parameters')

        service_action = parameters.get('action')
        prompt = get_default_prompt(service_action)
        payload = {'prompt': prompt}
        
        return create_response(200, "Success", payload)

    except (AuthenticationException, AuthorizationException, BadRequestException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(400, e.message)

    except Exception as e:
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
def get_default_prompt(service_action: str):
    # try:
    #     prompt_arn = SERVICE_ACTION_PROMPT_MAPPING.get(
    #         service_action, 
    #         DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN  # Default fallback
    #     )
                
    #     try:
    #         response = BEDROCK_CLIENT.get_prompt(promptIdentifier=prompt_arn)
            
    #         if 'variants' not in response or not response['variants']:
    #             raise BadRequestException(f"No variants found for prompt {prompt_arn}")
                
    #         return response['variants'][0]['templateConfiguration']['text']['text']
            
    #     except BEDROCK_CLIENT.exceptions.ResourceNotFoundException:
    #         raise ResourceNotFoundException("Bedrock prompt", prompt_arn)
    #     except Exception as e:
    #         logger.error(f"Error retrieving Bedrock prompt: {str(e)}")
    #         raise Exception(f"Could not get default prompt: {str(e)}")
    
    # except ResourceNotFoundException as e:
    #     raise e
    # except Exception as e:
    #     logger.error(f"Error in get_default_prompt: {str(e)}")
    #     raise Exception(f"Could not get default prompt: {str(e)}")

    return DEFAULT_PROMPT


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
            'Cache-control': 'no-store', # can we change this
            'Pragma': 'no-cache',
            'X-Frame-Options':'SAMEORIGIN'
        },
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }