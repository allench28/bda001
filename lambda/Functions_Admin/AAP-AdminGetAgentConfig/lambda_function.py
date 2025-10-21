import os
import boto3
import json
from decimal import Decimal
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException
from boto3.dynamodb.conditions import Key

AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
BEDROCK_CLIENT = boto3.client('bedrock-agent')

AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)

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
        user_merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
            
        parameters = event.get('queryStringParameters', {})

        if parameters:
            if parameters.get('agentConfigurationId'):
                agent_id = parameters.get('agentConfigurationId')

                has_permission(user_group_name, Permission.GET_SPECIFIC_AGENT.value)
                item = get_agent(agent_id)

                payload = {
                    "items": [item]
                }

                return create_response(200, "Agent retrieved successfully", payload)

            elif parameters.get('merchantId'):
                merchant_id = parameters.get('merchantId')

                has_permission(user_group_name, Permission.GET_ALL_AGENTS.value)
                formatted_items = get_merchant_agents(merchant_id)
                payload = {
                    'items': formatted_items
                }

                return create_response(200, "Agents retrieved successfully", payload)

            else:
                raise BadRequestException("Invalid query parameters")

        else:
            has_permission(user_group_name, Permission.GET_ALL_AGENTS.value)
            formatted_items = get_all_agents()
            payload = {
                'items': formatted_items
            }

            return create_response(200, "Agents retrieved successfully", payload)
        
    except (AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException, ApiException) as e:
        logger.error(f"Custom error: {str(e)}")
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
def get_all_agents():
    formatted_items = []
    
    contents = AGENT_CONFIGURATION_DDB_TABLE.scan()
    items = contents['Items']

    for item in items:
        formatted_items.append({
            'agentConfigurationsId': item['agentConfigurationsId'],
            'createdAt': item['createdAt'],
            'createdBy': item['createdBy'],
            'updatedAt': item['updatedAt'],
            'updatedBy': item['updatedBy'],
            'name': item['name'],
            'description': item['description'],
            'service': item['service'],
            'configuration': item['configuration'],
        })

    return formatted_items

@tracer.capture_method
def get_merchant_agents(merchant_id):
    formatted_items = []
    
    contents = AGENT_CONFIGURATION_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchant_id),
    )
    items = contents['Items']

    for item in items:
        formatted_items.append({
            'agentConfigurationId': item['agentConfigurationsId'],
            'createdAt': item['createdAt'],
            'createdBy': item['createdBy'],
            'updatedAt': item['updatedAt'],
            'updatedBy': item['updatedBy'],
            'name': item['name'],
            'description': item['description'],
            'service': item['service'],
            'configuration': item['configuration'],
        })

    return formatted_items

@tracer.capture_method
def get_agent(agent_id):
    response = AGENT_CONFIGURATION_DDB_TABLE.get_item(
        Key={
            'agentConfigurationsId': agent_id
        },
    )

    if 'Item' not in response:
        raise ResourceNotFoundException("Agent", agent_id)
    
    item = response['Item']

    # no permissions for bedrock prompt
    # prompt_arn = item['configuration']['systemPrompt']['promptArn']
    # prompt_version = item['configuration']['systemPrompt']['promptVersion']

    # prompt = BEDROCK_CLIENT.get_prompt(
    #     promptIdentifier=prompt_arn,
    #     promptVersion=prompt_version
    # )
    # prompt_text = prompt.get('variants')[0].get('templateConfiguration').get('text').get('text')

    configuration = {
        'contentChecking': item['configuration']['contentChecking'],
        'mappingOneURL': item['configuration']['mappingOneURL'],
        'mappingTwoURL': item['configuration']['mappingTwoURL'],
        'processingFrequencyConfig': item['configuration']['processingFrequencyConfig'],
        'systemPrompt':{
            'promptArn': item['configuration']['systemPrompt']['promptArn'],
            'promptVersion': item['configuration']['systemPrompt']['promptVersion'],
            'promptText': item['configuration']['systemPrompt']['prompt']
        }
    }

    formatted_item = {
        'agentConfigurationsId': item['agentConfigurationsId'],
        'createdAt': item['createdAt'],
        'createdBy': item['createdBy'],
        'updatedAt': item['updatedAt'],
        'updatedBy': item['updatedBy'],
        'name': item['name'],
        'description': item['description'],
        'service': item['service'],
        'configuration': configuration
    }

    return formatted_item
    
@tracer.capture_method
def create_response(status_code, message, payload=None):
    if not payload:
        payload = {}

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',  # Or specify your domain
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Content-Security-Policy': "default-src 'self'; script-src 'self'",
        'X-Content-Type-Options': 'nosniff',
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
        'Cache-control': 'no-store',
        'Pragma': 'no-cache',
        'X-Frame-Options':'SAMEORIGIN'
    }

    return {
        'statusCode': status_code,
        'headers': headers,
        'body': json.dumps({
            "statusCode": status_code, 
            "message": message, 
            **payload
        }, cls=DecimalEncoder)
    }