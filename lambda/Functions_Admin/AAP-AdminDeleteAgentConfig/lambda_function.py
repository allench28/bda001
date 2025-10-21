import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
AGENT_CONFIGURATION_BUCKET = os.environ.get('AGENT_CONFIGURATION_BUCKET')
USER_TABLE = os.environ.get('USER_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3')
EVENT_CLIENT = boto3.client('events') 
BEDROCK_CLIENT = boto3.client('bedrock-agent')

AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.DELETE_AGENT.value)

        request_body = json.loads(event.get('body', '{}'))
        agent_id_list = request_body.get('agentConfigurationIds', [])

        if not agent_id_list:
            raise BadRequestException('ids not found in request body')
        
        total_agent_deleted = delete_agents(agent_id_list)

        return create_response(200, f"Successfully deleted {total_agent_deleted.get('user', 0)} agent(s)")
    
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
def delete_ddb_record(agent_id:str):
    AGENT_CONFIGURATION_DDB_TABLE.delete_item(
        Key={'agentConfigurationsId': agent_id}
    )

@tracer.capture_method
def delete_agents(agent_id_list):
    total_agents_deleted_dict = {}

    for agent_id in agent_id_list:
        delete_s3(agent_id)
        delete_event_bridge(agent_id)
        # delete_prompt(agent_id)
        delete_ddb_record(agent_id)

        if 'user' in total_agents_deleted_dict:
            total_agents_deleted_dict['user'] += 1
        else:
            total_agents_deleted_dict['user'] = 1

    return total_agents_deleted_dict

@tracer.capture_method
def get_agent_merchant_id(agent_id):
    agent = AGENT_CONFIGURATION_DDB_TABLE.get_item(
        Key={
            'agentConfigurationsId': agent_id
        }
    ).get('Item')

    if not agent:
        raise BadRequestException('Agent not found!')
    
    return agent.get('merchantId')

@tracer.capture_method
def delete_s3(agent_id: str):
    try:
        prefix = f'agents/{agent_id}/'

        response = S3_CLIENT.list_objects_v2(Bucket=AGENT_CONFIGURATION_BUCKET, Prefix=prefix)

        if 'Contents' in response:
            for object in response['Contents']:
                S3_CLIENT.delete_object(Bucket=AGENT_CONFIGURATION_BUCKET, Key=object['Key'])
        else:
            raise ResourceNotFoundException('s3 folder', agent_id)

    except Exception as e:
        logger.error(f"Failed to delete mapping files for agent: {agent_id} : {str(e)}")

@tracer.capture_method
def delete_event_bridge(agent_id:str):
    try:
        rule_name = f"AAP-agent-schedule-{agent_id}"

        targets_response = EVENT_CLIENT.list_targets_by_rule(
            Rule=rule_name
        )

        if targets_response.get('Targets'):
            target_ids = [target['Id'] for target in targets_response['Targets']]
            EVENT_CLIENT.remove_targets(
                Rule=rule_name,
                Ids=target_ids
            )

        EVENT_CLIENT.delete_rule(Name=rule_name)

    except Exception as e:
        logger.error(f"Failed to delete eventbridge rule for agent: {agent_id} : {str(e)}")
    
# @tracer.capture_method
# def delete_prompt(agent_id):
#     try:
#         response = AGENT_CONFIGURATION_DDB_TABLE.get_item(
#             Key={
#                 'agentConfigurationsId': agent_id
#             }
#         )

#         prompt_arn = response['Item'].get('configuration').get('systemPrompt').get('promptArn')

#         # BEDROCK_CLIENT.delete_prompt(
#         #     promptIdentifier=prompt_arn
#         # )

#     except Exception as e:
#         logger.error(f"Failed to delete prompt for agent: {agent_id} : {str(e)}")

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