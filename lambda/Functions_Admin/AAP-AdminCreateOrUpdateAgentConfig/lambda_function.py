import os
import boto3
import json
import uuid
import io
import pandas as pd
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from validation.validation import validate_request
from validation.models import AgentConfigRequest
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException, ResourceNotFoundException
from botocore.exceptions import ClientError

AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
AGENT_CONFIGURATION_BUCKET = os.environ.get('AGENT_CONFIGURATION_BUCKET')
EMAIL_POLLING_LAMBDA_ARN = os.environ.get('EMAIL_POLLING_LAMBDA_ARN')
AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION')
AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID')
USER_TABLE = os.environ.get('USER_TABLE')
BDA_CONFIGURATION_TABLE = os.environ.get('BDA_CONFIGURATION_TABLE')
INBOX_MONITORING_TABLE = os.environ.get('INBOX_MONITORING_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3')
LAMBDA_CLIENT = boto3.client('lambda')
COGNITO_CLIENT = boto3.client('cognito-idp')
EVENT_CLIENT = boto3.client('events')

AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
BDA_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(BDA_CONFIGURATION_TABLE)
INBOX_MONITORING_DDB_TABLE = DDB_RESOURCE.Table(INBOX_MONITORING_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, email, _ = is_authenticated(event)
        user = get_user(sub)
        user_group = get_user_group(user.get('userGroupId'))
        user_group_name = user_group.get('userGroupName')
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        request_body = json.loads(event.get('body', '{}'))

        if not request_body:
            raise BadRequestException("Request body is empty")
        
        if not request_body.get('agentConfigurationsId'):
            has_permission(user_group_name, Permission.CREATE_AGENT.value)
            agent_id = str(uuid.uuid4())

            valid_request = validate_request(request_body, is_update=False)
            valid_request = create_prompt_details(agent_id, valid_request, now)
            
            create_agent(agent_id, valid_request, email, now)

            return create_response(200, "Agent successfully created")
        else:
            has_permission(user_group_name, Permission.UPDATE_AGENT.value)
            agent_id = request_body.get('agentConfigurationsId')

            valid_request = validate_request(request_body, is_update=True)
            valid_request = update_prompt_details(agent_id, valid_request, now)

            update_agent(agent_id, valid_request, email, now)

            return create_response(200, "Agent successfully updated")
    
    except (AuthenticationException, AuthorizationException, BadRequestException, ResourceNotFoundException) as e:
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
def create_inbox_monitoring_record(merchant_id, user, valid_request, timestamp):
    inbox_monitoring_item = {
        'inboxMonitoringId': str(uuid.uuid4()),
        'merchantId': merchant_id,
        'createdAt': timestamp,
        'createdBy': user,
        'updatedAt': timestamp,
        'updatedBy': user,
        'documentType': 'invoice',
        'email': valid_request.configuration.processing_frequency_config.email_recipients[0],
    }

    INBOX_MONITORING_DDB_TABLE.put_item(Item=inbox_monitoring_item)

@tracer.capture_method
def create_bda_configuration_record(merchant_id, user, timestamp):
    bda_config_item = {
        'bDAConfigurationId': str(uuid.uuid4()),
        'merchantId': merchant_id,
        'createdAt': timestamp,
        'createdBy': user,
        'updatedAt': timestamp,
        'updatedBy': user,
        'bdaProjectType': 'invoice',
        'profileId': 'us.data-automation-v1',
        'projectId': '72fc4b0a906b',
    }

    BDA_CONFIGURATION_DDB_TABLE.put_item(Item=bda_config_item)

@tracer.capture_method
def create_agent(agent_id, valid_request: AgentConfigRequest, user, timestamp):
    process_mapping_files(valid_request, agent_id)

    agent_item = {
        'agentConfigurationsId': agent_id,
        'createdAt': timestamp,
        'createdBy': user,
        'updatedAt': timestamp,
        'updatedBy': user,
        'name': valid_request.name,
        'description': valid_request.description,
        'service': valid_request.service.to_dict(),
        'configuration': valid_request.configuration.to_dict(),
        'activeStatus': valid_request.active_status,
        'merchantId': valid_request.merchantId
    }

    AGENT_CONFIGURATION_DDB_TABLE.put_item(Item=agent_item)
    create_inbox_monitoring_record(valid_request.merchantId, user, valid_request, timestamp)
    create_bda_configuration_record(valid_request.merchantId, user, timestamp)
    # create_update_eventbridge(agent_id, valid_request, is_update=False)


@tracer.capture_method
def update_agent(agent_id, valid_request: AgentConfigRequest, user, timestamp):
    process_mapping_files(valid_request, agent_id)

    existing_item = AGENT_CONFIGURATION_DDB_TABLE.get_item(
        Key={'agentConfigurationsId': agent_id}
    ).get('Item')

    if not existing_item:
        raise ResourceNotFoundException("Agent configuration", agent_id)

    updated_item = {
        'createdAt': existing_item['createdAt'],
        'createdBy': existing_item['createdBy'],
        'updatedAt': timestamp,
        'updatedBy': user,
        'name': valid_request.name,
        'description': valid_request.description,
        'service': valid_request.service.to_dict(),
        'configuration': valid_request.configuration.to_dict(),
        'activeStatus': valid_request.active_status,
        'merchantId': existing_item['merchantId']
    }

    updateExpression = "Set "
    expressionAttributesNames = {}
    expressionAttributesValues = {}

    for key, value in updated_item.items():
        updateExpression += ", "+ "#" + str(key) + " = :"+str(key) if updateExpression != "Set " else "#" + str(key)+" = :"+str(key)
        expressionAttributesNames['#'+str(key)] = str(key)
        expressionAttributesValues[':'+str(key)] = value

    AGENT_CONFIGURATION_DDB_TABLE.update_item(
        Key={
            'agentConfigurationsId': agent_id
        },
        UpdateExpression=updateExpression,
        ExpressionAttributeNames=expressionAttributesNames,
        ExpressionAttributeValues=expressionAttributesValues
    )
    
    # create_update_eventbridge(agent_id, valid_request, is_update=True)

    
@tracer.capture_method
def process_mapping_files(valid_request: AgentConfigRequest, agent_id):
    mapping_one = valid_request.configuration.mapping_one_url
    mapping_two = valid_request.configuration.mapping_two_url

    if not mapping_one and not mapping_two:
        raise BadRequestException("Both supplier and item mapping files are required")

    mapping_paths = {
        'mapping_one': mapping_one,
        'mapping_two': mapping_two
    }

    file_paths = [path for path in mapping_paths.values() if path]
    verify_files_exist(file_paths)
    verify_file_contents(file_paths)
    
    perm_mapping_paths = move_files_to_permanent(agent_id, mapping_paths)

    valid_request.configuration.mapping_one_url = perm_mapping_paths.get('mapping_one')
    valid_request.configuration.mapping_two_url = perm_mapping_paths.get('mapping_two')

    
@tracer.capture_method
def verify_files_exist(file_paths):
    for path in file_paths:
        file_name = path.split('/')[-1]
        try:
            S3_CLIENT.head_object(
                Bucket=AGENT_CONFIGURATION_BUCKET,
                Key=path
            )
        except S3_CLIENT.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"Mapping file not found: {path}")
                raise ResourceNotFoundException("Mapping file", file_name)
            else:
                logger.error(f"S3 error when checking file: {path}, error: {str(e)}")
                raise Exception(f"Error accessing mapping files")
    
    return True


@tracer.capture_method
def verify_file_contents(file_paths):
    for file_path in file_paths:
        response = S3_CLIENT.get_object(
            Bucket=AGENT_CONFIGURATION_BUCKET,
            Key=file_path
        )
        csv_content = response['Body'].read().decode('utf-8')

        if not csv_content.strip():
            logger.error(f"File {file_path} is empty")
            raise BadRequestException("One or more mapping files is empty")

        try:
            df = pd.read_csv(io.StringIO(csv_content))
        except Exception as csv_error:
            logger.error(f"Failed to parse CSV file {file_path}: {str(csv_error)}")
            raise BadRequestException("One or more mapping files is not a valid CSV")

        if df.empty:
            logger.error(f"File {file_path} has no data rows")
            raise BadRequestException("One or more mapping files has no data rows")
        
        df_headers = list(df.columns)
        has_code = any("code" in header.lower() for header in df_headers)
        has_name_or_desc = any("name" in header.lower() or "description" in header.lower() for header in df_headers)
        
        if not has_code or not has_name_or_desc:
            logger.error(f"File {file_path} is missing required columns. Headers found: {df_headers}")
            raise BadRequestException("Mapping files must contain required columns")

    return True


@tracer.capture_method
def move_files_to_permanent(agent_id, file_paths):
    permanent_paths = {}
    moved_files = []

    for file_type, path in file_paths.items():
        folder_name = path.split('/', 1)[0]
        if folder_name == "temp":
            original_filename = path.split('/')[-1]
            permanent_path = f"agents/{agent_id}/mappings/{file_type}/{original_filename}"
            S3_CLIENT.copy_object(
                Bucket=AGENT_CONFIGURATION_BUCKET,
                CopySource={'Bucket': AGENT_CONFIGURATION_BUCKET, 'Key': path},
                Key=permanent_path
            )
            permanent_paths[file_type] = permanent_path
            moved_files.append(path)
        else:
            permanent_paths[file_type] = path

    for temp_path in moved_files:
        S3_CLIENT.delete_object(
            Bucket=AGENT_CONFIGURATION_BUCKET,
            Key=temp_path
        )

    return permanent_paths

    
@tracer.capture_method
def generate_schedule_expression(frequency_type, frequency_value):
    frequency_type = frequency_type.upper()
    frequency_value = int(frequency_value)

    if frequency_type == 'MINUTES':
        frequency_value = max(1, frequency_value)
        plural = 's' if frequency_value > 1 else ''
        return f"rate({frequency_value} minute{plural})"
        
    elif frequency_type == 'HOURS':
        plural = 's' if frequency_value > 1 else ''
        return f"rate({frequency_value} hour{plural})"
        
    elif frequency_type == 'DAYS':
        plural = 's' if frequency_value > 1 else ''
        return f"rate({frequency_value} day{plural})"
    
    else:
        logger.error(f"Unknown frequency type: {frequency_type}, defaulting to hourly")
        return 'rate(1 hour)'


@tracer.capture_method
def create_update_eventbridge(agent_id, valid_request, is_update):
    processing_config = valid_request.configuration.processing_frequency_config

    schedule_expression = generate_schedule_expression(
            processing_config.trigger_frequency_type, 
            processing_config.trigger_frequency_value
        )
    
    rule_name = f"AAP-agent-schedule-{agent_id}"
    
    rule_params = {
        'Name': rule_name,
        'Description': f"Schedule for AAP agent: {agent_id}",
        'ScheduleExpression': schedule_expression,
        'State': 'DISABLED'
    }

    EVENT_CLIENT.put_rule(**rule_params)
    
    target_params = {
        'Rule': rule_name,
        'Targets': [
            {
                'Id': f"{rule_name}-target",
                'Arn': EMAIL_POLLING_LAMBDA_ARN
            }
        ]
    }
    
    EVENT_CLIENT.put_targets(**target_params)
    
    if not is_update:
        add_lambda_permission(EMAIL_POLLING_LAMBDA_ARN, rule_name)

@tracer.capture_method
def add_lambda_permission(lambda_arn, rule_name):
    lambda_name = lambda_arn.split(':')[-1]
    
    params = {
        'Action': 'lambda:InvokeFunction',
        'FunctionName': lambda_name,
        'Principal': 'events.amazonaws.com',
        'SourceArn': f"arn:aws:events:{AWS_DEFAULT_REGION }:{AWS_ACCOUNT_ID}:rule/{rule_name}",
        'StatementId': f"{rule_name}-permission"
    }

    LAMBDA_CLIENT.add_permission(**params)


@tracer.capture_method
def create_prompt_details(agent_id, valid_request, timestamp):
    prompt_name = f"AAP-agent-{agent_id}"
    prompt_arn = f"AAP-agent-{agent_id}-arn"
    prompt_version = "1"
    # description = f"Created at: {timestamp}"
    # prompt = valid_request.configuration.system_prompt

    # variant = build_prompt_variant(agent_id, prompt)

    # create_response = BEDROCK_CLIENT.create_prompt(
    #     name=prompt_name,
    #     description=description,
    #     tags={
    #         'PROJECT-NAME': 'AI-AGENT-PLATFORM',
    #         'TO-RETAIN': 'YES'
    #     },
    #     variants=[variant]
    # )

    # prompt_arn = create_response.get('arn')
    
    # version_response = BEDROCK_CLIENT.create_prompt_version(
    #     promptIdentifier=prompt_arn
    # )

    # prompt_version = version_response.get('version') # always 1

    valid_request.configuration.system_prompt = {
        'promptArn': prompt_arn,
        'promptVersion': prompt_version,
        'prompt': valid_request.configuration.system_prompt
    }

    return valid_request


@tracer.capture_method
def update_prompt_details(agent_id, valid_request, timestamp):
    # prompt_name = f"AAP-agent-{agent_id}"
    # description = f"Created at: {timestamp}"

    prompt = valid_request.configuration.system_prompt.get('updatedPrompt')
    prompt_arn = valid_request.configuration.system_prompt.get('promptArn')
    prompt_version = int(valid_request.configuration.system_prompt.get('promptVersion'))+1

    valid_request.configuration.system_prompt = {
        'promptArn': prompt_arn,
        'promptVersion': str(prompt_version),
        'prompt': prompt
    }

    # variant = build_prompt_variant(agent_id, prompt)

    # BEDROCK_CLIENT.update_prompt(
    #     name=prompt_name,
    #     promptIdentifier=prompt_arn,
    #     description=description,
    #     variants=[variant]
    # )
    # try: 
    #     response = BEDROCK_CLIENT.create_prompt_version(
    #         promptIdentifier=prompt_arn
    #     )

    # except ClientError as e:
    #     if e.response['Error']['Code'] == 'ValidationException':
    #         logger.error(f"Failed to update prompt: {str(e)}")
    #         if 'max-number-versions-per-prompt' in str(e):
    #             raise BadRequestException("Maximum number of prompt versions reached. Please create a new agent to continue making updates.")

    # valid_request.configuration.system_prompt['promptVersion'] = prompt_version
    # del valid_request.configuration.system_prompt['updatedPrompt']

    return valid_request


# @tracer.capture_method
# def build_prompt_variant(agent_id, prompt):
#     return {
#         'name': agent_id,
#         'templateConfiguration': {
#             'text': {
#                 'text': prompt
#             }
#         },
#         'templateType': 'TEXT'
#     }


@tracer.capture_method
def create_response(status_code: int, message: str, payload = None):
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