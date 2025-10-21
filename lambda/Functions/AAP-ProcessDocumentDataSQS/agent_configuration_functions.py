import boto3
import json
import os
import io
import pandas as pd
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.conditions import Attr, Key

logger = Logger()
tracer = Tracer()

DEFAULT_INVOICE_EXTRACTION_PROMPT_VERSION = 'DRAFT'
DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN = os.environ.get('DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN')
AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
AAP_AGENT_MAPPINGS_BUKET = os.environ.get('AGENT_MAPPINGS_BUCKET_NAME')

DDB_RESOURCE = boto3.resource('dynamodb')
BEDROCK_CLIENT = boto3.client('bedrock-agent')
S3_CLIENT = boto3.client('s3')

AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)

@tracer.capture_method
def generate_agent_prompt(extracted_document_text, prompt_details):
    mapping_database = ""
    
    prompt_arn = prompt_details.get('promptArn')
    prompt_version = prompt_details.get('promptVersion')
    mapping_one = prompt_details.get('mappingOne')
    mapping_two = prompt_details.get('mappingTwo')

    mapping_database = format_mappings(mapping_one, mapping_two)
    prompt = get_prompt_from_management(prompt_arn, prompt_version)

    logger.info(f'PROMPT DETAILS: {prompt_details}')

    full_prompt = prompt + "\n"+ mapping_database  + "\n<doc>\n" + extracted_document_text+"\n</doc>"
    
    return full_prompt

@tracer.capture_method
def get_agent_config(document_type, merchant_id):
    try:
        # Invoice, 
        if document_type == 'Invoice':
            service_type = 'Account Receivable'
            service_actions = 'Invoice Extraction'

        # [Purchase Order, Delivery Order, Credit Note, Debit Note]
        else:
            service_type = 'Account Receivable'
            service_actions = 'Invoice Extraction'

        response = AGENT_CONFIGURATION_DDB_TABLE.query(
            IndexName='merchantId-index',
            KeyConditionExpression=Key('merchantId').eq(merchant_id),
            FilterExpression=
                Attr('service.type').eq(service_type) &
                Attr('service.actions').eq(service_actions)
        )

        items = response['Items']
        if len(items) == 0:
            raise Exception("Agent configuration missing for document type")
            # return DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN, DEFAULT_INVOICE_EXTRACTION_PROMPT_VERSION, "", "", []
    
        else: 
            sorted_items = sorted(
                items, 
                key=lambda x: x.get('updatedAt', ''),
                reverse=True
            )

            configuration = sorted_items[0].get('configuration')
            system_prompt_config = configuration.get('systemPrompt')
            
            processing_frequency_config = configuration.get('processingFrequencyConfig')
            email_recipients = processing_frequency_config.get('emailRecipients', [])

            prompt_details = {
                'promptArn': system_prompt_config.get('promptArn'),
                'promptVersion': system_prompt_config.get('promptVersion'),
                'mappingOne': configuration.get('mappingOneURL'),
                'mappingTwo': configuration.get('mappingTwoURL')
            }

            return prompt_details, email_recipients

    except Exception as e:
        logger.error(f"Failed to retrieve system prompt from latest agent configuration: {str(e)}")
        return DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN, DEFAULT_INVOICE_EXTRACTION_PROMPT_VERSION
        
    
@tracer.capture_method
def get_prompt_from_management(prompt_arn, prompt_version='DRAFT'):
    try:
        if prompt_version == 'DRAFT':
            response = BEDROCK_CLIENT.get_prompt(
                promptIdentifier=prompt_arn,
            )
        else: 
            response = BEDROCK_CLIENT.get_prompt(
                promptIdentifier=prompt_arn,
                promptVersion=prompt_version
            )

        variants = response.get('variants')

        if len(variants) == 0:
            raise

        else:
            return variants[0].get('templateConfiguration').get('text').get('text')
    
    except Exception as e:
        logger.error(f"Failed to retrieve prompt from bedrock prompt management: {str(e)}")
        raise Exception("Failed to retrieve prompt from bedrock prompt management")
    

@tracer.capture_method
def parse_mappings(object_key) -> str:
    response = S3_CLIENT.get_object(Bucket=AAP_AGENT_MAPPINGS_BUKET, Key=object_key)
    csv_content = response['Body'].read().decode('utf-8')
    
    df = pd.read_csv(io.StringIO(csv_content))
    df_headers = list(df.columns)
    item_list = df.to_dict('records')

    formatted_df = f"Columns: {', '.join(df_headers)}\n"
    
    for item in item_list:
        row_data = []
        for header in df_headers:
            row_data.append(f"{item[header]}")
        formatted_df += "\n" + "|".join(row_data)

    return formatted_df

@tracer.capture_method
def format_mappings(mapping_one, mapping_two):
    parsed_mapping_one = parse_mappings(mapping_one)
    parsed_mapping_two = parse_mappings(mapping_two) 

    database = f"""<database>\n{parsed_mapping_one}\n{parsed_mapping_two}\n</database>"""
    return database