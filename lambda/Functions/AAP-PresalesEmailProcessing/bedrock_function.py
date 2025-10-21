import base64
import json
import os
import boto3
import botocore
from aws_lambda_powertools import Logger, Tracer
from botocore.config import Config

logger = Logger()
tracer = Tracer()

config = Config(
    connect_timeout=5,
    read_timeout=120,  # Increase this value
    retries={'max_attempts': 3})
bedrock_role = "arn:aws:iam::339712974969:role/BedrockCrossAccountRole"
credentials = boto3.client('sts').assume_role(
    RoleArn=bedrock_role,
    RoleSessionName='assume-role'
)
bedrock_session = boto3.session.Session(
    aws_access_key_id=credentials['Credentials']['AccessKeyId'],
    aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
    aws_session_token=credentials['Credentials']['SessionToken']
)
BEDROCK_CLIENT = bedrock_session.client(
    service_name='bedrock-runtime',
    region_name='us-east-1', config=config
)
AWS_REGION = os.environ.get('REGION')
ENDPOINT_URL = f'https://bedrock.{AWS_REGION}.amazonaws.com'
MODEL_ID = os.environ.get('MODEL_ID')


@tracer.capture_method
def promptBedrock(prompt_data):
    # Create message content with both text and file data if provided
    content = create_message_content(prompt_data)

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ],
        "temperature": 1,
        "top_p": 0.999,
        "top_k": 250
    })

    accept = "application/json"
    contentType = "application/json"
    result = ""

    try:
        response = BEDROCK_CLIENT.invoke_model(
            body=body,
            modelId=MODEL_ID,
            accept=accept,
            contentType=contentType
        )
        response_body = json.loads(response.get("body").read())
        result = response_body.get("content")[0].get("text")
        input_tokens = response_body.get("usage").get("input_tokens")
        output_tokens = response_body.get("usage").get("output_tokens")

    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'AccessDeniedException':
            raise Exception(
                f"\x1b[41m{error.response['Error']['Message']}\
            \nTo troubleshoot this issue please refer to the following resources.\
            \nhttps://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_access-denied.html\
            \nhttps://docs.aws.amazon.com/bedrock/latest/userguide/security-iam.html\x1b[0m\n"
            )
        else:
            logger.error(f"bedrock error: {error}")
            raise error

    return result, input_tokens, output_tokens


@tracer.capture_method
def create_message_content(prompt_data):
    content = []

    # Add text content
    content.append({
        "type": "text",
        "text": prompt_data
    })

    return content
