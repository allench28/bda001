import base64
import json
import os
import boto3
import botocore
import time
from aws_lambda_powertools import Logger, Tracer
from botocore.config import Config

logger = Logger()
tracer = Tracer()

config = Config(
    connect_timeout=10,
    read_timeout=150,  # Increase this value
    retries={'max_attempts': 5}
)
print('BOTO3_version', boto3.__version__)

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
    # region_name='us-east-1', 
    region_name='ap-southeast-1',
    config=config
)

AWS_REGION = os.environ.get('REGION')
CLAUDE_MODEL_ID = os.environ.get("MODEL_ID", "apac.anthropic.claude-3-7-sonnet-20250219-v1:0")
CLAUDE_4_MODEL_ID = os.environ.get("CLAUDE_4_MODEL_ID", "apac.anthropic.claude-sonnet-4-20250514-v1:0")
NOVA_MODEL_ID = os.environ.get("NOVA_MODEL_ID", "apac.amazon.nova-pro-v1:0")


@tracer.capture_method
def call_bedrock_converse(image_data, prompt, mode, document_type, is_pdf=False):
    """
    Call Bedrock converse API with S3 URIs or byte data
    """
    # Determine if we're using S3 URIs or byte data
    using_s3_uris = isinstance(image_data, list) and len(image_data) > 0 and 'uri' in image_data[0]
    
    if using_s3_uris:
        logger.info(f"Starting Bedrock converse call for {len(image_data)} S3 images for mode: {mode}")
    else:
        logger.info(f"Starting Bedrock converse call for {len(image_data)} byte images for mode: {mode}")
    
    if mode == "bounding_box":
        model_id = NOVA_MODEL_ID
        systemPrompt = "You are an expert at detecting precise bounding boxes for text elements in documents. You analyze scanned documents and provide accurate bounding box coordinates for specified entities. When processing multiple pages, analyze all pages together and provide bounding boxes for entities found on each page."
        maxTokens = 10000    
    elif mode == "extraction":
        model_id = CLAUDE_MODEL_ID

        if document_type == "BILLING":
            model_id = CLAUDE_4_MODEL_ID

        systemPrompt = "You are a meticulous transcriber of text from scanned documents and an expert in malaysia loan applications who understands technical terms and lending terminology in both English and Bahasa Malaysia. You don't make up any information that is not inside the document. When processing multiple pages, analyze all pages together and extract entities from across all pages, indicating which page each entity was found on."
        maxTokens = 10000
    
    try:
        # Build content array with all images and the prompt
        content = []

        
        # Add all page images to the content
        if is_pdf:
            content_item = {
                "document": {
                    "name": "Invoice",
                    "format": "pdf",
                    "source": {
                        "bytes": image_data
                    },
                    "citations": {"enabled": True}
                }
            }
            content.append(content_item)

        elif using_s3_uris:
            # Using S3 URIs
            for idx, image_info in enumerate(image_data):
                if idx >= 10:
                    logger.info(f"Skipping page {idx + 1} due to limit of 19 pages")
                    continue
                
                content_item = {
                    "image": {
                        "format": "png", 
                        "source": {
                            "s3Location": {
                                "uri": image_info['uri']
                            }
                        }
                    }
                }
                
                content.append(content_item)

        else:
            # Using byte data (backward compatibility)
            total_size = sum(len(page_bytes) for page_bytes, _ in image_data)
            logger.info(f"Total image data size: {total_size / (1024*1024):.2f} MB")
            
            for page_num, (page_bytes, page_size) in enumerate(image_data):
                if page_num >= 10:
                    logger.info(f"Skipping page {page_num + 1} due to limit of 19 pages")
                    continue
                content.append({
                    "image": {"format": "png", "source": {"bytes": page_bytes}}
                })
                logger.info(f"Added page {page_num + 1} image to content (size: {len(page_bytes) / 1024:.2f} KB, dimensions: {page_size})")
        
        content.append({"text": prompt})
        
        logger.info(f"Added prompt to content (length: {len(prompt)} chars)")
        
        messages = [
            {
                "role": "user",
                "content": content
            }
        ]
        
        system = [
            {
                "text": systemPrompt,
            }
        ]
        
        inf_params = {
            "maxTokens": maxTokens,
            "topP": 1,
            "temperature": 0,
        }
        
        logger.info(f"Making Bedrock API call for mode: {mode}")
        
        start_time = time.time()

        estimated_size = len(json.dumps(messages, default=str))
        logger.info(f"Message structure size estimate: {estimated_size / (1024*1024):.2f} MB")
        
        try:
            model_response = BEDROCK_CLIENT.converse(
                modelId=model_id, 
                system=system,
                messages=messages, 
                inferenceConfig=inf_params
            )
            
            api_duration = time.time() - start_time
            logger.info(f"Bedrock API call completed successfully in {api_duration:.2f} seconds")
            
            # Add duration to response for tracking
            model_response['api_duration'] = api_duration
            
            return {
                "success": True,
                "response": model_response,
                "total_pages": len(image_data)
            }
            
        except Exception as bedrock_error:
            api_duration = time.time() - start_time
            logger.error(f"Bedrock API call failed after {api_duration:.2f} seconds: {str(bedrock_error)}")
            
            return {
                "success": False,
                "error": str(bedrock_error),
                "error_type": type(bedrock_error).__name__,
                "api_duration": api_duration,
                "total_pages": len(image_data)
            }
        
    except Exception as e:
        logger.error(f"Unexpected error in call_bedrock_converse: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "total_pages": len(image_data) if image_data else 0
        }


@tracer.capture_method
def promptBedrock(prompt_data):
    """Original Bedrock function using invoke_model - kept for compatibility"""
    # Create message content with both text and file data if provided
    content = create_message_content(prompt_data)

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 10000,
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ],
        "temperature": 0,
        "top_p": 0.999,
        "top_k": 20
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
    """Create message content for simple text prompts"""
    content = []

    # Add text content
    content.append({
        "type": "text",
        "text": prompt_data
    })

    return content