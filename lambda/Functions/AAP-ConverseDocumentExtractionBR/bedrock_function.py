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
    region_name='us-east-1', 
    # region_name='ap-southeast-1',
    config=config
)

AWS_REGION = os.environ.get('REGION')
CLAUDE_MODEL_ID = os.environ.get("CLAUDE_MODEL_ID")
NOVA_MODEL_ID = os.environ.get("NOVA_MODEL_ID")


@tracer.capture_method
def call_bedrock_converse(image_data, prompt, mode, document_type):
    """
    Call Bedrock converse API with S3 URIs or byte data
    """
    # Determine if we're using S3 URIs or byte data
    using_s3_uris = isinstance(image_data, list) and len(image_data) > 0 and 'uri' in image_data[0]
    if mode == "bounding_box":
        model_id = NOVA_MODEL_ID
        systemPrompt = "You are an expert at detecting precise bounding boxes for text elements in documents. You analyze scanned documents and provide accurate bounding box coordinates for specified entities. When processing multiple pages, analyze all pages together and provide bounding boxes for entities found on each page."
        maxTokens = 10000    
    elif mode == "extraction":
        model_id = CLAUDE_MODEL_ID
        systemPrompt = ""
        maxTokens = 10000
    
    try:
        # Build content array with all images and the prompt
        content = []
        system = []
        
        # Add all page images to the content
        if using_s3_uris:
            # Using S3 URIs
            for idx, image_info in enumerate(image_data):
                if idx >= 10:
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
            
            for page_num, (page_bytes, page_size) in enumerate(image_data):
                if page_num >= 10:
                    continue
                content.append({
                    "image": {"format": "png", "source": {"bytes": page_bytes}}
                })
        
        content.append({"text": prompt})
        
        messages = [
            {
                "role": "user",
                "content": content
            }
        ]
        
        inf_params = {
            "maxTokens": maxTokens,
            "topP": 1,
            "temperature": 0,
        }
        
        start_time = time.time()
        
        try:
            if systemPrompt:
                system = [
                    {
                        "text": systemPrompt,
                    }
                ]
                
                model_response = BEDROCK_CLIENT.converse(
                    modelId=model_id, 
                    system=system,
                    messages=messages, 
                    inferenceConfig=inf_params
                )
            else:
                model_response = BEDROCK_CLIENT.converse(
                    modelId=model_id, 
                    messages=messages, 
                    inferenceConfig=inf_params
                )
            
            api_duration = time.time() - start_time
            
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
