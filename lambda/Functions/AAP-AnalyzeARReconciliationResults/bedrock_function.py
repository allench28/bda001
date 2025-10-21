import json
import os
import boto3
import botocore
import time
import random
import hashlib
from functools import lru_cache
from aws_lambda_powertools import Logger, Tracer
from botocore.config import Config

logger = Logger()
tracer = Tracer()

# Configure with better retry handling
config = Config(
    connect_timeout=5,
    read_timeout=120,
    retries={
        'max_attempts': 5,  # Increased from 3
        'mode': 'adaptive'  # AWS SDK's built-in adaptive retry mode
    }
)

# Initialize global state for rate limiting
_last_request_time = 0
_token_bucket = 10  # Initial tokens
_token_refill_rate = 5  # Tokens per second
_request_cache = {}  # Simple response cache

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
    config=config
)
AWS_REGION = os.environ.get('REGION')
ENDPOINT_URL = f'https://bedrock.{AWS_REGION}.amazonaws.com'
MODEL_ID = os.environ.get('MODEL_ID')

@lru_cache(maxsize=100)
def _get_cached_response(prompt_hash):
    """Get cached response if available"""
    return _request_cache.get(prompt_hash)

def _apply_rate_limiting():
    """Apply token bucket rate limiting"""
    global _last_request_time, _token_bucket
    
    now = time.time()
    time_passed = now - _last_request_time
    _last_request_time = now
    
    # Refill the token bucket based on time passed
    _token_bucket = min(10, _token_bucket + time_passed * _token_refill_rate)
    
    # If we don't have enough tokens, sleep
    if _token_bucket < 1:
        sleep_time = (1 - _token_bucket) / _token_refill_rate
        logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
        time.sleep(sleep_time)
        _token_bucket = 0
    else:
        _token_bucket -= 1

@tracer.capture_method
def promptBedrock(prompt_data):
    """Call Bedrock with improved error handling and rate limiting"""
    # Try to use cached response
    prompt_hash = hashlib.md5(prompt_data.encode()).hexdigest()
    cached = _get_cached_response(prompt_hash)
    if cached:
        logger.info("Using cached Bedrock response")
        return cached
    
    # Apply rate limiting
    _apply_rate_limiting()
    
    # Create message content
    content = create_message_content(prompt_data)
    
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 6000,
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

    # Custom retry logic with exponential backoff
    max_retries = 5
    for attempt in range(max_retries):
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
            
            # Cache the successful response
            _request_cache[prompt_hash] = (result, input_tokens, output_tokens)
            
            return result, input_tokens, output_tokens
            
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'ThrottlingException':
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    backoff = (2 ** attempt) + random.random()
                    logger.warning(f"Bedrock throttled. Retrying in {backoff:.2f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(backoff)
                    continue
                else:
                    logger.error(f"bedrock error: {error}")
                    return "Bedrock Failure", 0, 0
            
            elif error.response['Error']['Code'] == 'AccessDeniedException':
                raise Exception(
                    f"\x1b[41m{error.response['Error']['Message']}\
                \nTo troubleshoot this issue please refer to the following resources.\
                \nhttps://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_access-denied.html\
                \nhttps://docs.aws.amazon.com/bedrock/latest/userguide/security-iam.html\x1b[0m\n"
                )
            else:
                logger.error(f"bedrock error: {error}")
                return "Bedrock Failure", 0, 0
        
        except Exception as e:
            logger.error(f"Unexpected error in Bedrock call: {str(e)}")
            return "Bedrock Failure", 0, 0

@tracer.capture_method
def create_message_content(prompt_data):
    content = []
    content.append({
        "type": "text",
        "text": prompt_data
    })
    return content