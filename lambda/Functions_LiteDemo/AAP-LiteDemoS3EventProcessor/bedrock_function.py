import boto3
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



BEDROCK_CLIENT = boto3.client(
    service_name='bedrock-runtime',
    region_name='us-west-2',
    config=config
)

QWEN_ID = "qwen.qwen3-235b-a22b-2507-v1:0"


@tracer.capture_method
def extraction_markdown_bedrock(prompt_data):
    """Process text/markdown content with Bedrock using converse API"""
    logger.info(f"Starting Bedrock converse call for markdown extraction")
    
    start_time = time.time()
    
    try:
        model_response = BEDROCK_CLIENT.converse(
            modelId=QWEN_ID,
            messages=[{
                "role": "user",
                "content": [{"text": prompt_data}]
            }],
            inferenceConfig={
                "maxTokens": 11000,
                "temperature": 0,
                "topP": 0.0001
            }
        )
        
        return model_response
        
    except Exception as bedrock_error:
        logger.error(f"Bedrock API call failed: {str(bedrock_error)}")
        
        return {
            "output": {
                "message": {
                    "content": [{
                        "text": f"Error: {str(bedrock_error)}"
                    }]
                }
            }
        }
