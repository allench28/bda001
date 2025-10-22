import os
import json
import uuid
import boto3
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from botocore.client import Config

LITE_DEMO_BUCKET = os.environ.get('LITE_DEMO_BUCKET')
DOCUMENTS_TABLE_NAME = os.environ.get('DOCUMENTS_TABLE_NAME')

logger = Logger()
tracer = Tracer()

# DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Configurations for different upload types
UPLOAD_CONFIGS = {
    "document": {
        "bucket": LITE_DEMO_BUCKET,
        "path_prefix": 'input',
        "allowed_extensions": ["pdf"],
        "max_file_size": 15728640,  # 15MB
        "expiration": 3600,  # 1 hour
    },
    "csv": {
        "bucket": LITE_DEMO_BUCKET,
        "path_prefix": "data",
        "allowed_extensions": ["csv"],
        "max_file_size": 15728640,  # 15MB
        "expiration": 3600,  # 1 hour
    }
}

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        request_body = json.loads(event.get('body', '{}'))
        
        if not request_body.get('uploadType'):
            return create_response(400, 'uploadType parameter is required')
        
        if not request_body.get('fileName'):
            return create_response(400, 'fileName parameter is required')
        
        upload_type = request_body.get('uploadType')
        file_name = request_body.get('fileName')
        
        # Validate upload type
        if upload_type not in UPLOAD_CONFIGS:
            return create_response(400, f'Invalid uploadType. Allowed types: {", ".join(UPLOAD_CONFIGS.keys())}')
        
        config = UPLOAD_CONFIGS[upload_type].copy()
        
        # Generate unique upload ID
        upload_id = str(uuid.uuid4())
        cleaned_file_name = clean_file_name(file_name)
        key = f"{config['path_prefix']}/{upload_id}/{cleaned_file_name}"
        
        # Verify file extension
        file_ext = verify_file_extension(file_name, config['allowed_extensions'])
        
        # Generate presigned POST URL
        presigned_post_url = generate_presigned_post(
            config['bucket'], 
            key, 
            config['max_file_size'], 
            config['expiration']
        )
        
        # Create initial document record in DynamoDB
        create_document_record(upload_id, cleaned_file_name, key, upload_type)
        
        # Create response
        payload = {
            "processId": upload_id,
            "uploadUrl": presigned_post_url,
            "s3Path": key
        }
        
        return create_response(200, "Success", payload)

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
def clean_file_name(file_name):
    """Clean file name by removing symbols and replacing spaces with underscores"""
    import re
    # Keep only alphanumeric, dots, hyphens, underscores
    cleaned = re.sub(r'[^a-zA-Z0-9._-]', '_', file_name)
    # Replace multiple underscores with single underscore
    cleaned = re.sub(r'_+', '_', cleaned)
    return cleaned

@tracer.capture_method
def verify_file_extension(filename, allowed_file_ext):
    if '.' not in filename:
        raise Exception("Invalid file name")
    
    filename_parts = filename.split(".")
    file_ext = filename_parts[-1]
    
    if file_ext.lower() not in allowed_file_ext:
        raise Exception(f"File extension not allowed. Allowed extensions: {', '.join(allowed_file_ext)}")
    
    return file_ext

@tracer.capture_method
def generate_presigned_post(bucket_name, object_name, max_file_size, expiration=3600):
    S3_CLIENT = boto3.client('s3', config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'}))
    try:
        response = S3_CLIENT.generate_presigned_post(
            Bucket=bucket_name,
            Key=object_name,
            Conditions=[
                ["content-length-range", 0, max_file_size],
            ],
            ExpiresIn=expiration
        )
        return json.dumps(response)
    except Exception as e:
        raise Exception(f"Failed to generate presigned POST URL: {str(e)}")

@tracer.capture_method
def create_document_record(document_id, file_name, s3_key, upload_type):
    """
    Create initial document record in DynamoDB with status 'uploaded'
    """
    try:
        table = dynamodb.Table(DOCUMENTS_TABLE_NAME)
        
        current_timestamp = int(datetime.now().timestamp())
        current_iso = datetime.now().isoformat()
        
        item = {
            'documentId': document_id,
            'createdAt': current_timestamp,
            'fileName': file_name,
            's3Key': s3_key,
            'uploadType': upload_type,
            'status': 'uploaded',
            'uploadedAt': current_iso,
            'updatedAt': current_iso
        }
        
        table.put_item(Item=item)
        
        logger.info(f"Created document record", extra={
            'document_id': document_id,
            'file_name': file_name,
            'status': 'uploaded'
        })
        
        return item
        
    except Exception as e:
        logger.error(f"Failed to create document record: {str(e)}", extra={
            'document_id': document_id,
            'error': str(e)
        })
        # Don't fail the upload if DynamoDB fails
        # Just log and continue
        return None

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
            'X-Frame-Options': 'SAMEORIGIN'
        },
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }
