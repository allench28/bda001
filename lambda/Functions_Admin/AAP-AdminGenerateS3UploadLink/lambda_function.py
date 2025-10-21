import os
import boto3
import json
import uuid
from aws_lambda_powertools import Logger, Tracer
from botocore.client import Config
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

SMART_EYE_BUCKET = os.environ.get('SMART_EYE_BUCKET')
AGENT_CONFIGURATION_BUCKET = os.environ.get('AGENT_CONFIGURATION_BUCKET')
USER_TABLE = os.environ.get('USER_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)

logger = Logger()
tracer = Tracer()

# Configurations for different upload types
UPLOAD_CONFIGS = {
    "document": {
        "bucket": SMART_EYE_BUCKET,
        "path_prefix": 'input',
        "allowed_extensions": ["pdf"],
        "max_file_size": 15728640,  # 15MB
        "expiration": 3600,  # 1 hour
    },
    "mapping": {
        "bucket": AGENT_CONFIGURATION_BUCKET,
        "path_prefix": "temp",
        "allowed_extensions": ["csv"],
        "max_file_size": 15728640,  # 15MB
        "expiration": 300,  # 5 minutes
    }
}

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # Authentication and authorization
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.GENERATE_PRESIGNED_URLS.value)
        

        request_body = json.loads(event.get('body', '{}'))
        
        if not request_body.get('uploadType'):
            raise BadRequestException('uploadType parameter is required')
        
        if not request_body.get('fileName'):
            raise BadRequestException('fileName parameter is required')
        
        upload_type = request_body.get('uploadType')
        file_name = request_body.get('fileName')
        
        config = UPLOAD_CONFIGS[upload_type].copy()

        if upload_type == "document":
            config["path_prefix"] = f"{config['path_prefix']}/{merchant_id}"
        
        upload_id = str(uuid.uuid4())
        key = f"{config['path_prefix']}/{upload_id}/{file_name}"
        
        fileExt = verifyFileExtension(file_name, config['allowed_extensions'])
        
        presigned_post_url = generate_presigned_post(
            config['bucket'], 
            key, 
            config['max_file_size'], 
            config['expiration']
        )
        
        # Create response based on upload type
        payload = {
            "processId": upload_id
        }
        
        if upload_type == "document":
            payload["uploadUrl"] = presigned_post_url
            return create_response(200, "Success", payload, upload_id)
        else:
            payload["presignedUrl"] = presigned_post_url
            return create_response(200, "Success", payload)

    except (AuthenticationException, AuthorizationException, BadRequestException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(400, e.message)

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
def verifyFileExtension(filename, allowedFileExt):
    if '.' not in filename:
        raise BadRequestException("Bad Request: Invalid file name")
    else:
        filenameList = filename.split(".")
        fileExt = filenameList[len(filenameList)-1]
        if fileExt.lower() not in allowedFileExt:
            raise BadRequestException(f"Bad Request: File extension not allowed.")
        return fileExt

@tracer.capture_method
def generate_presigned_post(bucket_name, object_name, max_file_size, expiration=3600):
    S3_CLIENT = boto3.client('s3', config=Config(signature_version='s3v4'))
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
        raise e

@tracer.capture_method
def create_response(status_code, message, payload=None, image_result_id=None):
    if not payload:
        payload = {}
    
    response_body = {"statusCode": status_code, "message": message, **payload}
    
    if image_result_id:
        response_body["imageResultId"] = image_result_id

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
        'body': json.dumps(response_body)
    }