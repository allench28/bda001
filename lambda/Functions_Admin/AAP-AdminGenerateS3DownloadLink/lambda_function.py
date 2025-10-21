import os
import boto3
import json
import re
import urllib.parse
from aws_lambda_powertools import Logger, Tracer
from botocore.client import Config
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

SMART_EYE_BUCKET = os.environ.get('SMART_EYE_BUCKET')

S3_CLIENT = boto3.client(
    's3',
    region_name='ap-southeast-5',
    endpoint_url='https://s3.ap-southeast-5.amazonaws.com'
)

logger = Logger()
tracer = Tracer()
    
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.GENERATE_PRESIGNED_URLS.value)
        
        parameters = event.get('queryStringParameters', {})

        if not (parameters and parameters.get('s3Path')):
            raise BadRequestException('s3Path not found in parameters')
        
        s3_path = parameters.get('s3Path')
        s3_path = s3_path.replace("%2F", "/")

        document_merchant = s3_path.split("/")[1]
        if document_merchant != merchant_id:
            raise AuthorizationException("You are not authorized to access this resource")

        presigned_url = generate_presigned_url(SMART_EYE_BUCKET, s3_path, 3600)

        payload = {
            'url': presigned_url
        }
        
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
def generate_presigned_url(bucket_name, s3_path, expiration=3600):
    try:
        decoded_key = urllib.parse.unquote(s3_path)
        filename_part = decoded_key.split('/')[-1]

        response = S3_CLIENT.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name,
                'Key': decoded_key,
                'ResponseContentDisposition': f'attachment; filename="{filename_part}"'
            },
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        raise Exception(f"An error occurred while generating the presigned URL: {str(e)}")

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
            'Cache-control': 'no-store', #added for security fix
            'Pragma': 'no-cache',
            'X-Frame-Options':'SAMEORIGIN'
        },
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }
