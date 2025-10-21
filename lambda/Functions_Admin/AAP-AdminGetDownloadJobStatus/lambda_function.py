import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
from aws_lambda_powertools import Logger, Tracer
# from merchantHelper import getMerchant
from authorizationHelper import is_authenticated, has_permission, Permission, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException


DOWNLOAD_JOB_TABLE = os.environ.get('DOWNLOAD_JOB_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
lambdaClient = boto3.client('lambda')

DOWNLOAD_JOB_DDB_TABLE = DDB_RESOURCE.Table(DOWNLOAD_JOB_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    language = ""
    try:
        sub, _, _ = is_authenticated(event)
        current_user = get_user(sub)
        merchantId = current_user.get('merchantId')
        userGroupName = get_user_group(current_user.get('userGroupId')).get('userGroupName')
        current_user_group_name = get_user_group(current_user.get('userGroupId')).get('userGroupName')
        has_permission(current_user_group_name, Permission.GET_ALL_DOCUMENTS.value)

        parameters = event.get('queryStringParameters', {})

        downloadJobId = parameters.get('downloadJobId')

        downloadJob = DOWNLOAD_JOB_DDB_TABLE.get_item(
            Key = {'downloadJobId':downloadJobId}
        ).get('Item')


        if downloadJob is None:
            return create_response(404, "Download job not found", {
                'status': False,
                'message': 'Download job not found',
                'url': ''
            })
        
        if downloadJob.get('status') == 'COMPLETED':
            return create_response(200, "Success", {
                'status': True,
                'message': 'Success',
                'url': downloadJob.get('objectPresignedUrl')
            })
        elif downloadJob.get('status') == 'FAILED':
            return create_response(500, "Failed", {
                'status': False,
                'message': 'Failed',
                'url': ''
            })
        else:
            return create_response(200, "Pending", {
                'status': True,
                'message': 'Pending',
                'url': ''
            })


    except (AuthenticationException, AuthorizationException, BadRequestException, ResourceNotFoundException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(400, e.message)
   
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return create_response(500, "The server encountered an unexpected condition that prevented it from fulfilling your request.")

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
            'X-Frame-Options':'SAMEORIGIN'
        },
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }