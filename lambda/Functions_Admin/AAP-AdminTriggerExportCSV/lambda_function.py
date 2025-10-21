import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import uuid
import dateutil
import os
import requests
from requests_aws4auth import AWS4Auth
import csv
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import is_authenticated, get_user, get_user_group, has_permission, Permission
from custom_exceptions import AuthorizationException, BadRequestException, AuthenticationException

DOWNLOAD_JOB_TABLE = os.environ.get('DOWNLOAD_JOB_TABLE')
EXPORT_UPLOADED_DOCUMENTS_LAMBDA = os.environ.get('EXPORT_UPLOADED_DOCUMENTS_LAMBDA') 
EXPORT_EXTRACTED_DOCUMENTS_LAMBDA = os.environ.get('EXPORT_EXTRACTED_DOCUMENTS_LAMBDA') 
EXPORT_EXTRACTED_LINE_ITEMS_LAMBDA = os.environ.get('EXPORT_EXTRACTED_LINE_ITEMS_LAMBDA')
EXPORT_EXTRACTED_PO_LAMBDA = os.environ.get('EXPORT_EXTRACTED_PO_LAMBDA') 
EXPORT_EXTRACTED_PO_LINE_ITEMS_LAMBDA = os.environ.get('EXPORT_EXTRACTED_PO_LINE_ITEMS_LAMBDA')
EXPORT_THREE_WAY_MATCHING_LAMBDA = os.environ.get('EXPORT_THREE_WAY_MATCHING_LAMBDA')
EXPORT_RECONCILIATION_RESULTS_LAMBDA = os.environ.get('EXPORT_RECONCILIATION_RESULTS_LAMBDA')
EXPORT_FIXED_ASSET_RESULTS_LAMBDA = os.environ.get('EXPORT_FIXED_ASSET_RESULTS_LAMBDA')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
LAMBDA_CLIENT = boto3.client('lambda')

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
        current_user_name = current_user.get('name')
        merchant_id = current_user.get('merchantId')
        current_user_group_name = get_user_group(current_user.get('userGroupId')).get('userGroupName')
        has_permission(current_user_group_name, Permission.GET_ALL_DOCUMENTS.value)

        arguments = json.loads(event.get('body','{}'))
        module = arguments.get('module')

        jobId = create_job_DDB(merchant_id, module, current_user_name)
        
        status = trigger_export_job(arguments, merchant_id, module, jobId)
        if status is None:
            raise BadRequestException('Failed to trigger download job')
        
        return create_response(200, 'Download job triggered successfully', {
            'downloadJobId': jobId
        })
    except (AuthorizationException, BadRequestException, AuthenticationException) as ex:
        return create_response(400, ex.message)
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")  
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return create_response(500,  "The server encountered an unexpected condition that prevented it from fulfilling your request.")
    
@tracer.capture_method
def create_job_DDB(merchantId, module, username):
    payload = dict()
    payload['downloadJobId'] = str(uuid.uuid4())
    payload['status'] = 'PENDING'
    payload['merchantId'] = merchantId
    payload['createdAt'] = (datetime.now() + timedelta(hours=8)).isoformat(' ')
    payload['module'] = module
    payload['requestedBy'] = username
    DOWNLOAD_JOB_DDB_TABLE.put_item(Item=payload)
    return payload['downloadJobId']

@tracer.capture_method
def getMerchant(merchantId):
    merchantResp = MERCHANT_DDB_TABLE.get_item(
        Key={
            'merchantId': merchantId
        }
    ).get('Item')
    return merchantResp
        
@tracer.capture_method
def trigger_export_job(arguments, merchantId, module, jobId):
    payload = {}
    if module == "DocumentUpload":
        EXPORT_LAMBDA = EXPORT_UPLOADED_DOCUMENTS_LAMBDA
    elif module == "ExtractedDocuments":
        EXPORT_LAMBDA = EXPORT_EXTRACTED_DOCUMENTS_LAMBDA
    elif module == "ExtractedLineItems":
        EXPORT_LAMBDA = EXPORT_EXTRACTED_LINE_ITEMS_LAMBDA
    elif module == "ExtractedPo":
        EXPORT_LAMBDA = EXPORT_EXTRACTED_PO_LAMBDA
    elif module == "ExtractedPoLineItems":
        EXPORT_LAMBDA = EXPORT_EXTRACTED_PO_LINE_ITEMS_LAMBDA
    elif module == "ThreeWayMatchingResults":
        EXPORT_LAMBDA = EXPORT_THREE_WAY_MATCHING_LAMBDA
    elif module == "ReconciliationResults":
        EXPORT_LAMBDA = EXPORT_RECONCILIATION_RESULTS_LAMBDA
    elif module == "GeneratedPo":
        EXPORT_LAMBDA = EXPORT_EXTRACTED_PO_LAMBDA
    elif module == "FixedAsset":
        EXPORT_LAMBDA = EXPORT_FIXED_ASSET_RESULTS_LAMBDA
    else:
        return None

    payload['jobId'] = jobId
    payload['arguments'] = arguments
    payload['merchantId'] = merchantId
    
    LAMBDA_CLIENT.invoke(
        FunctionName=EXPORT_LAMBDA,
        InvocationType='Event',
        Payload=json.dumps(payload)
    )
    logger.info(f"Export job triggered successfully for module: {module}, jobId: {jobId}")
    

    return True

@tracer.capture_method
def create_response(status_code: int, message: str, payload = None):
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
