import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
from decimal import Decimal
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import is_authenticated, has_permission, Permission, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

JOB_TRACKING_TABLE = os.environ.get('JOB_TRACKING_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

JOB_TRACKING_DDB_TABLE = DDB_RESOURCE.Table(JOB_TRACKING_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

logger = Logger()
tracer = Tracer()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        current_user = get_user(sub)
        merchantId = current_user.get('merchantId')
        current_user_group_name = get_user_group(current_user.get('userGroupId')).get('userGroupName')
        has_permission(current_user_group_name, Permission.GET_ALL_DOCUMENTS.value)

        parameters = event.get('queryStringParameters', {}) or {}
        jobTrackingId = parameters.get('jobTrackingId')
        
        # If jobTrackingId provided, get specific job
        if jobTrackingId:
            jobTracking = JOB_TRACKING_DDB_TABLE.get_item(
                Key={'jobTrackingId': jobTrackingId}
            ).get('Item')

            if jobTracking is None:
                return create_response(404, "Job tracking record not found", {
                    'status': False,
                    'message': 'Job tracking record not found'
                })
            
            # Check if user has access to this job (must match their merchantId)
            if jobTracking.get('merchantId') != merchantId:
                return create_response(403, "You don't have permission to access this job", {
                    'status': False,
                    'message': 'Access denied'
                })
                
            # Return job status based on status field
            if jobTracking.get('status') == 'COMPLETED':
                return create_response(200, "Success", {
                    'status': True,
                    'message': 'Job completed',
                    'jobTracking': jobTracking,
                    'progress': calculate_progress(jobTracking)
                })
            elif jobTracking.get('status') == 'FAILED':
                return create_response(500, "Failed", {
                    'status': False,
                    'message': 'Job failed',
                    'jobTracking': jobTracking,
                    'progress': calculate_progress(jobTracking)
                })
            else:  # IN_PROGRESS
                return create_response(200, "In Progress", {
                    'status': True,
                    'message': 'Job in progress',
                    'jobTracking': jobTracking,
                    'progress': calculate_progress(jobTracking)
                })
        else:
            # No jobTrackingId provided, return recent jobs for this merchant
            response = JOB_TRACKING_DDB_TABLE.query(
                IndexName='gsi-merchantId-createdAt',  # Make sure this GSI exists
                KeyConditionExpression=Key('merchantId').eq(merchantId),
                ScanIndexForward=False,  # Sort in descending order by sort key (createdAt)
                Limit=10  # Return only the 10 most recent jobs
            )
            
            jobs = response.get('Items', [])
            
            # Add progress calculation for each job
            for job in jobs:
                job['progress'] = calculate_progress(job)
                
            return create_response(200, "Success", {
                'status': True,
                'message': 'Success',
                'jobs': jobs
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
def calculate_progress(job):
    """Calculate progress percentage for a job"""
    total = job.get('totalInvoices', 0)
    if total == 0:
        return 100 
    
    completed = job.get('totalCompletedRecords', 0)
    failed = job.get('totalFailedRecords', 0)
    processed = completed + failed
    
    # Calculate percentage with maximum of 100
    return min(int((processed / total) * 100), 100)

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
        'body': json.dumps({"statusCode": status_code, "message": message, **payload}, cls=DecimalEncoder)
    }