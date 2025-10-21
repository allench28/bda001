import os
import uuid
import boto3
import json
from datetime import datetime, timedelta
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException
from authorizationHelper import is_authenticated, get_user

# Environment variables
JOB_TRACKING_TABLE = os.environ.get('JOB_TRACKING_TABLE')
TRIGGER_RECONCILIATION_QUEUE_URL = os.environ.get('TRIGGER_RECONCILIATION_QUEUE_URL')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')

# AWS clients
DDB_RESOURCE = boto3.resource('dynamodb')
SQS_CLIENT = boto3.client('sqs')

JOB_TRACKING_DDB_TABLE = DDB_RESOURCE.Table(JOB_TRACKING_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)

logger = Logger()
tracer = Tracer()

# Merchant id
MERCHANT_ID = "6b8a78e2-95fe-403b-8008-e5f7c1a631fc"

RECONCILIATION_CONFIG = {
    'salesAmount': {
        'foodMarketplace': {
            'paymentMethods': ['GRABPAY', 'GRABFOOD'],
            'description': 'Food Marketplace Reconciliation'
        },
        'creditCard': {
            'paymentMethods': ['CREDIT_CARD', 'QR', 'TNG', 'CIMB_BONUS_POINT'],
            'description': 'Credit Card Reconciliation'
        }
    },
    'settlementAmount': {
        # Future implementation
        # 'foodMarketplace': {
        #     'paymentMethods': ['GRABPAY', 'GRABFOOD'],
        #     'description': 'Food Marketplace Settlement Reconciliation'
        # },
        # 'creditCard': {
        #     'paymentMethods': ['CREDIT_CARD', 'QR', 'TNG', 'CIMB_BONUS_POINT'],
        #     'description': 'Credit Card Settlement Reconciliation'  
        # }
    }
}

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        try:
            sub, _, _ = is_authenticated(event)
            user = get_user(sub)
            merchant_id = user.get('merchantId', MERCHANT_ID)
            
            # Parse the request body
            body = event.get("body", "{}")
            if isinstance(body, str):
                payload = json.loads(body)
            else:
                payload = body
            
            start_date = payload.get('startDate')
            end_date = payload.get('endDate')
            reconciliation_type = payload.get('reconciliationType', 'salesAmount')
            reconciliation_sub_type = payload.get('reconciliationSubType')  
            payment_methods = payload.get('paymentMethods', [])
            
            validation_error = validate_request_parameters(
                start_date, end_date, reconciliation_type, reconciliation_sub_type, payment_methods
            )

            if validation_error:
                return create_response(400, validation_error)
            
            available_payment_methods = RECONCILIATION_CONFIG[reconciliation_type][reconciliation_sub_type]['paymentMethods']
            
            invalid_payment_methods = [pm for pm in payment_methods if pm not in available_payment_methods]
            if invalid_payment_methods:
                return create_response(400, f"Invalid payment methods: {invalid_payment_methods} for {reconciliation_sub_type}. Available methods: {available_payment_methods}")
                
            job_id = createJobTracking(merchant_id, reconciliation_type, reconciliation_sub_type, payment_methods)
            
            date_chunks = breakIntoDateChunks(start_date, end_date)
            
            total_chunks = 0
            for chunk_start, chunk_end in date_chunks:
                for payment_method in payment_methods:
                    message = {
                        'jobId': job_id,
                        'merchantId': merchant_id,
                        'startDate': chunk_start,
                        'endDate': chunk_end,
                        'reconciliationType': reconciliation_type,
                        'reconciliationSubType': reconciliation_sub_type,
                        'paymentMethod': payment_method
                    }
                    
                    SQS_CLIENT.send_message(
                        QueueUrl=TRIGGER_RECONCILIATION_QUEUE_URL,
                        MessageBody=json.dumps(message)
                    )
                    total_chunks += 1
            
            updateJobStatus(job_id, 'QUEUED', {
                'totalChunks': total_chunks,
                'completedChunks': 0
            })
            
            createTimelineRecord(
                job_id, merchant_id, user.get('email', 'system'),
                'Reconciliation Job Queued',
                f"Reconciliation job queued successfully with {total_chunks} chunks for processing",
                'reconciliation', reconciliation_type, reconciliation_sub_type,
                additional_data={'totalChunks': total_chunks}
            )
            
            return create_response(200, "Reconciliation job started", {
                "jobId": job_id,
                "totalChunks": total_chunks,
                "reconciliationType": reconciliation_type,
                "reconciliationSubType": reconciliation_sub_type,
                "paymentMethods": payment_methods
            })
            
        except Exception as ex:
            logger.error(f"Error in API Gateway handling: {str(ex)}")
            
            if 'job_id' in locals():
                createTimelineRecord(
                    job_id, merchant_id, user.get('email', 'system') if 'user' in locals() else 'system',
                    'Reconciliation Job Failed',
                    f"Reconciliation job failed during setup: {str(ex)}",
                    'reconciliation', 'error', 'error'
                )
            return create_response(400, str(ex))
            
    except (BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException) as ex:
        logger.error(f"Custom error: {str(ex)}")
        return create_response(400, str(ex))
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        
        createTimelineRecord(
            'unknown', MERCHANT_ID, 'system',
            'Reconciliation Critical Error',
            f"Critical error in reconciliation trigger: {str(ex)}",
            'reconciliation', 'error', 'critical'
        )
        return create_response(500, "The server encountered an unexpected condition")

@tracer.capture_method
def validate_request_parameters(start_date, end_date, reconciliation_type, reconciliation_sub_type, payment_methods):
    """Validate all request parameters and return error message if invalid"""
    
    # Required field validations
    required_fields = [
        (start_date, "Start date is required"),
        (end_date, "End date is required"),
        (reconciliation_sub_type, "Reconciliation sub type is required"),
        (payment_methods and len(payment_methods) > 0, "At least one payment method is required")
    ]
    
    for condition, error_message in required_fields:
        if not condition:
            return error_message
    
    # Configuration validations
    if reconciliation_type not in RECONCILIATION_CONFIG:
        return f"Invalid reconciliation type: {reconciliation_type}"
    
    if reconciliation_sub_type not in RECONCILIATION_CONFIG[reconciliation_type]:
        return f"Invalid reconciliation sub type: {reconciliation_sub_type} for type: {reconciliation_type}"
    
    # Payment method validation
    available_payment_methods = RECONCILIATION_CONFIG[reconciliation_type][reconciliation_sub_type]['paymentMethods']
    invalid_payment_methods = [pm for pm in payment_methods if pm not in available_payment_methods]
    
    if invalid_payment_methods:
        return f"Invalid payment methods: {invalid_payment_methods} for {reconciliation_sub_type}. Available methods: {available_payment_methods}"
    
    return None

@tracer.capture_method
def createJobTracking(merchantId, reconciliationType, reconciliationSubType, paymentMethods, totalRecords=0):
    """Create a job tracking record in DynamoDB"""
    if not JOB_TRACKING_TABLE:
        return str(uuid.uuid4())
        
    jobId = str(uuid.uuid4())
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    jobData = {
        'jobTrackingId': jobId,
        'merchantId': merchantId,
        'module': 'reconciliation',
        'reconciliationType': reconciliationType,
        'reconciliationSubType': reconciliationSubType,
        'paymentMethods': paymentMethods,
        'status': 'QUEUED',
        'totalRecords': totalRecords,
        'matchedCount': 0,
        'unmatchedCount': 0,
        'createdAt': timestamp,
        'createdBy': "System",
        'updatedAt': timestamp,
        'updatedBy': "System"
    }
    
    JOB_TRACKING_DDB_TABLE.put_item(Item=jobData)

    return jobId

@tracer.capture_method
def updateJobStatus(jobId, status, additionalData=None):
    """Update job status in the tracking table"""
    if not JOB_TRACKING_TABLE:
        return
        
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    update_expression = "set #status = :status, updatedAt = :updatedAt"
    expression_values = {
        ":status": status,
        ":updatedAt": timestamp
    }
    
    expression_names = {
        "#status": "status"
    }
    
    # Add any additional data to update
    if additionalData:
        for key, value in additionalData.items():
            update_expression += f", #{key} = :{key}"
            expression_values[f":{key}"] = value
            expression_names[f"#{key}"] = key
    
    # Update the job record
    JOB_TRACKING_DDB_TABLE.update_item(
        Key={'jobTrackingId': jobId},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
        ExpressionAttributeNames=expression_names
    )

@tracer.capture_method
def create_response(status_code, message, payload=None):
    """Create a properly formatted response for API Gateway"""
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
        'body': json.dumps({
            "statusCode": status_code,
            "message": message,
            **payload
        })
    }

@tracer.capture_method
def breakIntoDateChunks(start_date, end_date, chunk_days=1):
    """Break a date range into smaller chunks"""
    # Check if the date strings include time components
    try:
        # Try parsing with date-only format first
        start = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        # If that fails, try parsing as ISO format
        try:
            # Handle ISO format with T separator
            if 'T' in start_date:
                start_date = start_date.split('T')[0]
            start = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Could not parse start_date: {start_date}")
            raise e
    
    try:
        # Try parsing with date-only format first
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        # If that fails, try parsing as ISO format
        try:
            # Handle ISO format with T separator
            if 'T' in end_date:
                end_date = end_date.split('T')[0]
            end = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Could not parse end_date: {end_date}")
            raise e
    
    chunks = []
    current = start
    
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days-1), end)
        chunks.append((
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        ))
        current = chunk_end + timedelta(days=1)
    
    return chunks

@tracer.capture_method
def createTimelineRecord(job_id, merchant_id, user, title, description, record_type, reconciliation_type, reconciliation_sub_type, additional_data=None):
    """Create a timeline record for audit trail"""
    if not TIMELINE_DDB_TABLE:
        return
        
    timeline_id = str(uuid.uuid4())
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    timeline_item = {
        'timelineId': timeline_id,
        'timelineForId': job_id,
        'merchantId': merchant_id,
        'createdAt': now,
        'createdBy': user,
        'updatedAt': now,
        'updatedBy': user,
        'type': record_type,
        'title': title,
        'description': description,
        'reconciliationType': reconciliation_type,
        'reconciliationSubType': reconciliation_sub_type,
        'module': 'reconciliation'
    }
    
    # Add any additional data
    if additional_data:
        timeline_item.update(additional_data)
    
    try:
        TIMELINE_DDB_TABLE.put_item(Item=timeline_item)
    except Exception as e:
        logger.error(f"Failed to create timeline record: {str(e)}")