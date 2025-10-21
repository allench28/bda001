"""
Lambda Function: LiteDemoS3EventProcessor
Trigger: S3 Event (OBJECT_CREATED)
Purpose: Process files uploaded to S3 input/ folder

This function:
1. Updates document status to 'processing' in DynamoDB
2. Processes the document (placeholder for future Textract/Bedrock integration)
3. Updates document status to 'completed' in DynamoDB
"""

import os
import boto3
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.data_classes import S3Event

logger = Logger()
tracer = Tracer()

# DynamoDB client
dynamodb = boto3.resource('dynamodb')
DOCUMENTS_TABLE_NAME = os.environ.get('DOCUMENTS_TABLE_NAME')

@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """
    Automatically triggered when PDF is uploaded to S3 input/ folder
    
    Args:
        event: S3 Event notification
        context: Lambda context
        
    Returns:
        Success response
    """
    try:
        # Parse S3 Event using AWS Lambda Powertools
        s3_event = S3Event(event)
        
        processed_files = []
        
        for record in s3_event.records:
            bucket = record.s3.bucket.name
            key = record.s3.get_object.key
            size = record.s3.get_object.size
            event_name = record.event_name
            
            logger.info(f"S3 Event Received", extra={
                'event_name': event_name,
                'bucket': bucket,
                'key': key,
                'size': size
            })
            
            # Extract document info from path
            # Expected format: input/{documentId}/{filename}.pdf
            path_parts = key.split('/')
            
            if len(path_parts) >= 3:
                document_id = path_parts[1]
                file_name = path_parts[-1]
                
                logger.info(f"Processing document", extra={
                    'document_id': document_id,
                    'file_name': file_name,
                    'bucket': bucket,
                    'key': key
                })
                
                # Update status to 'processing'
                update_document_status(document_id, 'processing')
                
                try:
                    # TODO: Add Textract or Bedrock extraction here
                    # For now, just simulate processing with a simple placeholder
                    extraction_result = process_document(bucket, key, document_id)
                    
                    # Update status to 'completed' with results
                    update_document_with_results(document_id, extraction_result)
                    
                    processed_files.append({
                        'documentId': document_id,
                        'fileName': file_name,
                        'filePath': key,
                        'fileSize': size,
                        'status': 'completed'
                    })
                    
                except Exception as process_error:
                    logger.error(f"Error processing document: {str(process_error)}", extra={
                        'document_id': document_id,
                        'error': str(process_error)
                    })
                    
                    # Update status to 'failed'
                    update_document_status(document_id, 'failed', str(process_error))
                    
                    processed_files.append({
                        'documentId': document_id,
                        'fileName': file_name,
                        'filePath': key,
                        'fileSize': size,
                        'status': 'failed',
                        'error': str(process_error)
                    })
            else:
                logger.warning(f"Invalid path structure: {key}")
        
        response = {
            'statusCode': 200,
            'message': 'S3 event processed successfully',
            'processedFiles': processed_files,
            'totalFiles': len(processed_files)
        }
        
        logger.info("Processing completed", extra={
            'total_files': len(processed_files),
            'files': processed_files
        })
        
        return response
        
    except Exception as e:
        logger.exception(f"Error processing S3 event: {str(e)}")
        # Raise exception so Lambda shows as Failed (not Success)
        raise

@tracer.capture_method
def update_document_status(document_id, status, error_message=None):
    """
    Update document status in DynamoDB
    """
    try:
        table = dynamodb.Table(DOCUMENTS_TABLE_NAME)
        
        current_iso = datetime.now().isoformat()
        
        # Get existing item first
        response = table.query(
            KeyConditionExpression='documentId = :doc_id',
            ExpressionAttributeValues={':doc_id': document_id},
            Limit=1
        )
        
        if response['Items']:
            item = response['Items'][0]
            
            # Update fields
            item['status'] = status
            item['updatedAt'] = current_iso
            
            # Add processedAt timestamp when status is completed or failed
            if status in ['completed', 'failed']:
                item['processedAt'] = current_iso
            
            # Add error message if status is failed
            if status == 'failed' and error_message:
                item['errorMessage'] = error_message
            
            # Put item back to table
            table.put_item(Item=item)
            
            logger.info(f"Updated document status", extra={
                'document_id': document_id,
                'status': status
            })
        else:
            logger.warning(f"Document not found in DynamoDB: {document_id}")
            
    except Exception as e:
        logger.error(f"Failed to update document status: {str(e)}", extra={
            'document_id': document_id,
            'status': status,
            'error': str(e)
        })
        # Don't raise - continue processing other files

@tracer.capture_method
def process_document(bucket, key, document_id):
    """
    Process document - placeholder for future Textract/Bedrock integration
    
    TODO: Implement actual extraction using:
    - AWS Textract for structured data extraction
    - AWS Bedrock for AI-powered parsing
    """
    logger.info(f"Processing document (placeholder)", extra={
        'document_id': document_id,
        'bucket': bucket,
        'key': key
    })
    
    # Placeholder extraction result
    # In the future, this will be replaced with actual Textract/Bedrock calls
    extraction_result = {
        'headerInformation': {
            'companyName': 'Placeholder Company',
            'invoiceNo': 'PENDING',
            'invoiceDate': 'PENDING',
            'dofRef': 'PENDING',
            'poRef': 'PENDING',
            'address': 'PENDING'
        },
        'lineItems': [
            {
                'itemNo': '1',
                'description': 'Processing pending - Textract integration coming soon',
                'quantity': '0',
                'unitPrice': '0.00',
                'total': '0.00'
            }
        ]
    }
    
    return extraction_result

@tracer.capture_method
def update_document_with_results(document_id, extraction_result):
    """
    Update document with extraction results and set status to completed
    """
    try:
        table = dynamodb.Table(DOCUMENTS_TABLE_NAME)
        
        current_iso = datetime.now().isoformat()
        
        # Get existing item first
        response = table.query(
            KeyConditionExpression='documentId = :doc_id',
            ExpressionAttributeValues={':doc_id': document_id},
            Limit=1
        )
        
        if response['Items']:
            item = response['Items'][0]
            
            # Update fields
            item['status'] = 'completed'
            item['headerInformation'] = extraction_result.get('headerInformation', {})
            item['lineItems'] = extraction_result.get('lineItems', [])
            item['processedAt'] = current_iso
            item['updatedAt'] = current_iso
            
            # Put item back to table
            table.put_item(Item=item)
            
            logger.info(f"Updated document with extraction results", extra={
                'document_id': document_id,
                'status': 'completed'
            })
        else:
            logger.warning(f"Document not found in DynamoDB: {document_id}")
            
    except Exception as e:
        logger.error(f"Failed to update document with results: {str(e)}", extra={
            'document_id': document_id,
            'error': str(e)
        })
        raise