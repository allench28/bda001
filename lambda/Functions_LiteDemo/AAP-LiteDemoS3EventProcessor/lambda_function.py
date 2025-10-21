"""
Lambda Function: LiteDemoS3EventProcessor
Trigger: S3 Event (OBJECT_CREATED)
Purpose: Process files uploaded to S3 input/ folder

This is a simple test function that logs S3 events and returns success.
"""

import json
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.data_classes import S3Event

logger = Logger()
tracer = Tracer()

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
                
                processed_files.append({
                    'documentId': document_id,
                    'fileName': file_name,
                    'filePath': key,
                    'fileSize': size,
                    'status': 'received'
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