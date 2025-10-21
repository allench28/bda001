import json
import boto3
import pyarrow.parquet as pq
import pandas as pd
import tempfile
import uuid
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
import decimal
import os

TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')

logger = Logger()
tracer = Tracer()

S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb')
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)

@tracer.capture_lambda_handler
def lambda_handler(event, context):

    try:
        for record in event['Records']:
            body = json.loads(record['body'])
            s3_key = body.get('s3Key')
            table_name = body.get('tableName')
            
            merchant_id = body.get('merchantId', 'unknown')
            job_id = body.get('jobId', 'unknown')

            if s3_key.startswith("s3://"):
                s3_key = s3_key[5:]
            bucket, key = s3_key.split("/", 1)
            
            table = DDB_RESOURCE.Table(table_name)
            
            items_to_write = []
            with tempfile.NamedTemporaryFile() as tmp_file:
                S3_CLIENT.download_file(bucket, key, tmp_file.name)
                table_arrow = pq.read_table(tmp_file.name)
                df = table_arrow.to_pandas()
            
            for _, row in df.iterrows():
                item = row.to_dict()
                for k, v in item.items():
                    if pd.isnull(v):
                        item[k] = None
                    elif isinstance(v, float):
                        item[k] = decimal.Decimal(str(v))
                    elif isinstance(v, str) and v.strip() == "":
                        item[k] = None
                items_to_write.append(item)

            # Batch write to DynamoDB
            batchWriteToDynamoDB(table, items_to_write)
            
            # Extract file name from S3 key
            file_name = s3_key.split("/")[-1]
            
            # Log successful document processing
            createTimelineRecord(
                job_id, merchant_id, 'system',
                'Document Processing Completed',
                f"Successfully processed {len(items_to_write)} records from {file_name}",
                'reconciliation',
            )

        return {
            "statusCode": 200,
            "body": json.dumps("DynamoDB insertion complete.")
        }

    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        
        createTimelineRecord(
            'unknown', 'unknown', 'system',
            'Document Processing Failed',
            f"Error processing document: {str(ex)}",
            'reconciliation', 'error', 'document_processing'
        )
        
        return {
            "statusCode": 500,
            "body": "The server encountered an unexpected condition that prevented it from fulfilling your request."
        }
    
@tracer.capture_method
def batchWriteToDynamoDB(table, items):
    """
    Write items to a DynamoDB table in batches.
    
    Args:
        table: DynamoDB table resource
        items: List of items to write to the table
        
    Returns:
        int: Number of items written
    """
    item_count = 0
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
            item_count += 1
            
    return item_count

@tracer.capture_method
def createTimelineRecord(job_id, merchant_id, user, title, description, record_type, additional_data=None):
    """Create a timeline record for audit trail"""
    if not TIMELINE_DDB_TABLE:
        logger.warning("Timeline table not configured, skipping timeline record creation")
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
        'module': 'reconciliation'
    }
    
    # Add any additional data
    if additional_data:
        timeline_item.update(additional_data)
    
    try:
        TIMELINE_DDB_TABLE.put_item(Item=timeline_item)
    except Exception as e:
        logger.error(f"Failed to create timeline record: {str(e)}")