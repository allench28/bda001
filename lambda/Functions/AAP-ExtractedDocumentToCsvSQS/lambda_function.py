import json
import os
import csv
import boto3
from datetime import datetime
from io import StringIO
from aws_lambda_powertools import Logger, Tracer

AAP_SMART_EYE_BUCKET = os.environ.get('AAP_SMART_EYE_BUCKET')
EXTRACTED_DOCUMENT_DDB = os.environ.get('EXTRACTED_DOCUMENT_DDB')

S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb')

EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_DDB)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        if not AAP_SMART_EYE_BUCKET:
            raise ValueError("AAP_SMART_EYE_BUCKET environment variable is not set")

        document_id = event.get('documentId')
        record = EXTRACTED_DOCUMENT_DDB_TABLE.get_item(
            Key={
                'extractedDocumentId': document_id
            }
        )

        document_data = record.get('Item')
        csv_content = generate_csv(document_data) 

        create_s3_object(document_data, csv_content, document_id)

        return {
            'statusCode': 200,
            'body': {
                'message': 'CSV file generated successfully',
                'record_id': document_id,
            }
        }
        
    except Exception as e:
        logger.exception("Error generating CSV")
        return {
            'statusCode': 500,
            'body': {
                'message': f'Error: {str(e)}'
            }
        }
    
@tracer.capture_method
def create_s3_object(document_data, csv_content, document_id):
    merchant_id = document_data.get('merchantId', 'unknown')
    doc_type = document_data.get('documentType', 'unknown').lower()
    
    s3_key = f"output/{merchant_id}/{doc_type}/{document_id}.csv"

    S3_CLIENT.put_object(
        Bucket=AAP_SMART_EYE_BUCKET,
        Key=s3_key,
        Body=csv_content,
        ContentType='text/csv'
    )
    

@tracer.capture_method
def generate_csv(record):
    data = record.get('data', {})
    form_data = data.get('formData', [])
    table_data = data.get('tableData', [])
    
    form_fields = {item['fieldMapping']: item['fieldValue'] for item in form_data}
    
    max_rows = 0
    table_columns = {}
    
    for column in table_data:
        field_name = column['fieldMapping']
        field_values = column.get('fieldValue', [])
        table_columns[field_name] = field_values
        max_rows = max(max_rows, len(field_values))
    
    headers = ['invoice id']
    headers.extend([field['fieldMapping'] for field in form_data])
    headers.extend([column['fieldMapping'] for column in table_data if column['fieldMapping'] not in headers])
    headers.extend(['created at', 'document type'])
    
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    
    if max_rows == 0:
        row = {'invoice id': record.get('extractedDocumentId', '')}
        row.update(form_fields)
        row['created at'] = record.get('createdAt', '')
        row['document type'] = record.get('documentType', '')
        writer.writerow(row)
    else:
        for i in range(max_rows):
            row = {'invoice id': record.get('extractedDocumentId', '')}
            row.update(form_fields)
            
            # Handle quotes in table values that might be JSON strings
            for field_name, values in table_columns.items():
                if i < len(values):
                    value = values[i]
                    # Remove quotes if the value is a JSON string with quotes
                    if isinstance(value, str) and value.startswith('"') and value.endswith('"'):
                        row[field_name] = value[1:-1]
                    else:
                        row[field_name] = value
                else:
                    row[field_name] = ''
                    
            row['created at'] = record.get('createdAt', '')
            row['document type'] = record.get('documentType', '')
            writer.writerow(row)
    
    return output.getvalue()