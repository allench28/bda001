import json
import os
import urllib3
import uuid
import boto3
from aws_lambda_powertools import Logger, Tracer
from io import StringIO
import csv
from datetime import datetime

N8N_INVOICE_WEBHOOK_URL = os.environ.get('N8N_INVOICE_WEBHOOK_URL')
N8N_PO_WEBHOOK_URL = os.environ.get('N8N_PO_WEBHOOK_URL')

EXTRACTED_DOCUMENTS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE')
EXTRACTED_PO_TABLE = os.environ.get('EXTRACTED_PO_TABLE')
EXTRACTED_PO_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_PO_LINE_ITEM_TABLE')
AAP_SMART_EYE_BUCKET = os.environ.get('AAP_SMART_EYE_BUCKET')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3')

EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
EXTRACTED_DOCUMENTS_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE)
EXTRACTED_PO_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_PO_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # Process SQS event
        record = event['Records'][0]
        logger.info(f"Processing record: {record}")
        
        message_body = json.loads(record['body'])
        document_id = message_body.get('documentId')
        merchant_id = message_body.get('merchantId')
        document_type = message_body.get('documentType')
        erp_type = message_body.get('erp_type', 'quickbook')
        
        header_mapping = get_csv_mapping_config(merchant_id, document_type, erp_type)

        document = get_document(document_id, document_type)

        line_items = get_line_items(document_id, document_type)

        csv_content = generate_csv_content(header_mapping, document, line_items)
        
        presigned_url = create_s3_object(merchant_id, document_type, csv_content, document_id)
        
        if document_type == 'invoice':
            webhook_url = N8N_INVOICE_WEBHOOK_URL
        else:  # PO
            webhook_url = N8N_PO_WEBHOOK_URL

        http = urllib3.PoolManager()
        response = http.request(
            'POST',
            webhook_url,
            body=json.dumps({
                "presigned_url": presigned_url,
                "merchant_id": merchant_id
            }),
            headers={'Content-Type': 'application/json'}
        )

        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Update document status based on webhook response
        if response.status != 200:
            response_body = json.loads(response.data.decode('utf-8'))
            exception_status = response_body.get('cause', 'error')
            status = "Failed to Submit to ERP"
            update_params = {
                'documentStatus': status,
                'exceptionStatus': exception_status
            }
        else:
            status = "Submitted to ERP"
            update_params = {
                'documentStatus': status
            }

        updated_document = update_document_status(document_id, document_type, update_params, now)
        create_Timeline_Record(merchant_id, updated_document, document_type, now)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Webhook called successfully',
                'document_status': status
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing request',
                'error': str(e)
            })
        }

@tracer.capture_method
def get_csv_mapping_config(merchant_id, document_type, erp_type):
    s3_key = f"mapping/{merchant_id}/{document_type}/erp_{erp_type}.json"
    response = S3_CLIENT.get_object(
        Bucket=AAP_SMART_EYE_BUCKET,
        Key=s3_key
    )

    mapping_config = json.loads(response['Body'].read().decode('utf-8'))
    return mapping_config

@tracer.capture_method
def get_document(document_id, document_type):
    if document_type == 'invoice':
        response = EXTRACTED_DOCUMENTS_DDB_TABLE.get_item(Key={'extractedDocumentsId': document_id})
    else:  # PO
        response = EXTRACTED_PO_DDB_TABLE.get_item(Key={'extractedPoId': document_id})
    
    return response.get('Item', {})
    
@tracer.capture_method
def get_line_items(document_id, document_type):
    if document_type == 'invoice':
        index_name = 'gsi-extractedDocumentsId'
        table = EXTRACTED_DOCUMENTS_LINE_ITEM_DDB_TABLE
        key_name = 'extractedDocumentsId'
    else:  # PO
        index_name = 'gsi-extractedPoId'
        table = EXTRACTED_PO_LINE_ITEM_DDB_TABLE
        key_name = 'extractedPoId'
    
    response = table.query(
        IndexName=index_name,
        KeyConditionExpression=boto3.dynamodb.conditions.Key(key_name).eq(document_id)
    )

    return response.get('Items', [])

@tracer.capture_method
def generate_csv_content(mapping_config, document_data, line_items):
    headers = mapping_config.get("headers", [])
    document_field_mapping = mapping_config.get("document_fields", {})
    line_item_field_mapping = mapping_config.get("line_item_fields", {})
    
    # Create a CSV string buffer
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(headers)
    
    # Extract document data using the mapping
    document_values = {}
    for header, field_name in document_field_mapping.items():
        document_values[header] = document_data.get(field_name, '')
    
    # If no line items, create at least one row with document data
    if not line_items:
        row = []
        for header in headers:
            row.append(document_values.get(header, ''))
        csv_writer.writerow(row)
    else:
        # Create a row for each line item with common document data
        for item in line_items:
            row = []
            for header in headers:
                if header in document_field_mapping:
                    row.append(document_values.get(header, ''))
                elif header in line_item_field_mapping:
                    field_name = line_item_field_mapping[header]
                    row.append(item.get(field_name, ''))
                else:
                    row.append('')
            csv_writer.writerow(row)
    
    # Get the CSV content as string
    csv_content = csv_buffer.getvalue()
    csv_buffer.close()
    
    return csv_content

@tracer.capture_method
def create_s3_object(merchant_id, document_type, csv_content, document_id):
    s3_key = f"approved/{merchant_id}/{document_type}/{document_id}.csv"

    S3_CLIENT.put_object(
        Bucket=AAP_SMART_EYE_BUCKET,
        Key=s3_key,
        Body=csv_content,
        ContentType='text/csv'
    )

    presigned_url = S3_CLIENT.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': AAP_SMART_EYE_BUCKET,
                'Key': s3_key
            },
            ExpiresIn=300  
        )
    
    return presigned_url


@tracer.capture_method
def update_document_status(document_id, document_type, params, timestamp):
    update_expression = 'SET updatedAt = :updatedAt, updatedBy = :updatedBy'
    expression_values = {
        ':updatedAt': timestamp,
        ':updatedBy': 'n8n'
    }
    
    for key, value in params.items():
        update_expression += f', {key} = :{key}'
        expression_values[f':{key}'] = value
    
    if document_type == 'invoice':
        response = EXTRACTED_DOCUMENTS_DDB_TABLE.update_item(
            Key={'extractedDocumentsId': document_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ReturnValues="ALL_NEW"
        )
    else:  # PO
        response = EXTRACTED_PO_DDB_TABLE.update_item(
            Key={'extractedPoId': document_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ReturnValues="ALL_NEW"
        )

    return response.get('Attributes', {})
# create time line record
@tracer.capture_method
def create_Timeline_Record(merchantId, documentData, document_type, now):
    if documentData['documentStatus'] == "Submitted to ERP":
        title = "Document Submitted to ERP"
        description = "Document successfully submitted to ERP"
    else:
        title = "Document Failed to Submit to ERP"
        description = documentData.get('exceptionStatus')

    logger.info(documentData)

    timelinePayload = {
        "timelineId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "title": title,
        "type": document_type,
        "description": description,
        "supplierName": documentData.get("supplierName", "-"),
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
    }

    if document_type == 'invoice':
        timelinePayload["timelineForId"] = documentData.get("extractedDocumentsId")
        timelinePayload["invoiceNumber"] = documentData.get("invoiceNumber", "-")
    else:  # PO
        timelinePayload["timelineForId"] = documentData.get("extractedPoId")
        timelinePayload["poNumber"] = documentData.get("poNumber", "-")

    TIMELINE_DDB_TABLE.put_item(Item=timelinePayload)