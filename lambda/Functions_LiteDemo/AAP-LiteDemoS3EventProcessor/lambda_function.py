"""
Lambda Function: LiteDemoS3EventProcessor
Trigger: S3 Event (OBJECT_CREATED)
Purpose: Process files uploaded to S3 input/ folder using Bedrock Data Automation

This function:
1. Updates document status to 'processing' in DynamoDB
2. Calls BDA to extract document data
3. Parses BDA results and formats them
4. Updates document status to 'completed' with extraction results in DynamoDB
"""

import os
import json
import time
import boto3
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.data_classes import S3Event
from prompts import PROMPTS
from bedrock_function import extraction_markdown_bedrock

logger = Logger()
tracer = Tracer()

# Environment variables
DOCUMENTS_TABLE_NAME = os.environ.get('DOCUMENTS_TABLE_NAME')
BDA_RUNTIME_ENDPOINT = os.environ.get('BDA_RUNTIME_ENDPOINT')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET')
BDA_PROJECT_ARN = os.environ.get('BDA_PROJECT_ARN')
BDA_PROFILE_ARN = os.environ.get('BDA_PROFILE_ARN')
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'qwen.qwen3-235b-a22b-2507-v1:0')

# AWS Clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
bda_runtime = boto3.client('bedrock-data-automation-runtime', region_name='us-east-1', endpoint_url=BDA_RUNTIME_ENDPOINT)
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-west-2')


class ProcessingError(Exception):
    """Custom exception for processing errors"""
    def __init__(self, message, context=None):
        self.message = message
        self.context = context or {}
        super().__init__(self.message)

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
                    # Step 1: Process document with BDA and get markdown
                    markdown_content = process_document_with_bda(bucket, key, document_id)

                    # Step 2: Classify documents into invoices and purchase orders
                    document_type = classify_document(markdown_content)
                    
                    # Step 2: Send markdown to Converse API for structured extraction
                    extraction_result = extract_structured_data_with_converse(markdown_content, document_type, document_id)
                    
                    # Step 3: Update DynamoDB with parsed results
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
                    
                    # BDA failed - update status to 'failed'
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
def classify_document(markdown_content):
    prompt = f"""
<markdown_content>
{markdown_content}
</markdown_content>

<task>
1. Identify the document type from <markdown_content>
2. return either invoice or po
</task>

<output_format>
{{"documentType": invoice/po}}
</output_format>
"""

    bedrock_response = extraction_markdown_bedrock(prompt)

    output_message = bedrock_response.get('output', {}).get('message', {})
    content_blocks = output_message.get('content', [])
    
    if not content_blocks:
        logger.info(f"No content in Bedrock response")
        return "invoice"
    
    response_text = content_blocks[0].get('text', '').strip()
    
    # Check if it's an error response
    if response_text.lower().startswith('error:'):
        logger.info(f"Bedrock classification failed: {response_text}")
        return "invoice"
    
    try:
        # Parse JSON response
        import json
        response_json = json.loads(response_text)
        document_type = response_json.get('documentType', 'invoice').lower()
        
        logger.info(f"Document classified as: {document_type}")
        return document_type
        
    except json.JSONDecodeError:
        logger.warning(f"Failed to parse JSON response, using raw text: {response_text}")
        return "invoice"




@tracer.capture_method
def extract_structured_data_with_converse(markdown_content, document_type, document_id):
    """
    Send markdown content to Converse API to extract structured data.
    
    Args:
        markdown_content: Markdown text from BDA
        document_id: Document identifier for logging
    
    Returns:
        dict: Structured extraction result with headerInformation and lineItems
    
    Raises:
        ProcessingError: If Converse API fails
    """
    try:
        logger.info(f"Starting Converse API extraction", extra={
            'document_id': document_id,
            'document_type': document_type,
            'markdown_length': len(markdown_content)
        })
        
        # Build prompt for extraction
        prompt = build_extraction_prompt(markdown_content, document_type)
        
        # Call Bedrock Converse API
        response = bedrock_runtime.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ],
            inferenceConfig={
                "maxTokens": 4096,
                "temperature": 0,
                "topP": 0.0001
            }
        )
        
        # Extract response text
        output_message = response.get('output', {}).get('message', {})
        content_blocks = output_message.get('content', [])
        
        if not content_blocks:
            raise ProcessingError("No response from Converse API")
        
        response_text = content_blocks[0].get('text', '')
        
        logger.info(f"Received Converse API response", extra={
            'document_id': document_id,
            'response_length': len(response_text)
        })
        
        # Parse JSON response from Converse
        extraction_result = parse_converse_response(response_text, document_type)
        
        return extraction_result
        
    except ProcessingError:
        raise
    except Exception as e:
        logger.error(f"Error in Converse API extraction: {str(e)}", extra={
            'document_id': document_id,
            'error': str(e)
        })
        raise ProcessingError(f"Converse API extraction failed: {str(e)}")


@tracer.capture_method
def build_extraction_prompt(markdown_content, document_type):
    prompt = PROMPTS[document_type.lower()].format(markdown_content=markdown_content)

    return prompt


# Display name mapping configuration by document type
DISPLAY_NAME_MAPPING = {
    'invoice': {
        # Header fields
        'AccountNumber':  {'columnName': 'accountNumber', 'displayName': 'Account Number'},
        'BillingPeriod':  {'columnName': 'billingPeriod', 'displayName': 'Billing Period'},
        'ContractNo':  {'columnName': 'contractNumber', 'displayName': 'Contract Number'},
        'Vendor': {'columnName': 'vendorName', 'displayName': 'Vendor Name'},
        'VendorAddress':  {'columnName': 'vendorAddress', 'displayName': 'Vendor Address'},
        'InvoiceNumber': {'columnName': 'invoiceNumber', 'displayName': 'Invoice Number'},
        'InvoiceDate': {'columnName': 'invoiceDate', 'displayName': 'Invoice Date'},
        'TotalCharge': {'columnName': 'invoiceAmount', 'displayName': 'Total Price'},
        'Currency': {'columnName': 'currency', 'displayName': 'Currency'},
        'DueDate': {'columnName': 'dueDate', 'displayName': 'Due Date'},
        'TaxAmount': {'columnName': 'taxAmount', 'displayName': 'Tax Amount'},
        'TaxRate':  {'columnName': 'taxRate', 'displayName': 'Tax Rate'},
        'TaxType':  {'columnName': 'taxType', 'displayName': 'Tax Type'},
        
        # Line item fields
        'Description': {'columnName': 'itemDescription', 'displayName': 'Item Description'},
        'Quantity': {'columnName': 'quantity', 'displayName': 'Quantity'},
        'UnitPrice': {'columnName': 'unitPrice', 'displayName': 'Unit Price'},
        'TotalAmount': {'columnName': 'totalItemPrice', 'displayName': 'Total Item Price'},
        'UOM': {'columnName': 'uom', 'displayName': 'Unit of Measure'}
    },
    'po': {
        # Header fields
        'Vendor': {'columnName': 'supplierName', 'displayName': 'Supplier Name'},
        'PONumber': {'columnName': 'poNumber', 'displayName': 'PO Number'},
        'PODate': {'columnName': 'poDate', 'displayName': 'PO Date'},
        'DeliveryDate': {'columnName': 'deliveryDate', 'displayName': 'Delivery Date'},
        'TotalAmount': {'columnName': 'totalAmount', 'displayName': 'Total Amount'},
        
        # Line item fields
        'Description': {'columnName': 'itemDescription', 'displayName': 'Item Description'},
        'Quantity': {'columnName': 'quantity', 'displayName': 'Quantity Ordered'},
        'UnitPrice': {'columnName': 'unitPrice', 'displayName': 'Unit Price'},
        'TotalAmount': {'columnName': 'lineTotal', 'displayName': 'Line Total'},
        'UOM': {'columnName': 'uom', 'displayName': 'Unit of Measure'}
    }
}

@tracer.capture_method
def parse_converse_response(response_text, document_type='invoice'):
    """
    Parse array response from Converse API and convert to expected format.
    
    Args:
        response_text: Response text from Converse API (array format)
    
    Returns:
        dict: Structured extraction result with data.formData and data.tableData
    
    Raises:
        ProcessingError: If parsing fails
    """
    try:
        # Clean response text
        cleaned_text = response_text.strip()
        if cleaned_text.startswith('```json'):
            cleaned_text = cleaned_text[7:]
        if cleaned_text.startswith('```'):
            cleaned_text = cleaned_text[3:]
        if cleaned_text.endswith('```'):
            cleaned_text = cleaned_text[:-3]
        cleaned_text = cleaned_text.strip()
        
        # Parse array response
        entities = json.loads(cleaned_text)
        
        # Convert to expected format
        form_data = []
        line_items = {}
        
        for entity in entities:
            entity_name = entity.get('entityName', '')
            entity_value = entity.get('entityValue', '')
            confidence = entity.get('confidence', 100)
            
            if entity_name.startswith('lineItem'):
                # Extract line item number and field
                parts = entity_name.split('_', 1)
                if len(parts) == 2:
                    item_key = parts[0]  # e.g., 'lineItem1'
                    field_name = parts[1]  # e.g., 'Description'
                    
                    if item_key not in line_items:
                        line_items[item_key] = []
                    
                    # Get mapping for line item field
                    doc_mapping = DISPLAY_NAME_MAPPING.get(document_type, {})
                    mapping = doc_mapping.get(field_name, {
                        'columnName': field_name.lower(),
                        'displayName': field_name
                    })
                    
                    line_items[item_key].append({
                        'columnName': mapping['columnName'],
                        'columnValue': entity_value,
                        'confidenceScore': confidence,
                        'displayName': mapping['displayName']
                    })
            else:
                # Header field - add to formData
                doc_mapping = DISPLAY_NAME_MAPPING.get(document_type, {})
                mapping = doc_mapping.get(entity_name, {
                    'columnName': entity_name.lower(),
                    'displayName': entity_name
                })
                
                form_data.append({
                    'columnName': mapping['columnName'],
                    'columnValue': entity_value,
                    'confidenceScore': confidence,
                    'displayName': mapping['displayName']
                })
        
        # Convert line_items dict to array format
        table_data = []
        for i in range(1, 100):  # Support up to 99 line items
            item_key = f'lineItem{i}'
            if item_key in line_items:
                table_data.append(line_items[item_key])
        
        result = {
            'data': {
                'formData': form_data,
                'tableData': table_data
            }
        }
        
        logger.info(f"Successfully parsed array response", extra={
            'form_data_count': len(form_data),
            'table_data_count': len(table_data)
        })
        
        return result
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON array response: {str(e)}")
        raise ProcessingError(f"Invalid JSON in array response: {str(e)}")
    except Exception as e:
        logger.error(f"Error parsing array response: {str(e)}")
        raise ProcessingError(f"Failed to parse array response: {str(e)}")


@tracer.capture_method
def update_document_with_results(document_id, extraction_result):
    """
    Update document in DynamoDB with extraction results.
    
    Args:
        document_id: Document identifier
        extraction_result: Structured extraction result
    
    Raises:
        ProcessingError: If update fails
    """
    try:
        table = dynamodb.Table(DOCUMENTS_TABLE_NAME)
        current_iso = datetime.now().isoformat()
        
        # Get existing document
        response = table.query(
            KeyConditionExpression='documentId = :doc_id',
            ExpressionAttributeValues={':doc_id': document_id},
            Limit=1
        )
        
        if not response['Items']:
            raise ProcessingError(f"Document not found in DynamoDB: {document_id}")
        
        item = response['Items'][0]
        
        # Update with extraction results
        item['status'] = 'completed'
        item['data'] = extraction_result.get('data', {})
        item['processedAt'] = current_iso
        item['updatedAt'] = current_iso
        
        # Remove error message if exists
        if 'errorMessage' in item:
            del item['errorMessage']
        
        # Save to DynamoDB
        table.put_item(Item=item)
        
        logger.info(f"Successfully updated document with extraction results", extra={
            'document_id': document_id,
            'status': 'completed',
            'form_data_count': len(extraction_result.get('data', {}).get('formData', [])),
            'table_data_count': len(extraction_result.get('data', {}).get('tableData', []))
        })
        
    except ProcessingError:
        raise
    except Exception as e:
        logger.error(f"Failed to update document with results: {str(e)}", extra={
            'document_id': document_id,
            'error': str(e)
        })
        raise ProcessingError(f"Failed to update document: {str(e)}")

@tracer.capture_method
def process_document_with_bda(bucket, key, document_id):
    """
    Process document using Bedrock Data Automation (BDA) and extract markdown content.
    
    Steps:
    1. Start BDA async extraction
    2. Poll for completion
    3. Get result.json from S3
    4. Parse and extract markdown content
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        document_id: Document identifier
    
    Returns:
        str: Markdown content extracted from BDA
    
    Raises:
        ProcessingError: If BDA processing fails
    """
    try:
        logger.info(f"Starting BDA extraction", extra={
            'document_id': document_id,
            'bucket': bucket,
            'key': key
        })
        
        # Step 1: Start BDA extraction
        invocation_arn = start_bda_extraction(bucket, key)
        
        # Step 2: Poll for completion
        bda_status = poll_bda_job(invocation_arn, max_retries=40, delay=3)
        
        if bda_status.get('status') != 'Success':
            raise ProcessingError(
                f"BDA extraction failed with status: {bda_status.get('status')}",
                {'invocation_arn': invocation_arn}
            )
        
        # Step 3: Get job metadata URI from status response
        job_metadata_uri = bda_status.get('outputConfiguration', {}).get('s3Uri')
        
        if not job_metadata_uri:
            raise ProcessingError(
                "No job metadata URI found in BDA status response",
                {'invocation_arn': invocation_arn}
            )
        
        # Step 4: Extract markdown from BDA output
        markdown_content = extract_markdown_from_bda(job_metadata_uri)
        
        logger.info(f"BDA extraction completed successfully", extra={
            'document_id': document_id,
            'invocation_arn': invocation_arn,
            'markdown_length': len(markdown_content)
        })
        
        return markdown_content
        
    except ProcessingError:
        raise
    except Exception as e:
        logger.error(f"Error in BDA processing: {str(e)}", extra={
            'document_id': document_id,
            'error': str(e)
        })
        raise ProcessingError(f"BDA processing failed: {str(e)}")

@tracer.capture_method
def start_bda_extraction(bucket_name, key):
    """
    Start BDA async extraction job
    """
    input_s3_uri = f"s3://{bucket_name}/{key}"
    output_uri = f"s3://{OUTPUT_BUCKET}/output"
    
    response = bda_runtime.invoke_data_automation_async(
        dataAutomationConfiguration={"dataAutomationProjectArn": BDA_PROJECT_ARN},
        dataAutomationProfileArn=BDA_PROFILE_ARN,
        inputConfiguration={"s3Uri": input_s3_uri},
        outputConfiguration={"s3Uri": output_uri}
    )
    
    invocation_arn = response.get('invocationArn')
    if not invocation_arn:
        raise Exception(f"Failed to start BDA extraction. No invocationArn returned.")
    
    logger.info(f"BDA extraction started", extra={
        'invocation_arn': invocation_arn,
        'input_uri': input_s3_uri
    })
    
    return invocation_arn

@tracer.capture_method
def poll_bda_job(invocation_arn, max_retries=40, delay=3):
    """
    Poll BDA job until completion or timeout.
    
    Args:
        invocation_arn: BDA invocation ARN
        max_retries: Maximum number of polling attempts (default: 40 = 2 minutes)
        delay: Delay between polls in seconds (default: 3)
    
    Returns:
        dict: BDA status response
    """
    for attempt in range(max_retries):
        response = bda_runtime.get_data_automation_status(invocationArn=invocation_arn)
        status = response.get('status')
        
        logger.info(f"BDA job status: {status}", extra={
            'attempt': attempt + 1,
            'max_retries': max_retries,
            'invocation_arn': invocation_arn
        })
        
        if status in ['Success', 'Failed', 'Aborted']:
            return response
        elif status == 'InProgress':
            time.sleep(delay)
        else:
            response["status"] = "Failed"
            response["message"] = f"Unknown status: {status}"
            return response
    
    # Timeout
    response = {"status": "Failed", "message": "Polling timeout exceeded"}
    return response

@tracer.capture_method
def extract_markdown_from_bda(job_metadata_uri):
    """
    Extract markdown content from BDA output using job metadata.
    
    Args:
        job_metadata_uri: S3 URI to job_metadata.json from BDA
    
    Returns:
        str: Markdown content
    
    Raises:
        ProcessingError: If extraction fails
    """
    try:
        # Parse job_metadata.json URI
        uri_parts = job_metadata_uri.replace('s3://', '').split('/', 1)
        metadata_bucket = uri_parts[0]
        metadata_key = uri_parts[1] if len(uri_parts) > 1 else ''
        
        logger.info(f"Retrieving job metadata from: {job_metadata_uri}")
        
        # Download and parse job_metadata.json
        metadata_response = s3_client.get_object(Bucket=metadata_bucket, Key=metadata_key)
        metadata_content = metadata_response['Body'].read().decode('utf-8')
        job_metadata = json.loads(metadata_content)
        
        # Extract standard_output_path from job metadata
        output_metadata_list = job_metadata.get('output_metadata', [])
        if not output_metadata_list:
            raise ProcessingError("No output_metadata found in job metadata")
        
        segment_metadata = output_metadata_list[0].get('segment_metadata', [])
        if not segment_metadata:
            raise ProcessingError("No segment_metadata found in output metadata")
        
        result_json_uri = segment_metadata[0].get('standard_output_path')
        if not result_json_uri:
            raise ProcessingError("No standard_output_path found in segment metadata")
        
        logger.info(f"Found result.json at: {result_json_uri}")
        
        # Parse result.json URI
        result_uri_parts = result_json_uri.replace('s3://', '').split('/', 1)
        result_bucket = result_uri_parts[0]
        result_key = result_uri_parts[1] if len(result_uri_parts) > 1 else ''
        
        # Download and parse result.json
        result_response = s3_client.get_object(Bucket=result_bucket, Key=result_key)
        result_content = result_response['Body'].read().decode('utf-8')
        bda_output = json.loads(result_content)
        
        # Extract markdown content from BDA output
        markdown_content = parse_bda_json_to_markdown(bda_output)
        
        logger.info(f"Successfully extracted markdown content", extra={
            'markdown_length': len(markdown_content)
        })
        
        return markdown_content
        
    except ProcessingError:
        raise
    except Exception as e:
        logger.error(f"Error extracting markdown from BDA: {str(e)}")
        raise ProcessingError(f"Failed to extract markdown: {str(e)}")


@tracer.capture_method
def parse_bda_json_to_markdown(bda_output):
    """
    Parse BDA JSON output and convert to markdown format.
    
    BDA structure:
    - pages[].representation.markdown: Full page markdown
    - elements[].representation.markdown: Individual element markdown
    
    Args:
        bda_output: BDA result.json content as dict
    
    Returns:
        str: Markdown formatted content
    """
    markdown_parts = []
    
    # Check if we have pages with markdown representation
    pages = bda_output.get('pages', [])
    
    if not pages:
        raise ProcessingError("No pages found in BDA output")
    
    logger.info(f"Parsing BDA output with {len(pages)} page(s)")
    
    # Strategy 1: Use page-level markdown if available
    for page_idx, page in enumerate(pages, start=1):
        page_markdown = page.get('representation', {}).get('markdown', '')
        
        if page_markdown.strip():
            markdown_parts.append(f"## Page {page_idx}\n\n{page_markdown}")
            markdown_parts.append("\n---\n")
            logger.info(f"Extracted markdown from page {page_idx} representation")
    
    # Strategy 2: If no page markdown, fall back to elements
    if not markdown_parts:
        logger.info("No page-level markdown found, extracting from elements")
        
        elements = bda_output.get('elements', [])
        
        if not elements:
            raise ProcessingError("No elements found in BDA output")
        
        # Sort elements by reading_order
        sorted_elements = sorted(elements, key=lambda e: e.get('reading_order', 0))
        
        for elem in sorted_elements:
            elem_markdown = elem.get('representation', {}).get('markdown', '')
            
            if elem_markdown.strip():
                markdown_parts.append(elem_markdown)
                markdown_parts.append("\n")
    
    # Combine all parts
    markdown_content = '\n'.join(markdown_parts).strip()
    
    if not markdown_content:
        raise ProcessingError("No text content extracted from BDA output")
    
    logger.info(f"Successfully parsed markdown content: {len(markdown_content)} characters")
    
    return markdown_content

