import boto3
import json
import os
import uuid
from decimal import Decimal
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import ResourceNotFoundException, BadRequestException
from netsuite_client import NetSuiteClient
from authorizationHelper import is_authenticated, get_user

# Environment variables for configuration
MERCHANT_TABLE = os.getenv('MERCHANT_TABLE')
NS_CREDENTIALS = os.getenv('NS_CREDENTIALS')
S3_BUCKET = os.getenv('SMART_EYE_BUCKET', 'aap-data-processing')
EXTRACTED_PO_TABLE = os.getenv('EXTRACTED_PO_TABLE', 'AAP-ExtractedPo')
EXTRACTED_PO_LINE_ITEMS_TABLE = os.getenv('EXTRACTED_PO_LINE_ITEMS_TABLE', 'AAP-ExtractedPoLineItems')
EXTRACTED_GRN_TABLE = os.getenv('EXTRACTED_GRN_TABLE', 'AAP-ExtractedGrn')
EXTRACTED_GRN_LINE_ITEMS_TABLE = os.getenv('EXTRACTED_GRN_LINE_ITEMS_TABLE', 'AAP-ExtractedGrnLineItems')
USER_TABLE = os.getenv('USER_TABLE', 'AAP-User')
USER_GROUP_TABLE = os.getenv('USER_GROUP_TABLE', 'AAP-UserGroup')

# AWS resources
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')
SECRETS_MANAGER = boto3.client('secretsmanager', region_name='ap-southeast-1')
S3_CLIENT = boto3.client('s3', region_name='ap-southeast-1')

# DynamoDB tables
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

# Logging and tracing
logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """
    Main Lambda handler function to process NetSuite data
    Handles both direct Lambda invocation and API Gateway requests

    Example event for API Gateway:

    {
        "resource": "/netsuite-documents",
        "path": "/netsuite-documents",
        "httpMethod": "POST",
        "headers": {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/json",
            "Authorization": "Bearer eyJraWQiOiJrZXkxIiwiYWxnIjoiUlMyNTYifQ...",
            "Host": "api.example.com",
            "User-Agent": "PostmanRuntime/7.32.3",
            "X-Amzn-Trace-Id": "Root=1-6457b345-0c3f0e04f10d35642b92491a",
            "X-Forwarded-For": "192.168.0.1",
            "X-Forwarded-Port": "443",
            "X-Forwarded-Proto": "https"
        },
        "multiValueHeaders": {
            "Accept": ["application/json"],
            "Accept-Encoding": ["gzip, deflate, br"],
            "Content-Type": ["application/json"],
            "Authorization": ["Bearer eyJraWQiOiJrZXkxIiwiYWxnIjoiUlMyNTYifQ..."],
            "Host": ["api.example.com"],
            "User-Agent": ["PostmanRuntime/7.32.3"],
            "X-Amzn-Trace-Id": ["Root=1-6457b345-0c3f0e04f10d35642b92491a"],
            "X-Forwarded-For": ["192.168.0.1"],
            "X-Forwarded-Port": ["443"],
            "X-Forwarded-Proto": ["https"]
        },
        "queryStringParameters": null,
        "multiValueQueryStringParameters": null,
        "pathParameters": null,
        "stageVariables": null,
        "body": "{\"modules\": [{\"type\": \"purchase-order\"}, {\"type\": \"item-receipt\"}]}",
        "isBase64Encoded": false,
        "requestContext": {
            "resourceId": "abc123",
            "resourcePath": "/netsuite-documents",
            "httpMethod": "POST",
            "extendedRequestId": "example-request-id",
            "requestTime": "25/Mar/2023:12:34:56 +0000",
            "path": "/netsuite-documents",
            "accountId": "123456789012",
            "protocol": "HTTP/1.1",
            "stage": "prod",
            "domainPrefix": "api",
            "requestTimeEpoch": 1679744096000,
            "requestId": "a1b2c3d4-5678-90ab-cdef-example11111",
            "identity": {
            "cognitoIdentityPoolId": null,
            "accountId": null,
            "cognitoIdentityId": null,
            "caller": null,
            "sourceIp": "192.168.0.1",
            "principalOrgId": null,
            "accessKey": null,
            "cognitoAuthenticationType": null,
            "cognitoAuthenticationProvider": null,
            "userArn": null,
            "userAgent": "PostmanRuntime/7.32.3",
            "user": null
            },
            "domainName": "api.example.com",
            "apiId": "api-id"
        }
    }
    """
    try:
        # Handle API Gateway events
        if event.get('httpMethod') or event.get('requestContext'):
            logger.info("Processing API Gateway request")
            
            # Authenticate user
            try:
                sub, _, _ = is_authenticated(event)
                user = get_user(sub)
                merchant_id = user.get('merchantId')
                
                # Parse request body if present
                if event.get('body'):
                    try:
                        body = json.loads(event.get('body'))
                    except json.JSONDecodeError:
                        return create_response(400, "Invalid JSON in request body")
                else:
                    body = {}
                
                # Get modules from request or use default
                modules = body.get('modules', [
                    {"type": "purchase-order"},
                    {"type": "item-receipt"}
                ])
                
                # Get credentials for the merchant
                secret = getNSCredentials(merchant_id)
                if not secret:
                    return create_response(404, f"NetSuite credentials not found for merchant {merchant_id}")
                
                # Create NetSuite client
                netsuite_client = NetSuiteClient(
                    account=secret.get('account'),
                    consumer_key=secret.get('consumer_key'),
                    consumer_secret=secret.get('consumer_secret'),
                    token_id=secret.get('token_id'),
                    token_secret=secret.get('token_secret')
                )
                
                # Process records
                csv_output_files = []
                for module in modules:
                    resource_type = module.get('type')
                    if not resource_type:
                        continue
                    
                    ns_records = netsuite_client.get_records(resource_type)
                    if not ns_records:
                        continue
                    
                    mapped_headers, mapped_line_items = mapRecordData(resource_type, merchant_id, ns_records)
                    
                    if not mapped_headers or not mapped_line_items:
                        continue
                    
                    # Store records in DynamoDB
                    store_records_response = storeRecordsInDynamoDB(resource_type, mapped_headers, mapped_line_items)
                    
                    if store_records_response:
                        csv_output_files.append({
                            'type': resource_type,
                            'records_processed': len(mapped_headers)
                        })
                
                # Return success
                return create_response(200, "Successfully processed NetSuite data", {
                    'status': True,
                    'processed_modules': csv_output_files
                })
                
            except (ResourceNotFoundException, BadRequestException) as ex:
                return create_response(400, str(ex))
            except Exception as ex:
                logger.exception(f"Error processing API request: {str(ex)}")
                return create_response(500, "Internal server error")
        
        # Original event handling for EventBridge/CloudWatch events
        else:
            detail = event.get('detail') or {}
            merchants = detail.get('merchants') or []
            csv_output_files = []

            for merchant_tx in merchants:
                merchant_id = merchant_tx.get('merchantId')
                if not merchant_id:
                    continue

                merchant_details = getMerchantDetails(merchant_id)
                if not merchant_details:
                    continue

                modules = merchant_tx.get('modules', [])
                secret = getNSCredentials(merchant_id)
                if not secret:
                    continue

                netsuite_client = NetSuiteClient(
                    account=secret.get('account'),
                    consumer_key=secret.get('consumer_key'),
                    consumer_secret=secret.get('consumer_secret'),
                    token_id=secret.get('token_id'),
                    token_secret=secret.get('token_secret')
                )

                for module in modules:
                    resource_type = module.get('type')
                    if not resource_type:
                        continue
                    
                    ns_records = netsuite_client.get_records(resource_type)
                    if not ns_records:
                        continue

                    mapped_headers, mapped_line_items = mapRecordData(resource_type, merchant_id, ns_records)

                    if not mapped_headers or not mapped_line_items:
                        continue
                    
                    logger.info(f"Processing {resource_type} for merchant {merchant_id} with {len(mapped_headers)} headers and {len(mapped_line_items)} line items")
                    logger.info(mapped_headers[:5])  # Log first 5 headers for debugging
                    logger.info(mapped_line_items[:20])  # Log first 20 line items for debugging

                    store_records_response = storeRecordsInDynamoDB(resource_type, mapped_headers, mapped_line_items)

                    logger.info(f"Stored {len(mapped_headers)} headers and {len(mapped_line_items)} line items in DynamoDB for {resource_type} of merchant {merchant_id}")
                    logger.info(f"Store records response: {store_records_response}")

                    if not store_records_response:
                        continue

            return {
                'status': True, 
                'message': "Processed NetSuite data successfully",
                'csv_files': csv_output_files
            }

    except (ResourceNotFoundException, BadRequestException) as ex:
        logger.error(str(ex))
        return {'status': False, 'message': str(ex)}

    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception("Unexpected error occurred")
        return {'status': False, 'message': str(ex)}

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
        }, default=decimal_default)
    }

def decimal_default(obj):
    """Helper function for JSON serialization of Decimal types"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

@tracer.capture_method
def getNSCredentials(merchant_id):
    """Retrieve NetSuite credentials from Secrets Manager"""
    secret_key = f"{NS_CREDENTIALS}_{merchant_id}"

    response = SECRETS_MANAGER.get_secret_value(SecretId=secret_key)
    secret_string = response.get('SecretString')
    
    if not response or not secret_string:
        return None

    return json.loads(secret_string)

@tracer.capture_method
def getMerchantDetails(merchant_id):
    """Retrieve merchant details from DynamoDB"""
    
    response = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchant_id})

    merchant = response.get('Item')

    if not response or not merchant:
        return None
    
    return merchant
   

@tracer.capture_method
def mapPurchaseOrders(merchantId, purchase_order_data):
    """
    Map NetSuite purchase order data to internal format
    
    Args:
        merchantId: Merchant ID
        purchase_order_data: Tuple containing (header_data, line_items)
        
    Returns:
        tuple: (mapped_headers_array, mapped_line_items_array) - Arrays of mapped PO headers and line items
    """
    mapped_headers = []
    mapped_line_items = []
    
    headers, line_items = purchase_order_data
    
    extracted_po_ids = {}
    
    for po in headers:
        po_id = po.get('id')
        po_tranid = po.get('tranid', 'Unknown')
        
        extracted_po_id = str(uuid.uuid4())
        extracted_po_ids[po_id] = extracted_po_id

        document_upload_id = str(uuid.uuid4())
        
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        poDetail = {
            "merchantId": merchantId,
            "poNumber": po_tranid,
            "approvedBy": "-",
            "approvedAt": "-",
            "generatedSoId": "-", 
            "extractedPoId": extracted_po_id,
            "boundingBoxes": {},
            "buyerAddress": po.get('buyeraddress', "-"),
            "buyerCode": po.get('buyercode', "-"),
            "buyerName": po.get('buyername', "-"),
            "confidenceScore": "100",
            "createdAt": now,
            "createdBy": "System",
            "currency": po.get('currency') or "-",
            "deliveryAddress": po.get('shippingaddress', "-"),
            "documentStatus": "Success",
            "documentType": "po",
            "documentUploadId": document_upload_id,
            "dueDate": po.get('duedate', "-"),
            "exceptionStatus": "N/A",
            "filePath": "",
            "merchantId": merchantId,
            "poDate": po.get('trandate', "-"),
            "requestDeliveryDate": po.get('shipdate', "-"),
            "sourceFile": "netsuite",
            "supplierAddress": po.get('vendoraddress', "-"),
            "supplierName": po.get('vendorname', "-"),
            "supplierCode": po.get('vendorid', "-"),
            "taxRate": po.get('taxrate', "-"),
            "taxType": po.get('taxtype', "-"),
            "totalAmountWithoutTax": po.get('totalAmountWithoutTax', "-"),
            "totalAmountWithTax": po.get('totalAmountWithTax', "-"),
            "totalTaxAmount": po.get('totalTaxAmount', "-"),
            "updatedAt": now,
            "paymentTerms": po.get('terms', "-"),
            "updatedBy": "System",
            "transactionStatus": po.get('status', '-'),
            "source": "netsuite",
        }
        
        mapped_headers.append(poDetail)
    
    # Process all line items
    for item in line_items:
        po_id = item.get('po_id')
        if not po_id or po_id not in extracted_po_ids:
            continue
            
        po_data = next((po for po in headers if po.get('id') == po_id), None)
        if not po_data:
            continue
            
        po_tranid = po_data.get('tranid', 'Unknown')
        extracted_po_id = extracted_po_ids[po_id]
        
        line_item_id = str(uuid.uuid4())
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        line_item = {
            "extractedPoLineItemsId": line_item_id,
            "boundingBoxes": {},
            "buyerCode": po_data.get('buyercode', '-'),
            "buyerName": po_data.get('buyername', '-'),
            "createdAt": now,
            "createdBy": "System",
            "currency": po_data.get('currency', '-'),
            "description": item.get('memo', '-'),
            "documentUploadId": document_upload_id,
            "exceptionStatus": "N/A",
            "extractedPoId": extracted_po_id,
            "itemCode": item.get('itemid', '-'),
            "itemUom": item.get('abbreviation', '') or item.get('unitname', ''),
            "merchantId": merchantId,
            "poNumber": po_tranid,
            "quantity": item.get('quantity', '-'),
            "status": "Success",
            "supplierCode": po_data.get('vendorid', '-'),
            "supplierName": po_data.get('vendorname', '-'),
            "taxAmount": item.get('taxamount', '0'),
            "totalPrice": item.get('netamount', '-'),
            "unitPrice": item.get('rate', '-'),
            "updatedAt": now,
            "updatedBy": "System",
            "generatedSoId": "-",
            "generatedSoLineItemId": "-",
            "sourceFile": "netsuite",
            "approvalStatus": po_data.get('approvalstatus', '-'),
            "source": "netsuite",
        }
        
        mapped_line_items.append(line_item)
    
    return (mapped_headers, mapped_line_items)

@tracer.capture_method
def mapItemReceipts(merchantId, item_receipt_data):
    """
    Map NetSuite item receipt data to internal format
    
    Args:
        merchantId: Merchant ID
        item_receipt_data: Tuple containing (header_data, line_items)
        
    Returns:
        tuple: (mapped_headers_array, mapped_line_items_array) - Arrays of mapped GRN headers and line items
    """
    mapped_headers = []
    mapped_line_items = []
    
    headers, line_items = item_receipt_data
    
    extracted_grn_ids = {}
    
    for ir in headers:
        ir_id = ir.get('id')
        po_number = ir.get('po_number', '')
        
        # Generate a UUID for the extracted GRN ID
        extracted_grn_id = str(uuid.uuid4())
        extracted_grn_ids[ir_id] = extracted_grn_id

        document_upload_id = str(uuid.uuid4())
        
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        grn_detail = {
            "extractedGrnId": extracted_grn_id,
            "merchantId": merchantId,
            "grnNumber": ir.get('tranid','-'),
            "grnDate": ir.get('trandate', ''),
            "purchaseOrderNo": po_number,
            "supplierName": ir.get('vendorname', ''),
            "supplierCode": ir.get('vendorid', ''),
            "statusOfGoodsReceived": ir.get('status', 'Goods Received'),
            "boundingBoxes": {},
            "confidenceScore": 100,
            "createdAt": now,
            "createdBy": "System",
            "documentStatus": "Success",
            "documentType": "grn",
            "documentUploadId": document_upload_id,
            "exceptionStatus": "N/A",
            "filePath": "",
            "remarks": "",
            "sourceFile": "netsuite",
            "updatedAt": now,
            "updatedBy": "System",
            "source": "netsuite",
        }
        
        mapped_headers.append(grn_detail)
    
    # Process all line items
    for item in line_items:
        ir_id = item.get('ir_id')
        if not ir_id or ir_id not in extracted_grn_ids:
            continue
            
        ir_data = next((ir for ir in headers if ir.get('id') == ir_id), None)
        if not ir_data:
            continue
            
        extracted_grn_id = extracted_grn_ids[ir_id]
        
        line_item_id = str(uuid.uuid4())
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Calculate total amount if possible, or use netAmount directly
        quantity = item.get('quantity', 0)
        
        item_name_full = item.get('name', '-')
        item_name_only = item_name_full.split('_')[1] if '_' in item_name_full else item_name_full

        line_item = {
            "extractedGrnLineItemsId": line_item_id,
            "extractedGrnId": extracted_grn_id,
            "merchantId": merchantId,
            "grnNumber": f"GRN-{ir_id}",
            "itemCode": item.get('itemid', '-'),
            "description": item_name_only,
            "quantity": str(quantity),
            "itemUom": item.get('abbreviation', '') or item.get('unitname', ''),
            "totalAmount": item.get('totalAmount', 0),
            "supplierName": ir_data.get('vendorname', ''),
            "supplierCode": ir_data.get('vendorid', ''),
            "status": "Success",
            "boundingBoxes": {},
            "createdAt": now,
            "createdBy": "System",
            "documentUploadId": document_upload_id,
            "exceptionStatus": "N/A",
            "sourceFile": "netsuite",
            "updatedAt": now,
            "updatedBy": "System",
            "source": "netsuite",
        }
        
        mapped_line_items.append(line_item)
    
    return (mapped_headers, mapped_line_items)

@tracer.capture_method
def mapRecordData(record_type, merchant_id, records):
    """Map record data using appropriate mapping strategy"""

    function_maps = {
        'purchase-order': mapPurchaseOrders,
        'item-receipt': mapItemReceipts
    }

    selected_map_function = function_maps.get(record_type)

    if not selected_map_function:
        logger.warning(f"No mapping function available for record type: {record_type}")
        return (None, None)
    
    return selected_map_function(merchant_id, records)

@tracer.capture_method
def storeRecordsAsCSV(record_type, headers, line_items):
    """Store the mapped data into CSV files"""
    import csv
    import os
    from datetime import datetime
    
    # Create a temporary directory for output files
    temp_dir = '/tmp/csv_output'
    os.makedirs(temp_dir, exist_ok=True)
    
    # Generate timestamp for unique filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Define csv path
    csv_path = f"{temp_dir}/{record_type}_data_{timestamp}.csv"
    
    # Define the CSV field names based on the specified headers
    fieldnames = [
        'PO Number',
        'PO Date',
        'Supplier Name',
        'Supplier ID',
        'Currency',
        'Payment Term',
        'Unit price',
        'Total Invoice Amount',
        'Tax Details (SST/VAT/GST)',
        'Tax Amount',
        'Item Code',
        'Description',
        'Ordered Quantity',
        'UOM',
        'Approval Status'
    ]
    
    # Map header and line item data to the CSV format
    csv_rows = []
    
    # Create a lookup dictionary for PO headers by extractedPoId
    po_headers_by_id = {header.get('extractedPoId'): header for header in headers}
    
    # Process line items and join with header information
    for line_item in line_items:
        po_id = line_item.get('extractedPoId')
        po_header = po_headers_by_id.get(po_id, {})
        
        # Map the data to the CSV row structure
        row = {
            'PO Number': po_header.get('poNumber', '-'),
            'PO Date': po_header.get('poDate', '-'),
            'Supplier Name': po_header.get('supplierName', '-'),
            'Supplier ID': '-',  # No direct mapping for supplier ID
            'Currency': po_header.get('currency', '-'),
            'Payment Term': po_header.get('paymentTerms', '-'),
            'Unit price': line_item.get('unitPrice', '-'),
            'Total Invoice Amount': po_header.get('totalAmountWithTax', '-'),
            'Tax Details (SST/VAT/GST)': po_header.get('taxType', '-'),
            'Tax Amount': po_header.get('totalTaxAmount', '-'),
            'Item Code': line_item.get('itemCode', '-'),
            'Description': line_item.get('description', '-'),
            'Ordered Quantity': line_item.get('quantity', '-'),
            'UOM': line_item.get('itemUom', '-'),
            'Approval Status': po_header.get('approvalstatus', '-')
        }
        csv_rows.append(row)
    
    # Write data to CSV file
    if csv_rows:
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in csv_rows:
                writer.writerow(row)
        
        logger.info(f"CSV file created: {csv_path}")
        return {'csv_file': csv_path}
    else:
        logger.warning("No data to write to CSV")
        return {}
    
@tracer.capture_method
def uploadToS3(file_path, merchant_id, record_type):
    """Upload a file to S3 bucket with appropriate prefix"""
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return None
        
    # Extract filename from path
    filename = os.path.basename(file_path)
    
    # Create prefix structure: /merchant_id/document_type/input/
    today = datetime.now()
    
    # S3 key follows pattern that will be compatible with the other lambda function
    s3_key = f"{record_type}/{merchant_id}/input/{filename}"
    
    try:
        # Upload file to S3
        S3_CLIENT.upload_file(file_path, S3_BUCKET, s3_key)
        logger.info(f"Successfully uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")
        
        # Return the S3 URI and metadata
        return {
            's3_uri': f"s3://{S3_BUCKET}/{s3_key}",
            's3_bucket': S3_BUCKET,
            's3_key': s3_key,
            'upload_date': today.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
    except Exception as e:
        logger.error(f"Error uploading file to S3: {str(e)}")
        return None

@tracer.capture_method
def storeRecordsInDynamoDB(record_type, headers, line_items):
    """Store the mapped data into DynamoDB tables"""
    header_table_name, line_items_table_name = getTableNames(record_type)
    if not header_table_name or not line_items_table_name:
        return False

    EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(header_table_name)
    EXTRACTED_DOCUMENT_LINE_ITEM_TABLE = DDB_RESOURCE.Table(line_items_table_name)

    record_ddb_table_info = {
        "purchase-order": {
            "indexName": "gsi-merchantId-poNumber",
            "keyFields": ["merchantId", "poNumber"],
            "idField": "extractedPoId",
            "lineItemIdField": "extractedPoLineItemsId",
        },
        "item-receipt": {
            "indexName": "gsi-merchantId-grnNumber",
            "keyFields": ["merchantId", "grnNumber"],
            "idField": "extractedGrnId",
            "lineItemIdField": "extractedGrnLineItemsId"
        }
    }

    config = record_ddb_table_info.get(record_type)
    if not config:
        return False

    unique_headers = []
    existing_header_ids = set()
    id_mapping = {}  # Map from new UUID to existing UUID in the database
    index_name = config["indexName"]
    key_fields = config["keyFields"]
    id_field = config["idField"]

    for header_item in headers:
        # Check if all key fields exist in the header
        if all(header_item.get(field) for field in key_fields):
            # Build query expression and values dynamically
            expression_values = {f':{field[0]}': header_item.get(field) for field in key_fields}
            condition_parts = [f"{field} = :{field[0]}" for field in key_fields]
            key_condition = " AND ".join(condition_parts)
            
            items = EXTRACTED_DOCUMENT_DDB_TABLE.query(
                IndexName=index_name,
                KeyConditionExpression=key_condition,
                ExpressionAttributeValues=expression_values
            ).get('Items')

            if items:
                # Get the ID from the current run
                new_id = header_item.get(id_field)
                # Get the ID from the database (first item found)
                existing_id = items[0].get(id_field)
                if new_id and existing_id:
                    id_mapping[new_id] = existing_id
                    existing_header_ids.add(existing_id)
                continue

        unique_headers.append(header_item)

    # Update line items to use the existing header IDs from the database
    for item in line_items:
        header_id = item.get(id_field)
        if header_id in id_mapping:
            # Replace the header ID with the one from the database
            item[id_field] = id_mapping[header_id]
   
    unique_line_items = [item for item in line_items if item.get(id_field) not in existing_header_ids]

    # Log counts for debugging
    logger.info(f"Processing {len(unique_headers)} unique headers and {len(unique_line_items)} line items")
    logger.info(f"Mapped {len(id_mapping)} header IDs from new to existing")

    with EXTRACTED_DOCUMENT_DDB_TABLE.batch_writer() as batch:
        for header_item in unique_headers:
            batch.put_item(Item=header_item)

    with EXTRACTED_DOCUMENT_LINE_ITEM_TABLE.batch_writer() as batch:
        for item in unique_line_items:
            batch.put_item(Item=item)

    return True

@tracer.capture_method
def getTableNames(record_type):
    """Return corresponding table names for record type"""
    table_map = {
        "purchase-order": (EXTRACTED_PO_TABLE, EXTRACTED_PO_LINE_ITEMS_TABLE),
        "item-receipt": (EXTRACTED_GRN_TABLE, EXTRACTED_GRN_LINE_ITEMS_TABLE)
    }

    selected_table = table_map.get(record_type)

    if not selected_table:
        return (None, None)
    
    return selected_table