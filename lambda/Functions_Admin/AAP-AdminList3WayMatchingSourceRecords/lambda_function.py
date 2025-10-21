import json
import boto3
import os
import csv
import io
from urllib.parse import unquote_plus
import logging
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import is_authenticated, get_user, get_user_group, has_permission, Permission


# Configure logging
logger = Logger()
tracer = Tracer()

s3_client = boto3.client('s3')
SMART_EYE_BUCKET = os.environ.get('SMART_EYE_BUCKET')
MAX_LINE_ITEMS = int(os.environ.get('MAX_LINE_ITEMS', '100')) 
USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """List CSV files in the S3 bucket with metadata and parse contents for item-level details"""
    try:
        # Authenticate user and get merchant ID
        sub, _, _ = is_authenticated(event)
        current_user = get_user(sub)
        current_user_name = current_user.get('name')
        merchant_id = current_user.get('merchantId')
        current_user_group_name = get_user_group(current_user.get('userGroupId')).get('userGroupName')
        has_permission(current_user_group_name, Permission.GET_ALL_DOCUMENTS.value)

        # Log the incoming event for debugging
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Extract query parameters if provided
        query_params = {}
        if 'queryStringParameters' in event and event['queryStringParameters']:
            query_params = event['queryStringParameters']
        
        document_type = query_params.get('documentType', '')
        show_contents = query_params.get('showContents', 'true').lower() == 'true'
        
        # Use retrieveCSV function similar to AAP-ThreeWayMatching to get the correct file
        if document_type:
            if document_type == "grn":
                file_key = retrieveCSV(merchant_id, 'grn')
            else:
                file_key = retrieveCSV(merchant_id, 'po')
        else:
            file_key = None
            
        logger.info(f"Query parameters: documentType={document_type}, merchantId={merchant_id}, fileKey={file_key}, showContents={show_contents}")
        
        # If specific file is requested, just get that file's contents
        if file_key:
            # Verify the file exists
            try:
                s3_client.head_object(Bucket=SMART_EYE_BUCKET, Key=file_key)
                logger.info(f"File exists: {file_key}")
            except Exception as e:
                logger.error(f"File not found: {file_key}. Error: {str(e)}")
                return format_response({
                    'error': f"The requested file for {document_type} was not found for merchant {merchant_id}",
                    'bucket': SMART_EYE_BUCKET
                })
            
            file_type = determine_file_type(file_key)
            file_contents = parse_csv_file(SMART_EYE_BUCKET, file_key, file_type) if show_contents else []
            
            return format_response({
                'file': {
                    'key': file_key,
                    'name': file_key.split('/')[-1],
                    'type': file_type,
                    'contents': file_contents
                }
            })
        
        # If no specific document type requested, search in both PO and GRN directories
        if not file_key:  # When no specific file was requested
            files = []
            
            # Check PO files
            po_prefix = f"purchase-order/{merchant_id}/input/"
            po_response = s3_client.list_objects_v2(
                Bucket=SMART_EYE_BUCKET,
                Prefix=po_prefix
            )
            
            # Check GRN files
            grn_prefix = f"grn-csv/{merchant_id}/input/"
            grn_response = s3_client.list_objects_v2(
                Bucket=SMART_EYE_BUCKET,
                Prefix=grn_prefix
            )
            
            # Process both sets of files
            all_s3_objects = []
            if 'Contents' in po_response:
                all_s3_objects.extend(po_response['Contents'])
            if 'Contents' in grn_response:
                all_s3_objects.extend(grn_response['Contents'])
                
            # Format the response
            files = []
            for item in all_s3_objects:
                # Only include CSV files
                key = item['Key']
                if key.lower().endswith('.csv'):
                    try:
                        # Get file metadata
                        metadata_response = s3_client.head_object(
                            Bucket=SMART_EYE_BUCKET,
                            Key=key
                        )
                        
                        # Determine file type (PO or GRN)
                        file_type = determine_file_type(key)
                        
                        # Create file object with metadata
                        file_obj = {
                            'key': key,
                            'name': key.split('/')[-1],
                            'type': file_type,
                            'size': item['Size'],
                            'lastModified': item['LastModified'].isoformat(),
                            'contentType': metadata_response.get('ContentType', 'text/csv')
                        }
                        
                        # If contents are requested, parse the CSV file
                        if show_contents:
                            file_obj['contents'] = parse_csv_file(SMART_EYE_BUCKET, key, file_type)
                        
                        files.append(file_obj)
                        logger.info(f"Added file to response: {key}")
                    except Exception as e:
                        logger.error(f"Error processing file {key}: {str(e)}")
                        # Include the file with minimal information and error
                        files.append({
                            'key': key,
                            'name': key.split('/')[-1],
                            'size': item['Size'],
                            'lastModified': item['LastModified'].isoformat(),
                            'error': str(e)
                        })
        
        logger.info(f"Returning response with {len(files)} files")
        return format_response({
            'files': files,
            'count': len(files),
            'bucket': SMART_EYE_BUCKET,
            'prefix': merchant_id
        })
    except Exception as e:
        logger.error(f"Error listing S3 objects: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': get_cors_headers(),
            'body': json.dumps({
                'message': f'Error listing S3 objects: {str(e)}'
            })
        }

@tracer.capture_method
def retrieveCSV(merchantId, fileType):
    """Get the most recent CSV file of specified type for a merchant"""
    try:
        s3Contents = []
        
        if fileType == "grn":
            prefix = f"grn-csv/{merchantId}/input/"
        elif fileType == "po":
            prefix = f"purchase-order/{merchantId}/input/"
        else:
            return None

        logger.info(f"Searching for {fileType} CSV in {prefix}")
        response = s3_client.list_objects_v2(
            Bucket=SMART_EYE_BUCKET,
            Prefix=prefix
        )
        s3Contents.extend(response.get('Contents', []))

        while response.get('IsTruncated', False):
            response = s3_client.list_objects_v2(
                Bucket=SMART_EYE_BUCKET,
                Prefix=prefix,
                ContinuationToken=response['NextContinuationToken']
            )
            s3Contents.extend(response.get('Contents', []))

        # Filter for CSV files
        csvFiles = [obj for obj in s3Contents if obj['Key'].endswith('.csv')]
        if not csvFiles:
            logger.warning(f"No {fileType} CSV files found for merchant {merchantId}")
            return None
            
        # Sort by last modified date
        csvFiles.sort(key=lambda x: x['LastModified'], reverse=True)
        
        # Return the most recent CSV file
        most_recent_file = csvFiles[0]['Key']
        logger.info(f"Found most recent {fileType} file: {most_recent_file}")
        return most_recent_file
    
    except Exception as ex:
        logger.exception(f"Error retrieving {fileType} CSV: {str(ex)}")
        return None

@tracer.capture_method
def determine_file_type(key):
    """Determine file type based on path structure and filename"""
    # First, check the path structure for more reliable determination
    if '/purchase-order/' in key:
        return 'PO'
    elif '/grn-csv/' in key:
        return 'GRN'
    elif '/export/' in key:
        return 'EXPORT'
    
    # Fallback to keyword detection
    key_lower = key.lower()
    if 'po' in key_lower or 'purchase' in key_lower:
        return 'PO'
    elif 'grn' in key_lower or 'receipt' in key_lower or 'goods' in key_lower:
        return 'GRN'
    elif 'export' in key_lower:
        return 'EXPORT'
    else:
        return 'Unknown'

@tracer.capture_method
def parse_csv_file(bucket, key, file_type):
    """Parse a CSV file and extract line items based on file type"""
    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Read the CSV content
        csv_content = response['Body'].read().decode('utf-8')
        logger.info(f"CSV content read, size: {len(csv_content)} bytes")
        
        # Parse CSV
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        
        # Log headers for debugging
        headers = csv_reader.fieldnames
        logger.info(f"CSV headers: {headers}")
        
        # Convert to list of dictionaries and apply field mappings
        line_items = []
        line_item_count = 0
        
        for row in csv_reader:
            if line_item_count >= MAX_LINE_ITEMS:
                break  # Prevent processing too many line items
                
            # Map fields based on file type
            if file_type == 'PO':
                item = map_po_fields(row)
            elif file_type == 'GRN':
                item = map_grn_fields(row)
            else:
                # For unknown file types, just use the raw row data
                item = {k: v for k, v in row.items()}
            
            # Add line number for reference
            item['line_number'] = line_item_count + 1
            
            line_items.append(item)
            line_item_count += 1
            
        if line_item_count >= MAX_LINE_ITEMS:
            line_items.append({
                'warning': f'Only showing first {MAX_LINE_ITEMS} items. File contains more rows.'
            })
            
        logger.info(f"Parsed {line_item_count} line items from CSV")
        return line_items
        
    except Exception as e:
        logger.error(f"Error parsing CSV file {key}: {str(e)}")
        import traceback
        traceback.print_exc()
        return [{'error': f'Failed to parse file: {str(e)}'}]

def map_po_fields(row):
    """Map raw CSV fields to standardized PO fields"""
    mapped_item = {}
    
    # Define field mappings - original field name to standardized name
    field_mappings = {
        'PO Number': 'poNumber',
        'Purchase Order Number': 'poNumber',
        'PO Date': 'poDate',
        'Date': 'poDate',
        'Supplier Name': 'supplierName',
        'Vendor': 'supplierName',
        'Supplier ID': 'supplierCode',  
        'Vendor ID': 'supplierCode',
        'Currency': 'currency',
        'Payment Term': 'paymentTerm',
        'Payment Terms': 'paymentTerm',
        'Item Code': 'itemCode',
        'Product Code': 'itemCode', 
        'Item Number': 'itemCode',
        'Description': 'description',
        'Item Description': 'description',
        'Product Description': 'description',
        'Quantity': 'quantity',
        'Ordered Quantity': ['quantity', 'orderedQuantity'],
        'Order Qty': 'quantity',
        'UOM': 'uom',
        'Unit': 'uom',
        'Unit of Measure': 'uom',
        'Unit price': 'unitPrice',
        ' Unit price': 'unitPrice',
        'Price': 'unitPrice',
        'Rate': 'unitPrice',
        'Total': 'totalAmount',
        'Amount': 'totalAmount',
        'Total Invoice Amount': 'totalInvoiceAmount',
        'Line Total': 'totalAmount',
        'Tax Details (SST/VAT/GST)': 'taxDetails',
        ' Tax Details (SST/VAT/GST)': 'taxDetails',
        'Tax Amount': 'taxAmount',
        ' Tax Amount': 'taxAmount',
        'Approval Status': 'approvalStatus'
    }
    
    # Map fields with case-insensitive matching
    for original_field, normalized_field in field_mappings.items():
        # Find matching field in row (case-insensitive)
        matching_key = next((k for k in row.keys() if k.strip().lower() == original_field.lower()), None)
        if matching_key:
            value = row[matching_key].strip() if isinstance(row[matching_key], str) else row[matching_key]
            # Handle multi-target fields
            if isinstance(normalized_field, list):
                for target in normalized_field:
                    mapped_item[target] = value
            else:
                mapped_item[normalized_field] = value
    
    # Convert numeric fields
    for field in ['quantity', 'unitPrice', 'totalAmount', 'taxAmount', 'totalInvoiceAmount']:
        if field in mapped_item and mapped_item[field]:
            try:
                if isinstance(mapped_item[field], str):
                    # Clean and convert numeric strings
                    clean_value = mapped_item[field].replace(',', '').replace('$', '').replace('£', '').replace('€', '').strip()
                    if clean_value:
                        mapped_item[field] = float(clean_value)
            except (ValueError, TypeError):
                pass
    
    # Calculate totalInvoiceAmount using simplified priority logic
    if not mapped_item.get('totalInvoiceAmount'):
        if mapped_item.get('totalAmount'):
            # Use totalAmount if available
            mapped_item['totalInvoiceAmount'] = mapped_item['totalAmount']
        elif mapped_item.get('unitPrice') and (mapped_item.get('quantity') or mapped_item.get('orderedQuantity')):
            # Calculate from unit price and quantity
            try:
                unit_price = float(mapped_item['unitPrice']) if not isinstance(mapped_item['unitPrice'], float) else mapped_item['unitPrice']
                quantity = mapped_item.get('quantity', mapped_item.get('orderedQuantity', 0))
                if not isinstance(quantity, (int, float)):
                    quantity = float(quantity) if quantity else 0
                mapped_item['totalInvoiceAmount'] = unit_price * quantity
                mapped_item['totalAmount'] = mapped_item['totalInvoiceAmount']  # For consistency
            except Exception as e:
                logger.warning(f"Could not calculate totalInvoiceAmount: {e}")
                mapped_item['totalInvoiceAmount'] = 0
        else:
            mapped_item['totalInvoiceAmount'] = 0
            
    return mapped_item

def map_grn_fields(row):
    """Map raw CSV fields to standardized GRN fields"""
    mapped_item = {}
    
    # Define field mappings - original field name to standardized name
    field_mappings = {
        'GRN Number': 'grnNumber',
        'Goods Receipt Number': 'grnNumber',
        'Receipt Number': 'grnNumber',
        'GRN Date': 'grnDate',
        'Receipt Date': 'grnDate',
        'Date': 'grnDate',
        'PO Number': 'purchaseOrderNo', 
        'PO Number ': 'purchaseOrderNo',
        'Purchase Order': 'purchaseOrderNo',
        'PO Reference': 'purchaseOrderNo',
        'Supplier Name': 'supplierName',
        'Vendor': 'supplierName',
        'Supplier ID': 'supplierCode',  
        'Vendor ID': 'supplierCode',
        'Item Code': 'itemCode',
        'Product Code': 'itemCode',
        'Item Number': 'itemCode',
        'Description': 'description',
        'Item Description': 'description',
        'Product Description': 'description',
        'Quantity': 'quantity', 
        'Received Quantity': 'quantity',
        'Receipt Qty': 'quantity',
        'UOM': 'itemUom',  
        'Unit': 'itemUom',
        'Unit of Measure': 'itemUom',
        'Total': 'totalAmount',
        'Amount': 'totalAmount',
        'Total Amount': 'totalAmount'
    }
    
    # Map fields, stripping whitespace and handling case insensitivity
    for original_field, normalized_field in field_mappings.items():
        # Try exact match first
        if original_field in row:
            mapped_item[normalized_field] = row[original_field].strip() if isinstance(row[original_field], str) else row[original_field]
        else:
            # Try case-insensitive and whitespace normalized match
            for field in row.keys():
                if field.strip().lower() == original_field.lower():
                    mapped_item[normalized_field] = row[field].strip() if isinstance(row[field], str) else row[field]
                    break
    
    # Convert numeric fields
    numeric_fields = ['quantity', 'totalAmount']
    for field in numeric_fields:
        if field in mapped_item and mapped_item[field]:
            try:
                # Remove commas, spaces, currency symbols
                if isinstance(mapped_item[field], str):
                    clean_value = mapped_item[field].replace(',', '').replace('$', '').replace('£', '').replace('€', '').strip()
                    if clean_value:
                        mapped_item[field] = float(clean_value)
            except (ValueError, TypeError):
                # Keep as string if conversion fails
                pass
    
    # Add totalInvoiceAmount field for consistency with PO items
    if 'totalAmount' in mapped_item and mapped_item['totalAmount']:
        mapped_item['totalInvoiceAmount'] = mapped_item['totalAmount']
    else:
        mapped_item['totalInvoiceAmount'] = 0
        
    return mapped_item

def get_cors_headers():
    """Return standard CORS headers"""
    return {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'
    }

def format_response(body_content):
    """Format API Gateway response with CORS headers"""
    return {
        'statusCode': 200,
        'headers': get_cors_headers(),
        'body': json.dumps(body_content, default=str)
    }
