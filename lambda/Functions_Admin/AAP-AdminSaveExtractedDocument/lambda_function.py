import os
import boto3
import uuid
import json
from aws_lambda_powertools import Logger, Tracer
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError
from authorizationHelper import is_authenticated, has_permission, Permission, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException
from boto3.dynamodb.conditions import Key

EXTRACTED_DOCUMENTS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE')
EXTRACTED_PO_TABLE = os.environ.get('EXTRACTED_PO_TABLE')
EXTRACTED_PO_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_PO_LINE_ITEM_TABLE')
EXTRACTED_GRN_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_GRN_LINE_ITEM_TABLE')
EXTRACTED_GRN_TABLE = os.environ.get('EXTRACTED_GRN_TABLE')
EXTRACTED_REFERRAL_LETTER_TABLE = os.environ.get('EXTRACTED_REFERRAL_LETTER_TABLE')

TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
EXTRACTED_DOCUMENTS_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE)
EXTRACTED_PO_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_PO_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_LINE_ITEM_TABLE)
EXTRACTED_GRN_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_TABLE)
EXTRACTED_GRN_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
EXTRACTED_REFERRAL_LETTER_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_REFERRAL_LETTER_TABLE)

EXCLUDED_FIELDS = [
    'createdAt', 
    'createdBy', 
    'merchantId', 
    'extractedReferralLetterId',
    'extractedDocumentsLineItemsId', 
    'extractedDocumentsId',
    'extractedPoLineItemsId',
    'extractedPoId',
    'extractedGrnLineItemsId',
    'extractedGrnId',
    'documentUploadId',
    'sourceFile',
    'boundingBoxes',
    'documentStatus',
    'confidenceScore'
]
NUMBER_FIELDS = [
    'taxAmount',
    'taxRate',
    'totalInvoiceAmount',
    'totalTaxAmount',
    'totalAmountWithTax',
    'totalAmountWithoutTax',
    'quantity',
    'totalPrice',
]

DOCUMENT_TYPE_MAPPING = {
        'invoice': {
            'document_table': EXTRACTED_DOCUMENTS_DDB_TABLE,
            'line_item_table': EXTRACTED_DOCUMENTS_LINE_ITEM_DDB_TABLE,
            'doc_key': 'extractedDocumentsId',
            'line_item_key': 'extractedDocumentsLineItemsId',
            'gsi': 'gsi-extractedDocumentsId',
            'doc_number': 'invoiceNumber'},
            
        'po': {
            'document_table': EXTRACTED_PO_DDB_TABLE, 
            'line_item_table': EXTRACTED_PO_LINE_ITEM_DDB_TABLE,
            'doc_key': 'extractedPoId',
            'line_item_key': 'extractedPoLineItemsId',
            'gsi': 'gsi-extractedPoId',
            'doc_number': 'poNumber'},
        'grn': {
            'document_table': EXTRACTED_GRN_DDB_TABLE, 
            'line_item_table': EXTRACTED_GRN_LINE_ITEM_DDB_TABLE,
            'doc_key': 'extractedGrnId',
            'line_item_key': 'extractedGrnLineItemsId',
            'gsi': 'gsi-extractedGrnId',
            'doc_number': 'grnNumber'},
        'medicalReferralLetter': {
            'document_table': EXTRACTED_REFERRAL_LETTER_DDB_TABLE,
            'line_item_table': None,  # No line items for this type
            'doc_key': 'extractedReferralLetterId',
            'line_item_key': None,  # No line items
            'gsi': None,
            'doc_number': None}
    }

logger = Logger()
tracer = Tracer()

def float_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    return obj

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, email, _ = is_authenticated(event)
        user = get_user(sub)
        merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.UPDATE_DOCUMENT.value)
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        request_body = json.loads(event.get('body', '{}'))
        logger.info(f"Request body: {request_body}")

        if 'data' in request_body:
            data = request_body.get('data', {})
            form_data = data.get('formData', {})
            table_data = data.get('tableData', [])
        else:
            form_data = request_body.get('formData', {})
            table_data = request_body.get('tableData', [])

        form_data = float_to_decimal(form_data)
        table_data = float_to_decimal(table_data)
        logger.info(f"Table data: {table_data}")
        
        # Get document ID from form_data
        if 'extractedDocumentsId' in form_data:
            extracted_document_id = form_data.get('extractedDocumentsId')
            document_type = 'invoice'
        elif 'extractedPoId' in form_data:
            extracted_document_id = form_data.get('extractedPoId')
            document_type = 'po'
        elif 'extractedGrnId' in form_data:
            extracted_document_id = form_data.get('extractedGrnId')
            document_type = 'grn'
        elif 'extractedReferralLetterId' in form_data:
            extracted_document_id = form_data.get('extractedReferralLetterId')
            document_type = 'medicalReferralLetter'
        else:
            raise BadRequestException("Document ID is required")

        # Fetch the existing document
        existing_document = get_extracted_document(extracted_document_id, document_type)
        document_merchant_id = existing_document.get('merchantId')
        # document_type = existing_document.get('documentType')

        if document_merchant_id != merchant_id:
            raise AuthorizationException("Not authorized to update this document")
        
        supplier_updated = False
        if ('supplierName' in form_data and form_data.get('supplierName') != existing_document.get('supplierName')) or \
           ('supplierCode' in form_data and form_data.get('supplierCode') != existing_document.get('supplierCode')):
            supplier_updated = True

        buyer_updated = False
        if ('buyerName' in form_data and form_data.get('buyerName') != existing_document.get('buyerName')) or \
           ('buyerCode' in form_data and form_data.get('buyerCode') != existing_document.get('buyerCode')):
            buyer_updated = True
   
        # Update the document
        update_document = update_extracted_document(form_data, user.get('email'), merchant_id, now, document_type)

        if document_type != 'medicalReferralLetter':
            # If supplier or buyer info was updated, propagate to line items
            if supplier_updated or buyer_updated:
                update_supplier_info_in_line_items(
                    extracted_document_id,
                    document_type,
                    update_document.get('supplierName', '-'), 
                    update_document.get('supplierCode', '-'),
                    existing_document.get('buyerName', '-'),
                    existing_document.get('buyerCode', '-'),
                    user.get('email'),
                    now
                )
            # Process table data
            process_status = process_table_data(table_data, user.get('email'), update_document, merchant_id, now)
            # Create timeline record
            create_timeline_record(existing_document, user.get('email'), now)
            logger.info("Timeline record created")

        return create_response(200, "Document Successfully Updated")

    except (AuthenticationException, AuthorizationException, BadRequestException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(400, e.message)

    except Exception as e:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(e))
        logger.exception({"message": str(e)})
        return create_response(
            500,
            "The server encountered an unexpected condition that prevented it from fulfilling your request."
        )

@tracer.capture_method
def create_update_expression(item_data, user, now, excluded_fields=None):
    if excluded_fields is None:
        excluded_fields = EXCLUDED_FIELDS
        
    update_expression_parts = []
    expression_attribute_values = {}
    expression_attribute_names = {}
    
    for key, value in item_data.items():
        if key in excluded_fields:
            continue
        if key in NUMBER_FIELDS:
            if value != "":
                value = Decimal(str(value))
            else:
                value = Decimal('0')
            
        attr_name = f'#{key}'
        attr_value = f':{key}'
        update_expression_parts.append(f'{attr_name} = {attr_value}')
        expression_attribute_names[attr_name] = key
        expression_attribute_values[attr_value] = value
    
    if not expression_attribute_names.get('#updatedAt'):
        expression_attribute_names['#updatedAt'] = 'updatedAt'
        update_expression_parts.append('#updatedAt = :updatedAt')

    if not expression_attribute_names.get('#updatedBy'):
        expression_attribute_names['#updatedBy'] = 'updatedBy'
        update_expression_parts.append('#updatedBy = :updatedBy')

    expression_attribute_values[':updatedAt'] = now
    expression_attribute_values[':updatedBy'] = user
    
    # Create the final update expression
    update_expression = 'SET ' + ', '.join(update_expression_parts)
    
    return update_expression, expression_attribute_names, expression_attribute_values

@tracer.capture_method
def get_extracted_document(extracted_document_id, document_type):
    document_type_mapped = DOCUMENT_TYPE_MAPPING[document_type]
    table = document_type_mapped['document_table']
    primary_key = document_type_mapped['doc_key']

    response = table.get_item(Key={primary_key: extracted_document_id})
    item = response.get('Item')  

    if not item:
        raise BadRequestException("Document not found")

    return item

@tracer.capture_method
def query_extracted_documents_line_items(extracted_document_id, document_type):

    document_type_mapped = DOCUMENT_TYPE_MAPPING[document_type]
    table = document_type_mapped.get('line_item_table')
    gsi = document_type_mapped.get('gsi')
    secondary_key = document_type_mapped.get('doc_key')
    
    response = table.query(
    IndexName=gsi,
    KeyConditionExpression=Key(secondary_key).eq(extracted_document_id)) 
    return response.get('Items', [])
        
@tracer.capture_method
def update_extracted_document(form_data, user, merchant_id, now, document_type):
    logger.info(f"Updating document of type {document_type}")
    logger.info(f'Document type mapping : {DOCUMENT_TYPE_MAPPING[document_type]}')
    document_type_mapped = DOCUMENT_TYPE_MAPPING[document_type]
    table = document_type_mapped['document_table']
    primary_key = document_type_mapped.get('doc_key')
    if extracted_document_id := form_data.get(primary_key):
            update_expression, expr_attr_names, expr_attr_values = create_update_expression(form_data, user, now)
            try:

                response = table.update_item(
                                Key={primary_key: extracted_document_id},
                                UpdateExpression=update_expression,
                                ExpressionAttributeNames=expr_attr_names,
                                ExpressionAttributeValues=expr_attr_values,
                                ConditionExpression=f'attribute_exists({primary_key})',
                                ReturnValues='ALL_NEW'
                                )
                return response.get('Attributes', {})
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    logger.error(f"Document {extracted_document_id} does not exist")
                    raise BadRequestException('Document not found. It may have been deleted by another user.')

    raise BadRequestException('No valid document ID found in form data.')

@tracer.capture_method
def update_supplier_info_in_line_items(extracted_document_id, document_type, supplier_name, supplier_code, buyer_name, buyer_code, user, now):
    """
    Update supplier name and code in all line items associated with a document
    """
    # Get all line items for this document
    line_items = query_extracted_documents_line_items(extracted_document_id, document_type)
    
    update_count = 0

    for item in line_items:
        line_item_id = (item.get('extractedDocumentsLineItemsId') 
                        or item.get('extractedPoLineItemsId')
                        or item.get('extractedGrnLineItemsId'))
        
        # Prepare update data containing only supplier information
        update_data = {
            'supplierName': supplier_name,
            'supplierCode': supplier_code,
            'buyerName': buyer_name,
            'buyerCode': buyer_code,
        }
        
        # Update the line item
        update_line_item_record(line_item_id, update_data, user, now, document_type)
        update_count += 1
    
    logger.info(f"Updated supplier/buyer info in {update_count} line items for document {extracted_document_id}")
    return update_count

@tracer.capture_method
def process_table_data(table_data, user, existing_document, merchant_id, now, excluded_fields=None):
    # Determine document type and IDs
    document_type = existing_document.get('documentType')
    if document_type == 'invoice':
        extracted_document_id = existing_document.get('extractedDocumentsId')
        line_item_id_field = 'extractedDocumentsLineItemsId'
    elif document_type == 'po':
        extracted_document_id = existing_document.get('extractedPoId')
        line_item_id_field = 'extractedPoLineItemsId'
    elif document_type == 'grn':
        extracted_document_id = existing_document.get('extractedGrnId')
        line_item_id_field = 'extractedGrnLineItemsId'
    
    document_upload_id = existing_document.get('documentUploadId')
    # Validate table data

    if excluded_fields is None:
        excluded_fields = EXCLUDED_FIELDS

    # Get existing line items for this document
    existing_line_items = query_extracted_documents_line_items(extracted_document_id, document_type)
    logger.info(f"Existing line items: {existing_line_items}")
    existing_line_item_map = {item.get(line_item_id_field): item for item in existing_line_items}
    logger.info(f"Existing line items: {existing_line_item_map}")
    logger.info(f"Table data: {table_data}")

    # Track IDs for operations tracking
    updated_ids = []
    created_ids = []
    deleted_ids = []

    supplier_name = existing_document.get('supplierName', "-")
    supplier_code = existing_document.get('supplierCode', "-")
    buyer_name = existing_document.get('buyerName', "-")
    buyer_code = existing_document.get('buyerCode', "-")
    currency = existing_document.get('currency', "-")
    
    # Process incoming line items (update existing, create new)
    processed_ids = set()
    for item in table_data:
        item = float_to_decimal(item)
        line_item_id = item.get(line_item_id_field)

        item['supplierName'] = supplier_name
        item['supplierCode'] = supplier_code
        item['buyerName'] = buyer_name
        item['buyerCode'] = buyer_code
        item['currency'] = currency

        if line_item_id and line_item_id in existing_line_item_map:
            # Update existing line item
            processed_ids.add(line_item_id)
            update_line_item_record(line_item_id, item, user, now, document_type, excluded_fields)
            updated_ids.append(line_item_id)
        else:
            # Create new line item
            new_id = create_line_item(item, extracted_document_id, existing_document, document_upload_id, merchant_id, user, now)
            processed_ids.add(new_id)
            created_ids.append(new_id)
    
    # Find line items to delete (those in existing but not in processed)
    for line_item_id in existing_line_item_map.keys():
        if line_item_id not in processed_ids:
            delete_line_item(line_item_id,document_type)
            deleted_ids.append(line_item_id)
            
    logger.info(f"Updated {len(updated_ids)} line items, created {len(created_ids)}, deleted {len(deleted_ids)}")
    return True

@tracer.capture_method
def create_line_item(line_item_data, extracted_document_id, existing_document, document_upload_id, merchant_id, user, now):
    line_item_id = str(uuid.uuid4())
    base_item = {
        'documentUploadId': document_upload_id,
        'merchantId': merchant_id,
        'createdAt': now,
        'createdBy': user,
        'updatedAt': now,
        'updatedBy': user,
        'boundingBoxes': {},
        'status': 'Success',
        'exceptionStatus': 'Manual Creation',
    }

    document_type = existing_document.get('documentType')
    document_type_mapped = DOCUMENT_TYPE_MAPPING[document_type]
    table = document_type_mapped['line_item_table']
    doc_key = document_type_mapped.get('doc_key')
    doc_number = document_type_mapped.get('doc_number')
    line_item_key = document_type_mapped.get('line_item_key')

    item = {
        line_item_key: line_item_id,
        doc_key: extracted_document_id,
        doc_number: existing_document.get(doc_number),
        **base_item
    }

    # Exclude these keys from direct copying
    exclude_keys = {line_item_key, 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'merchantId', doc_number}

    # Copy over valid keys, converting floats to Decimal
    for key, value in line_item_data.items():
        if key not in exclude_keys:
            if isinstance(value, float):  
                item[key] = Decimal(str(value))
            else:
                item[key] = value

    # Automatically compute taxAmount if not present
    if 'taxAmount' not in item:
        try:
            quantity = Decimal(str(line_item_data.get('quantity', '0')))
            unit_price = Decimal(str(line_item_data.get('unitPrice', '0')))
            total_price = Decimal(str(line_item_data.get('totalPrice', '0')))

            subtotal = quantity * unit_price
            tax_amount = total_price - subtotal
            item['taxAmount'] = tax_amount.quantize(Decimal('0.01'))  
        except (TypeError, ValueError):
            item['taxAmount'] = None  

    table.put_item(Item=item)
    return line_item_id


@tracer.capture_method
def delete_line_item(line_item_id, document_type):
    document_type_mapped = DOCUMENT_TYPE_MAPPING[document_type]
    table = document_type_mapped.get('line_item_table')
    secondary_key = document_type_mapped.get('line_item_key')

    try:
        table.delete_item(
            Key={secondary_key: line_item_id},
            ConditionExpression=f'attribute_exists({secondary_key})'
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise BadRequestException("Line item not found")
        else:
            raise  # Re-raise any other unexpected errors


@tracer.capture_method
def update_line_item_record(line_item_id, line_item_data, user, now, document_type, excluded_fields=None):
    if excluded_fields is None:
        excluded_fields = EXCLUDED_FIELDS

    document_type_mapped = DOCUMENT_TYPE_MAPPING.get(document_type)
    if not document_type_mapped:
        raise BadRequestException(f"Unsupported document type: {document_type}")

    table = document_type_mapped['line_item_table']
    line_item_key = document_type_mapped['line_item_key']

    # Fetch existing item from DynamoDB
    try:
        response = table.get_item(Key={line_item_key: line_item_id})
        existing_item = response.get('Item')
        if not existing_item:
            raise BadRequestException("Line item not found")
    except ClientError as e:
        logger.error(f"Failed to fetch existing line item: {e}")
        raise

    # Check if totalPrice or unitPrice changed => recalculate taxAmount
    if 'totalPrice' in line_item_data or 'unitPrice' in line_item_data or 'quantity' in line_item_data:
        try:
            quantity = Decimal(str(
                line_item_data.get('quantity') or existing_item.get('quantity') or '0'
            ))
            unit_price = Decimal(str(
                line_item_data.get('unitPrice') or existing_item.get('unitPrice') or '0'
            ))
            total_price = Decimal(str(
                line_item_data.get('totalPrice') or existing_item.get('totalPrice') or '0'
            ))

            subtotal = quantity * unit_price
            tax_amount = total_price - subtotal
            line_item_data['taxAmount'] = tax_amount.quantize(Decimal('0.01'))
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to recalculate taxAmount for line item {line_item_id}: {e}")

    # Create update expression
    update_expression, expr_attr_names, expr_attr_values = create_update_expression(line_item_data, user, now)

    if not expr_attr_values:
        logger.info(f"No fields to update for line item {line_item_id}")
        return True

    try:
        table.update_item(
            Key={line_item_key: line_item_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
            ConditionExpression=f'attribute_exists({line_item_key})'
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise BadRequestException("Line item not found")
        else:
            logger.error(f"Unexpected error updating line item {line_item_id}: {e}")
            raise

@tracer.capture_method
def create_timeline_record(extracted_document, user, now):
    timeline_id = str(uuid.uuid4())
    document_type = extracted_document.get('documentType')

    if document_type not in DOCUMENT_TYPE_MAPPING:
        logger.warning(f"Unknown documentType '{document_type}' in document. Skipping timeline creation.")
        return

    mapping = DOCUMENT_TYPE_MAPPING[document_type]
    id_key = mapping['doc_key']

    if id_key not in extracted_document:
        logger.warning(f"{id_key} not found in extracted_document. Skipping timeline creation.")
        return

    extracted_document_id = extracted_document.get(id_key)
    merchant_id = extracted_document.get('merchantId')

    timeline_item = {
        'timelineId': timeline_id,
        'timelineForId': extracted_document_id,
        'merchantId': merchant_id,
        'createdAt': now,
        'createdBy': user,
        'updatedAt': now,
        'updatedBy': user,
        'type': document_type,
        'title': 'Document update',
        'description': f"{document_type.upper()} document updated by {user}",
    }

    # Add type-specific fields
    if document_type == 'invoice':
        timeline_item['invoiceNumber'] = extracted_document.get('invoiceNumber', '-')
        timeline_item['supplierName'] = extracted_document.get('supplierName', '-')
    elif document_type == 'po':
        timeline_item['poNumber'] = extracted_document.get('poNumber', '-')
        timeline_item['buyerName'] = extracted_document.get('buyerName', '-')
    elif document_type == 'grn':
        timeline_item['grnNumber'] = extracted_document.get('grnNumber', '-')
        timeline_item['supplierName'] = extracted_document.get('supplierName', '-')

    # Clean up and save
    timeline_item = {k: v for k, v in timeline_item.items() if v is not None}
    TIMELINE_DDB_TABLE.put_item(Item=timeline_item)
    logger.info(f"Timeline record created for {document_type.upper()} document with ID {extracted_document_id}")

   
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
            'X-Frame-Options':'SAMEORIGIN'
        },
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }