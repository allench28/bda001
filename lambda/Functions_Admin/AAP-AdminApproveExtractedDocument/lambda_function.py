import os
import boto3
import json
import uuid
import time
from decimal import Decimal
import decimal
from aws_lambda_powertools import Logger, Tracer
from datetime import datetime
from botocore.exceptions import ClientError
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException
from boto3.dynamodb.conditions import Key, Attr

EXTRACTED_DOCUMENTS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')
EXTRACTED_PO_TABLE = os.environ.get('EXTRACTED_PO_TABLE')
EXTRACTED_PO_LINE_ITEMS_TABLE = os.environ.get('EXTRACTED_PO_LINE_ITEMS_TABLE')
EXTRACTED_GRN_TABLE = os.environ.get('EXTRACTED_GRN_TABLE')
EXTRACTED_GRN_LINE_ITEMS_TABLE = os.environ.get('EXTRACTED_GRN_LINE_ITEMS_TABLE')
SEQUENCE_NUMBER_GENERATOR_TABLE = os.environ.get('SEQUENCE_NUMBER_GENERATOR_TABLE')
N8N_SQS_QUEUE = os.environ.get('N8N_SQS_QUEUE')

DDB_RESOURCE = boto3.resource('dynamodb')
LAMBDA_CLIENT = boto3.client('lambda')
SQS_CLIENT = boto3.client('sqs')

EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_PO_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_PO_DDB_LINE_ITEMS_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_LINE_ITEMS_TABLE)
EXTRACTED_GRN_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_TABLE)
EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_LINE_ITEMS_TABLE)
SEQUENCE_NUMBER_GENERATOR_DDB_TABLE = DDB_RESOURCE.Table(SEQUENCE_NUMBER_GENERATOR_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, email, _ = is_authenticated(event)
        user = get_user(sub)
        merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.APPROVE_DOCUMENT.value)
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        request_body = json.loads(event.get('body', '{}'))
        print(request_body)
        # create_timeline_record(updated_document_upload, email, now, 'documentUpload')

        if request_body.get('extractedDocumentId'):
            extracted_document_id = request_body.get('extractedDocumentId')
            extracted_document = get_extracted_document(extracted_document_id)
            document_upload_id = extracted_document.get('documentUploadId')

            if extracted_document.get('merchantId') != merchant_id:
                raise AuthorizationException("You are not authorized to approve this document")
            
            updated_extracted_document = update_extracted_document(extracted_document_id, email, now)
            update_extracted_document_line_items(extracted_document_id, email, now)
            create_timeline_record(updated_extracted_document, email, now, 'extractedDocument')
            document_type = 'extracted_document'

            # Generate PO after approval
            generatePo(document_upload_id, merchant_id)

        elif request_body.get('extractedPoId'):
            extracted_document_id = request_body.get('extractedPoId')
            extracted_po = get_extracted_po(extracted_document_id)
            document_upload_id = extracted_po.get('documentUploadId')

            if extracted_po.get('merchantId') != merchant_id:
                raise AuthorizationException("You are not authorized to approve this document")
            
            updated_extracted_po = update_extracted_po(extracted_document_id, email, now)
            update_po_line_items(extracted_document_id, email, now)
            create_timeline_record(updated_extracted_po, email, now, 'extractedPo')
            document_type = 'po'

        elif request_body.get('extractedGrnId'):
            extracted_document_id = request_body.get('extractedGrnId')
            extracted_grn = get_extracted_grn(extracted_document_id)
            document_upload_id = extracted_grn.get('documentUploadId')

            if extracted_grn.get('merchantId') != merchant_id:
                raise AuthorizationException("You are not authorized to approve this document")
            
            updated_extracted_grn = update_extracted_grn(extracted_document_id, email, now)
            update_grn_line_items(extracted_document_id, email, now)
            create_timeline_record(updated_extracted_grn, email, now, 'extractedGrn')
            document_type = 'grn'

        else:
            raise BadRequestException("Issue with determining document type")

        updated_document_upload = update_document_upload_status(document_upload_id, document_type , email, now)
        #send_to_erp_sqs(document_type, extracted_document_id, merchant_id)
        return create_response(200, "Document Approved successfully")
        
    except (AuthenticationException, AuthorizationException, BadRequestException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(e.status_code, e.message)
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return create_response(404, "Document not found")
        else:
            raise

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
#ERP SQS
"""
@tracer.capture_method
def send_to_erp_sqs(document_type, document_id, merchant_id):
    payload = {
        'documentId': document_id,
        'documentType': document_type,
        'merchantId': merchant_id,
    }

    response = SQS_CLIENT.send_message(
        QueueUrl=N8N_SQS_QUEUE,
        MessageBody=json.dumps(payload),
    )
"""   
@tracer.capture_method
def get_extracted_document(extracted_document_id):
    response = EXTRACTED_DOCUMENTS_DDB_TABLE.get_item(
        Key={'extractedDocumentsId': extracted_document_id}
    )
    return response.get('Item')

@tracer.capture_method
def get_extracted_po(extracted_po_id):
    response = EXTRACTED_PO_DDB_TABLE.get_item(
        Key={'extractedPoId': extracted_po_id}
    )
    return response.get('Item')

@tracer.capture_method
def get_extracted_grn(extracted_grn_id):
    response = EXTRACTED_GRN_DDB_TABLE.get_item(
        Key={'extractedGrnId': extracted_grn_id}
    )
    return response.get('Item')
    
@tracer.capture_method
def update_extracted_document(extracted_document_id, user, timestamp):
    response = EXTRACTED_DOCUMENTS_DDB_TABLE.update_item(
        Key={'extractedDocumentsId': extracted_document_id},
        UpdateExpression='SET documentStatus = :documentStatus, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy, approvedAt = :approvedAt, approvedBy = :approvedBy',
        ExpressionAttributeValues={
            ':documentStatus': 'Success',
            ':exceptionStatus': 'N/A',
            ':updatedAt': timestamp,
            ':updatedBy': user,
            ':approvedAt': timestamp,
            ':approvedBy': user
        },
        ConditionExpression="attribute_exists(extractedDocumentsId)",
        ReturnValues="ALL_NEW"
    )

    return response.get('Attributes')

@tracer.capture_method
def update_extracted_po(extracted_po_id, user, timestamp):
    response = EXTRACTED_PO_DDB_TABLE.update_item(
        Key={'extractedPoId': extracted_po_id},
        UpdateExpression='SET documentStatus = :documentStatus, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy, approvedAt = :approvedAt, approvedBy = :approvedBy',
        ExpressionAttributeValues={
            ':documentStatus': 'Success',
            ':exceptionStatus': 'N/A',
            ':updatedAt': timestamp,
            ':updatedBy': user,
            ':approvedAt': timestamp,
            ':approvedBy': user
        },
        ConditionExpression="attribute_exists(extractedPoId)",
        ReturnValues="ALL_NEW"
    )

    return response.get('Attributes')

@tracer.capture_method
def update_extracted_grn(extracted_grn_id, user, timestamp):
    response = EXTRACTED_GRN_DDB_TABLE.update_item(
        Key={'extractedGrnId': extracted_grn_id},
        UpdateExpression='SET documentStatus = :documentStatus, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy, approvedAt = :approvedAt, approvedBy = :approvedBy',
        ExpressionAttributeValues={
            ':documentStatus': 'Success',
            ':exceptionStatus': 'N/A',
            ':updatedAt': timestamp,
            ':updatedBy': user,
            ':approvedAt': timestamp,
            ':approvedBy': user
        },
        ConditionExpression="attribute_exists(extractedGrnId)",
        ReturnValues="ALL_NEW"
    )

    return response.get('Attributes')

@tracer.capture_method
def update_extracted_document_line_items(extracted_document_id, user, now):
    response = EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.query(
        IndexName="gsi-extractedDocumentsId",
        KeyConditionExpression=Key('extractedDocumentsId').eq(extracted_document_id)
    )
    
    line_items = response.get('Items', [])
    
    for line_item in line_items:
        line_item_id = line_item.get('extractedDocumentsLineItemsId')
        EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.update_item(
            Key={'extractedDocumentsLineItemsId': line_item_id},
            UpdateExpression='SET #status_attr = :status, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy',
            ExpressionAttributeNames={
                '#status_attr': 'status'
            },
            ExpressionAttributeValues={
                ':status': 'Success',
                ':exceptionStatus': 'N/A',
                ':updatedAt': now,
                ':updatedBy': user
            }
        )

@tracer.capture_method
def update_po_line_items(extracted_po_id, user, now):
    response = EXTRACTED_PO_DDB_LINE_ITEMS_TABLE.query(
        IndexName="gsi-extractedPoId",
        KeyConditionExpression=Key('extractedPoId').eq(extracted_po_id)
    )
    
    line_items = response.get('Items', [])
    
    for line_item in line_items:
        line_item_id = line_item.get('extractedPoLineItemsId')
        EXTRACTED_PO_DDB_LINE_ITEMS_TABLE.update_item(
            Key={'extractedPoLineItemsId': line_item_id},
            UpdateExpression='SET #status_attr = :status, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy',
            ExpressionAttributeNames={
                '#status_attr': 'status'
            },
            ExpressionAttributeValues={
                ':status': 'Success',
                ':exceptionStatus': 'N/A',
                ':updatedAt': now,
                ':updatedBy': user
            }
        )


@tracer.capture_method
def update_grn_line_items(extracted_grn_id, user, now):
    response = EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE.query(
        IndexName="gsi-extractedGrnId",
        KeyConditionExpression=Key('extractedGrnId').eq(extracted_grn_id)
    )
    
    line_items = response.get('Items', [])
    
    for line_item in line_items:
        line_item_id = line_item.get('extractedGrnLineItemsId')
        EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE.update_item(
            Key={'extractedGrnLineItemsId': line_item_id},
            UpdateExpression='SET #status_attr = :status, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy',
            ExpressionAttributeNames={
                '#status_attr': 'status'
            },
            ExpressionAttributeValues={
                ':status': 'Success',
                ':exceptionStatus': 'N/A',
                ':updatedAt': now,
                ':updatedBy': user
            }
        )
   

@tracer.capture_method
def update_document_upload_status(document_upload_id, document_type, user, now):
    if document_type == 'extracted_document':
        all_extracted_documents = EXTRACTED_DOCUMENTS_DDB_TABLE.query(
            IndexName="gsi-documentUploadId",
            KeyConditionExpression=Key('documentUploadId').eq(document_upload_id)
        ).get('Items', [])
    elif document_type == 'po':
        all_extracted_documents = EXTRACTED_PO_DDB_TABLE.query(
            IndexName="gsi-documentUploadId",
            KeyConditionExpression=Key('documentUploadId').eq(document_upload_id)
        ).get('Items', [])
    elif document_type == 'grn':
        all_extracted_documents = EXTRACTED_GRN_DDB_TABLE.query(
            IndexName="gsi-documentUploadId",
            KeyConditionExpression=Key('documentUploadId').eq(document_upload_id)
        ).get('Items', [])
    else:
        raise BadRequestException("Invalid document type")
    

    all_approved = all(extracted_document.get('documentStatus') == 'Success' for extracted_document in all_extracted_documents)

    if all_approved:
        response = DOCUMENT_UPLOAD_DDB_TABLE.update_item(
            Key={'documentUploadId': document_upload_id},
            UpdateExpression='SET #status = :status, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy',
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':status': "Success",
                ':exceptionStatus': "N/A",
                ':updatedAt': now,
                ':updatedBy': user
            },
            ReturnValues="ALL_NEW"
        )

        return response.get('Attributes')
    else:
        response = DOCUMENT_UPLOAD_DDB_TABLE.update_item(
            Key={'documentUploadId': document_upload_id},
            UpdateExpression='SET updatedAt = :updatedAt, updatedBy = :updatedBy',
            ExpressionAttributeValues={
                ':updatedAt': now,
                ':updatedBy': user
            },
            ReturnValues="ALL_NEW"
        )

        return response.get('Attributes')

@tracer.capture_method
def create_timeline_record(document, user, now, extracted_document_type):
    timeline_id = str(uuid.uuid4())

    if extracted_document_type == 'extractedDocument':
        document_id = document.get('extractedDocumentsId')
        merchant_id = document.get('merchantId')
        extracted_document_number = document.get('extracted_documentNumber')
        supplier_name = document.get('supplierName')
        document_type = document.get('documentType')
        
        timeline_item = {
            'timelineId': timeline_id,
            'timelineForId': document_id,
            'merchantId': merchant_id,
            'createdAt': now,
            'createdBy': user,
            'updatedAt': now,
            'updatedBy': user,
            'type': document_type,
            'title': 'Completed Review',
            'description': f"{document_type.upper()} document {extracted_document_number} has completed review",
            'extracted_documentNumber':  extracted_document_number,
            'supplierName': supplier_name
        }
    elif extracted_document_type == 'extractedPo':
        document_id = document.get('extractedPoId')
        merchant_id = document.get('merchantId')
        po_number = document.get('poNumber')
        buyer_name = document.get('buyerName')
        document_type = document.get('documentType')
        
        timeline_item = {
            'timelineId': timeline_id,
            'timelineForId': document_id,
            'merchantId': merchant_id,
            'createdAt': now,
            'createdBy': user,
            'updatedAt': now,
            'updatedBy': user,
            'type': document_type,
            'title': 'Completed Review',
            'description': f"{document_type.upper()} document {po_number} has completed review",
            'poNumber':  po_number,
            'buyerName': buyer_name
        }
    elif extracted_document_type == 'extractedGrn':
        document_id = document.get('extractedGrnId')
        merchant_id = document.get('merchantId')
        grn_number = document.get('grnNumber')
        supplier_name = document.get('supplierName')
        document_type = document.get('documentType')
        
        timeline_item = {
            'timelineId': timeline_id,
            'timelineForId': document_id,
            'merchantId': merchant_id,
            'createdAt': now,
            'createdBy': user,
            'updatedAt': now,
            'updatedBy': user,
            'type': document_type,
            'title': 'Completed Review',
            'description': f"{document_type.upper()} document {grn_number} has completed review",
            'grnNumber':  grn_number,
            'supplierName': supplier_name
        }
    elif extracted_document_type == 'documentUpload':
        document_id = document.get('documentUploadId')
        merchant_id = document.get('merchantId')
        input_source = document.get('inputSource')
        file_name = document.get('fileName')
        document_type = document.get('documentType', "-")
        
        timeline_item = {
            'timelineId': timeline_id,
            'timelineForId': document_id,
            'merchantId': merchant_id,
            'createdAt': now,
            'createdBy': user,
            'updatedAt': now,
            'updatedBy': user,
            'type':  document_type if document_type else "-",
            'title': 'Completed Review',
            'description': f"All extracted documents in {file_name} has completed review",
            'extracted_documentNumber':  file_name,
            'supplierName': input_source
        }
        pass
    else:
        raise BadRequestException("Invalid document type")        
    
    TIMELINE_DDB_TABLE.put_item(Item=timeline_item)

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
        'body': json.dumps({
            "statusCode": status_code,
            "message": message,
            **payload
        })
    }

def generatePo(document_upload_id, merchant_id):
    extracted_document_id = ''
    po_number = ''
    successful_line_items = []
    line_items_created = 0
    buyerGroup =''
    purchaserCode =''

    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Get extracted document
    extracted_document = EXTRACTED_DOCUMENTS_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id)
    ).get('Items', [])
    if extracted_document:
        extracted_document = extracted_document[0]
        extracted_document_id = extracted_document['extractedDocumentsId']
        extracted_po_id = extracted_document['referenceExtractedPoId']
    else:
        logger.error(f"No extracted document found for documentUploadId: {document_upload_id}")
        return 

    # Get extracted document line items
    extracted_document_line_items = EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.query(
        IndexName='gsi-extractedDocumentsId',
        KeyConditionExpression=Key('extractedDocumentsId').eq(extracted_document_id)
    ).get('Items', [])

    # Get extracted po line items to check for duplicates
    po_line_items = EXTRACTED_PO_DDB_LINE_ITEMS_TABLE.query(
        IndexName='gsi-extractedPoId',
        KeyConditionExpression=Key('extractedPoId').eq(extracted_po_id)
    ).get('Items', [])
    
    # FIX: Calculate totalAmount from extracted document line items FIRST
    totalAmount = Decimal('0')
    
    # Generate PO line items and calculate total
    for line_item in extracted_document_line_items:
        # Check and skip for existing PO line items
        skip_item = False
        for po_line_item in po_line_items:
            if po_line_item['extractedDocumentsLineItemsId'] == line_item['extractedDocumentsLineItemsId']:
                logger.info(f"PO line item already exists for extracted_document line item {line_item['extractedDocumentsLineItemsId']}, SKIP")
                skip_item = True
                break
        
        # Skip this item if duplicate found
        if skip_item:
            continue

            
        # Check status and generate PO if success
        status = line_item.get("status", "").lower()
        if status == "success":
            extracted_po_line_item_id = str(uuid.uuid4())
            buyerGroup, purchaserCode = populateBuyerGroup(line_item)

            # FIX: Get totalPrice from extracted document line item and add to total
            line_item_total_price = clean_to_decimal(line_item.get('totalPrice', '0'))
            totalAmount += line_item_total_price
            
            extracted_po_line_item_payload = {
                "extractedPoLineItemsId": extracted_po_line_item_id,
                "documentUploadId": document_upload_id,
                "extractedPoId": extracted_po_id,
                "extractedDocumentsLineItemsId": line_item.get('extractedDocumentsLineItemsId', '-'),
                "extractedDocumentsId": extracted_document.get('extractedDocumentsId', '-'),
                "expectedReceipt": extracted_document.get('expectedReceipt', '-'),
                "invoiceDate": extracted_document.get('invoiceDate', '-'),
                "invoiceNumber": extracted_document.get('invoiceNumber', '-'),
                "description": line_item.get('description', '-'),
                "itemCode": line_item.get('itemCode', '-'),
                "quantity": clean_to_decimal(line_item.get('quantity', '-')),
                "unitPrice": line_item.get('unitPrice', '-'),
                "uom": line_item.get('itemUom', '-'),
                "itemType": line_item.get('itemType', '-'),
                "totalPrice": line_item_total_price,  # Use cleaned decimal value
                "purchaserCode": purchaserCode,
                "poExpiry": extracted_document.get('poExpiry', '-'),
                "buyerGroup": buyerGroup,
                "locationCode": extracted_document.get('locationCode', '-'),
                "merchantId": extracted_document.get('merchantId', '-'),
                "status": "Success",
                "exceptionStatus": "N/A",
                "createdAt": now,
                "updatedAt": now
            } 

            EXTRACTED_PO_DDB_LINE_ITEMS_TABLE.put_item(Item=extracted_po_line_item_payload)
            successful_line_items.append(extracted_po_line_item_payload)
            line_items_created += 1
            logger.info(f"Created line item {line_items_created} with totalPrice: {line_item_total_price}, PO line item id: {extracted_po_line_item_id}")

    logger.info(f"Calculated totalAmount from {line_items_created} line items: {totalAmount}")

    poNumber = generatePONumber(extracted_document.get('invoiceDate'))
    
    # Check if PO already exists
    try:
        existing_po_response = EXTRACTED_PO_DDB_TABLE.get_item(
            Key={'extractedPoId': extracted_po_id}
        )
        existing_po = existing_po_response.get('Item')
        
        if existing_po:
            # FIX: Update existing PO with new totalAmount and line items
            logger.info(f"PO {extracted_po_id} already exists, updating with new line items")
            
            # Get existing line items and totalAmount
            existing_line_items = existing_po.get('lineItem', [])
            existing_total = clean_to_decimal(existing_po.get('totalAmount', '0'))
            
            # Calculate new total (existing + new line items)
            updated_total = existing_total + totalAmount
            updated_line_items = existing_line_items + [successful_line_items] if successful_line_items else existing_line_items
            
            EXTRACTED_PO_DDB_TABLE.update_item(
                Key={'extractedPoId': extracted_po_id},
                UpdateExpression='''SET 
                    totalAmount = :totalAmount,
                    lineItem = :lineItem,
                    updatedAt = :updatedAt,
                    updatedBy = :updatedBy''',
                ExpressionAttributeValues={
                    ':totalAmount': updated_total,
                    ':lineItem': updated_line_items,
                    ':updatedAt': now,
                    ':updatedBy': 'System'
                }
            )
            logger.info(f"Updated existing PO {extracted_po_id} with totalAmount: {updated_total} (was: {existing_total}, added: {totalAmount})")
            
        else:
            # Create new PO with calculated totalAmount
            extracted_po_payload = {
                "extractedPoId": extracted_po_id,
                "merchantId": merchant_id,
                "poNumber": poNumber,
                "poDate": extracted_document.get('invoiceDate'),
                "supplierName": extracted_document.get('supplierName'),
                "supplierCode": extracted_document.get('supplierCode'),
                "buyerName": extracted_document.get('buyerName'),
                "storeLocation": extracted_document.get('storeLocation'),
                "locationCode": extracted_document.get('locationCode'),
                "dim": extracted_document.get('dim'),
                "invoiceDate": extracted_document.get("invoiceDate", ""),
                "invoiceNumber": extracted_document.get("invoiceNumber", ""),
                "poExpiry": extracted_document.get("poExpiry"),
                "currency": extracted_document.get('currency'),
                "totalAmount": totalAmount,
                "status": "Success",
                "exceptionStatus": "N/A",
                "createdAt": now,
                "createdBy": "System",
                "updatedAt": now,
                "updatedBy": "System",
                "sourceFile": extracted_document.get('sourceFile'),
                "documentUploadId": document_upload_id,
                "lineItem": [successful_line_items] if successful_line_items else []
            }
            EXTRACTED_PO_DDB_TABLE.put_item(
                Item=extracted_po_payload,
                ConditionExpression='attribute_not_exists(extractedPoId)'  # Prevent overwrites
            )
            logger.info(f"Created new PO {extracted_po_id} with totalAmount: {totalAmount} and {line_items_created} line items")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info(f"PO {extracted_po_id} already exists (race condition), updating instead")
            
            # Handle race condition by getting existing PO and updating
            existing_po_response = EXTRACTED_PO_DDB_TABLE.get_item(
                Key={'extractedPoId': extracted_po_id}
            )
            existing_po = existing_po_response.get('Item')
            
            if existing_po:
                existing_total = clean_to_decimal(existing_po.get('totalAmount', '0'))
                updated_total = existing_total + totalAmount
                existing_line_items = existing_po.get('lineItem', [])
                updated_line_items = existing_line_items + [successful_line_items] if successful_line_items else existing_line_items
                
                EXTRACTED_PO_DDB_TABLE.update_item(
                    Key={'extractedPoId': extracted_po_id},
                    UpdateExpression='''SET 
                        totalAmount = :totalAmount,
                        lineItem = :lineItem,
                        updatedAt = :updatedAt,
                        updatedBy = :updatedBy''',
                    ExpressionAttributeValues={
                        ':totalAmount': updated_total,
                        ':lineItem': updated_line_items,
                        ':updatedAt': now,
                        ':updatedBy': 'System'
                    }
                )
                logger.info(f"Updated PO {extracted_po_id} after race condition with totalAmount: {updated_total}")
        else:
            logger.error(f"Error creating/updating PO: {str(e)}")
            raise

    logger.info(f"PO generation completed for document_upload_id: {document_upload_id}")
    logger.info(f"  - PO ID: {extracted_po_id}")
    logger.info(f"  - PO Number: {poNumber}")
    logger.info(f"  - Line items created: {line_items_created}")
    logger.info(f"  - Total Amount: {totalAmount}")

def clean_to_decimal(value):
    if not value or value == "-":
        return Decimal('0')
    # Remove currency symbols before conversion
    cleaned = str(value).replace('RM', '').replace('USD', '').replace('SGD', '').replace('$', '').replace(',', '').strip()
    try:
        return Decimal(cleaned)
    except (ValueError, TypeError, decimal.InvalidOperation):
        logger.warning(f"Could not convert '{value}' (cleaned: '{cleaned}') to Decimal, using 0")
        return Decimal('0')

@tracer.capture_method
def generatePoNumber(merchantId, extraction_date):

    # Convert extraction_date to proper format and get today's date
    if isinstance(extraction_date, str):
        try:
            date_obj = datetime.strptime(extraction_date, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            # Try alternative format if the first one fails
            date_obj = datetime.strptime(extraction_date, '%Y-%m-%d')
    else:
        date_obj = extraction_date
    
    # Format as DDMMYYYY for consistency with the requirement
    today = date_obj.strftime('%Y-%m-%d')
    todayDate = date_obj.strftime('%d%m%Y')
    postingNoPrefix = f'ROBO-{todayDate}'
    
    # Check if sequence number generator exists for this prefix
    sequenceNumGenResp = getSequenceNumberGenerator(postingNoPrefix)
    
    if not sequenceNumGenResp:
        # Create new sequence generator
        createSequenceNumberGenerator(postingNoPrefix, extraction_date)
        postingNo = f'{postingNoPrefix}-0001'
    else:
        # Get latest date from existing sequence generator
        latestDate = datetime.strptime(sequenceNumGenResp.get('updatedAt'), '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d')
        
        if latestDate == today:
            # Same date - increment sequence
            latestValue = str(int(sequenceNumGenResp.get('latestValue')) + 1).zfill(4)
        else:
            # New date - reset sequence
            latestValue = '0001'
        
        # Update sequence generator
        updateSequenceNumberGenerator(postingNoPrefix, latestValue, extraction_date)
        postingNo = f'{postingNoPrefix}-{latestValue}'
    
    return postingNo


@tracer.capture_method
def getSequenceNumberGenerator(sequenceNumberGeneratorId):
    """Get sequence number generator record"""
    try:
        response = SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.get_item(
            Key={'sequenceNumberGeneratorId': sequenceNumberGeneratorId}
        )
        return response.get('Item')
    except Exception as e:
        logger.error(f"Error getting sequence number generator: {str(e)}")
        return None

@tracer.capture_method
def createSequenceNumberGenerator(sequenceNumberGeneratorId, now):
    """Create new sequence number generator record"""
    try:
        SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.put_item(Item={
            'sequenceNumberGeneratorId': sequenceNumberGeneratorId,
            'latestValue': '0001',
            'updatedAt': now
        })
        logger.info(f"Created sequence number generator for: {sequenceNumberGeneratorId}")
    except Exception as e:
        logger.error(f"Error creating sequence number generator: {str(e)}")

@tracer.capture_method
def updateSequenceNumberGenerator(sequenceNumberGeneratorId, latestValue, now):
    """Update sequence number generator with new value"""
    try:
        SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.update_item(
            Key={'sequenceNumberGeneratorId': sequenceNumberGeneratorId},
            UpdateExpression='SET latestValue=:latestValue, updatedAt=:updatedAt',
            ExpressionAttributeValues={
                ':latestValue': latestValue,
                ':updatedAt': now
            }
        )
        logger.info(f"Updated sequence number generator: {sequenceNumberGeneratorId} to value: {latestValue}")
    except Exception as e:
        logger.error(f"Error updating sequence number generator: {str(e)}")

@tracer.capture_method
def populateBuyerGroup(line_item):
    itemCode = line_item.get('itemCode')
    buyerGroup = line_item.get('buyerGroup')
    purchaserCode = line_item.get('purchaserCode')

    if not itemCode or itemCode == '-':
        buyerGroup = '-'
        purchaserCode = '-'


    first_char = itemCode[0].upper()
    
    if first_char == 'C':
        buyerGroup = 'CON_CAPEX'
        purchaserCode = 'PR04'
    elif first_char == 'O':
        buyerGroup = 'CON_OPEX'
        purchaserCode = 'PR05'
    else:
        buyerGroup = 'CON_SPARE'
        purchaserCode = 'PR03'
    
    return buyerGroup,purchaserCode