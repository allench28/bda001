import json
import os
import boto3
import json
import uuid
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException
from datetime import datetime
from boto3.dynamodb.conditions import Key

EXTRACTED_DOCUMENTS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.DELETE_DOCUMENT.value)
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        request_body = json.loads(event.get('body', '{}'))
        uploaded_document_list = request_body.get('uploadedDocumentIds', [])
        extracted_document_list = request_body.get('extractedDocumentIds', [])

        if (uploaded_document_list and extracted_document_list) or (not uploaded_document_list and not extracted_document_list):
            raise BadRequestException('Please provide either uploadedDocumentIds or extractedDocumentIds')
        
        if uploaded_document_list:
            total_affected = delete_uploaded_documents(uploaded_document_list, merchant_id, user.get('email'), now)

        if extracted_document_list:
            total_affected = delete_extracted_documents(extracted_document_list, merchant_id, user.get('email'), now)
        
        return create_response(200, f"Successfully deleted {total_affected} record(s)")
        
    except (AuthenticationException, AuthorizationException, BadRequestException) as e:
        logger.error(f"Custom error: {str(e)}")
        return create_response(400, e.message)
        
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return create_response(
            500, 
            "The server encountered an unexpected condition that prevented it from fulfilling your request."
        )
    
@tracer.capture_method
def delete_uploaded_documents(document_list, merchant_id, user, now):
    total_deleted = 0

    for document_id in document_list:
        document = get_document(document_id, is_uploaded_document=True)
        document_merchant_id = document.get('merchantId')

        if document_merchant_id != merchant_id:
            raise AuthorizationException("You are not authorized to delete this document")

        DOCUMENT_UPLOAD_DDB_TABLE.delete_item(
            Key={
                'documentUploadId': document_id
            }
        )

        total_deleted += 1

        response = EXTRACTED_DOCUMENTS_DDB_TABLE.query(
            IndexName='gsi-documentUploadId',
            KeyConditionExpression=Key('documentUploadId').eq(document_id)
        )
        
        for item in response.get('Items', []):
            extracted_document_id = item.get('extractedDocumentsId')
            EXTRACTED_DOCUMENTS_DDB_TABLE.delete_item(
                Key={
                    'extractedDocumentsId': extracted_document_id
                }
            )
            total_deleted += 1
            
            total_deleted += delete_line_items(extracted_document_id)

        create_timeline_record(document, user, now, is_uploaded_document=True)

    return total_deleted

@tracer.capture_method
def delete_extracted_documents(document_list, merchant_id, user, now):
    total_deleted = 0

    for document_id in document_list:
        document = get_document(document_id)
        document_merchant_id = document.get('merchantId')

        if document_merchant_id != merchant_id:
            raise AuthorizationException("You are not authorized to delete this document")

        EXTRACTED_DOCUMENTS_DDB_TABLE.delete_item(
            Key={
                'extractedDocumentsId': document_id
            }
        )

        total_deleted += 1

        total_deleted += delete_line_items(document_id)

        create_timeline_record(document, user, now)

    return total_deleted

@tracer.capture_method
def delete_line_items(extracted_document_id):
    total_line_items_deleted = 0
    response = EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.query(
        IndexName='gsi-extractedDocumentsId',
        KeyConditionExpression=Key('extractedDocumentsId').eq(extracted_document_id)
    )
    
    for item in response.get('Items', []):
        EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.delete_item(
            Key={
                'extractedDocumentsLineItemsId': item.get('extractedDocumentsLineItemsId') 
            }
        )
        total_line_items_deleted += 1

    return total_line_items_deleted
    

@tracer.capture_method
def get_document(document_id, is_uploaded_document=False):
    if is_uploaded_document:
        document = DOCUMENT_UPLOAD_DDB_TABLE.get_item(
            Key={
                'documentUploadId': document_id
            }
        ).get('Item')
    else:
        document = EXTRACTED_DOCUMENTS_DDB_TABLE.get_item(
            Key={
                'extractedDocumentsId': document_id
            }
        ).get('Item')

    if not document:
        raise BadRequestException('Document not found!')
    
    return document

@tracer.capture_method
def create_timeline_record(document, user, now, is_uploaded_document=False):
    timeline_id = str(uuid.uuid4())

    if is_uploaded_document:
        document_id = document.get('documentUploadId')
        merchant_id = document.get('merchantId')
        input_source = document.get('inputSource')
        file_name = document.get('fileName')
        document_type = document.get('documentType', "-")

        logger.info(f"Document type: {document_type}")
        
        timeline_item = {
            'timelineId': timeline_id,
            'timelineForId': document_id,
            'merchantId': merchant_id,
            'createdAt': now,
            'createdBy': user,
            'updatedAt': now,
            'updatedBy': user,
            'type':  document_type if document_type else "-",
            'title': 'Document delete',
            'description': f"{file_name} deleted by {user}",
            'invoiceNumber':  file_name,
            'supplierName': input_source
        }

        logger.info(f"Timeline item: {timeline_item}")
        
    else:
        document_id = document.get('extractedDocumentsId')
        merchant_id = document.get('merchantId')
        invoice_number = document.get('invoiceNumber')
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
            'title': 'Document delete',
            'description': f"{document_type.upper()} document {invoice_number} deleted by {user}",
            'invoiceNumber':  invoice_number,
            'supplierName': supplier_name
        }
    
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
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }
