import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
from decimal import Decimal
from authorizationHelper import is_authenticated, has_permission, Permission, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException
from boto3.dynamodb.conditions import Attr, Key

EXTRACTED_DOCUMENTS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')
DOCUMENT_UPLOAD_TABLE = os.environ.get("DOCUMENT_UPLOAD_TABLE")
EXTRACTED_GRN_TABLE = os.environ.get("EXTRACTED_GRN_TABLE")
EXTRACTED_GRN_LINE_ITEMS_TABLE = os.environ.get("EXTRACTED_GRN_LINE_ITEMS_TABLE")
EXTRACTED_PO_LINE_ITEMS_TABLE = os.environ.get("EXTRACTED_PO_LINE_ITEMS_TABLE")
EXTRACTED_PO_TABLE = os.environ.get("EXTRACTED_PO_TABLE")
EXTRACTED_REFERRAL_LETTER_TABLE = os.environ.get("EXTRACTED_REFERRAL_LETTER_TABLE")

DDB_RESOURCE = boto3.resource('dynamodb')

EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_GRN_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_TABLE) 
EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_LINE_ITEMS_TABLE)
EXTRACTED_PO_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_PO_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_LINE_ITEMS_TABLE)
EXTRACTED_REFERRAL_LETTER_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_REFERRAL_LETTER_TABLE)

logger = Logger()
tracer = Tracer()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        merchant_id = user.get('merchantId')
        user_group_name = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group_name, Permission.GET_SPECIFIC_DOCUMENT.value)

        parameters = event.get('queryStringParameters', {})

        if parameters and parameters.get('extractedDocumentId'):
            extracted_document_id = parameters.get('extractedDocumentId')
                
            extracted_document = get_extracted_document(extracted_document_id, merchant_id)
            line_items = get_extracted_document_line_items(extracted_document_id)

            payload = format_resonse(extracted_document, line_items)

            return create_response(200, "Success", payload)
        
        elif parameters and parameters.get('extractedGrnId'):
            extracted_document_id = parameters.get('extractedGrnId')
                
            extracted_document = get_extracted_grn(extracted_document_id, merchant_id)
            line_items = get_extracted_grn_line_items(extracted_document_id)

            payload = format_resonse(extracted_document, line_items)

            return create_response(200, "Success", payload)
        
        elif parameters and parameters.get('extractedPoId'):
            extracted_document_id = parameters.get('extractedPoId')
                
            extracted_document = get_extracted_po(extracted_document_id, merchant_id)
            line_items = get_extracted_po_line_items(extracted_document_id)

            payload = format_resonse(extracted_document, line_items)

            return create_response(200, "Success", payload)
        
        elif parameters and parameters.get('extractedReferralLetterId'):
            extracted_document_id = parameters.get('extractedReferralLetterId')
                
            extracted_document = get_extracted_referral_letter(extracted_document_id, merchant_id)
            line_items = get_extracted_referral_letter_line_items(extracted_document_id)

            payload = format_resonse(extracted_document, line_items)

            return create_response(200, "Success", payload)
    
        else:
            raise BadRequestException("ID is required")
            
    except (AuthenticationException, AuthorizationException, BadRequestException, ResourceNotFoundException) as e:
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
def get_extracted_document(extracted_document_id, merchant_id):
    response = EXTRACTED_DOCUMENTS_DDB_TABLE.get_item(
        Key={
            'extractedDocumentsId': extracted_document_id
        },
    )

    existing_document = response.get('Item')
    if existing_document.get('merchantId') != merchant_id:
        raise AuthorizationException("You do not have permission to access this document")

    return existing_document

@tracer.capture_method
def get_extracted_grn(extracted_document_id, merchant_id):
    response = EXTRACTED_GRN_DDB_TABLE.get_item(
        Key={
            'extractedGrnId': extracted_document_id
        },
    )

    existing_document = response.get('Item')
    if existing_document.get('merchantId') != merchant_id:
        raise AuthorizationException("You do not have permission to access this document")

    return existing_document

@tracer.capture_method
def get_extracted_po(extracted_document_id, merchant_id):
    response = EXTRACTED_PO_DDB_TABLE.get_item(
        Key={
            'extractedPoId': extracted_document_id
        },
    )

    existing_document = response.get('Item')
    if existing_document.get('merchantId') != merchant_id:
        raise AuthorizationException("You do not have permission to access this document")

    return existing_document

@tracer.capture_method
def get_extracted_referral_letter(extracted_document_id, merchant_id):
    response = EXTRACTED_REFERRAL_LETTER_DDB_TABLE.get_item(
        Key={
            'extractedReferralLetterId': extracted_document_id
        },
    )

    existing_document = response.get('Item')
    if existing_document.get('merchantId') != merchant_id:
        raise AuthorizationException("You do not have permission to access this document")

    return existing_document

@tracer.capture_method
def get_extracted_document_line_items(extracted_document_id):
    response = EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.query(
        IndexName='gsi-extractedDocumentsId',
        KeyConditionExpression=Key('extractedDocumentsId').eq(extracted_document_id)
    )

    line_items = response.get('Items')

    return line_items

@tracer.capture_method
def get_extracted_grn_line_items(extracted_document_id):
    response = EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE.query(
        IndexName='gsi-extractedGrnId',
        KeyConditionExpression=Key('extractedGrnId').eq(extracted_document_id)
    )

    line_items = response.get('Items')

    return line_items

@tracer.capture_method
def get_extracted_po_line_items(extracted_document_id):
    response = EXTRACTED_PO_LINE_ITEMS_DDB_TABLE.query(
        IndexName='gsi-extractedPoId',
        KeyConditionExpression=Key('extractedPoId').eq(extracted_document_id)
    )

    line_items = response.get('Items')

    return line_items

@tracer.capture_method
def get_extracted_referral_letter_line_items(extracted_document_id):
    # response = EXTRACTED_REFERRAL_LETTER_DDB_TABLE.query(
    #     IndexName='gsi-extractedReferralLetterId',
    #     KeyConditionExpression=Key('extractedReferralLetterId').eq(extracted_document_id)
    # )

    # line_items = response.get('Items')

    return []

@tracer.capture_method
def query_extracted_documents(document_upload_id):
    response = EXTRACTED_DOCUMENTS_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents

@tracer.capture_method
def query_extracted_grn(document_upload_id):
    response = EXTRACTED_GRN_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents

@tracer.capture_method
def query_extracted_po(document_upload_id):
    response = EXTRACTED_PO_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents

@tracer.capture_method
def query_extracted_referral_letter(document_upload_id):
    response = EXTRACTED_REFERRAL_LETTER_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents

@tracer.capture_method
def get_document_list(documentUploadId, documentType):
    documentList = []
    if documentType == 'grn':
        extracted_grn = query_extracted_grn(documentUploadId)
        for item in extracted_grn:
            documentList.append({
                'extractedGrnId': item.get('extractedGrnId'),
                'grnNumber': item.get('grnNumber'),
            })

    elif documentType == 'po':
        extracted_po = query_extracted_po(documentUploadId)
        for item in extracted_po:
            documentList.append({
                'extractedPoId': item.get('extractedPoId'),
                'poNumber': item.get('poNumber'),
            })
    
    elif documentType == 'medicalReferralLetter':
        extracted_referral_letter = query_extracted_referral_letter(documentUploadId)
        for idx, item in enumerate(extracted_referral_letter):
            documentList.append({
                'extractedReferralLetterId': item.get('extractedReferralLetterId'),
                'referralLetterNumber': idx+1,
            })

    else:
        extracted_documents = query_extracted_documents(documentUploadId)
        for item in extracted_documents:
            documentList.append({
                'extractedDocumentsId': item.get('extractedDocumentsId'),
                'invoiceNumber': item.get('invoiceNumber'),
            })
        
    return documentList

@tracer.capture_method
def format_resonse(extracted_document, line_items):
    if extracted_document.get('extractedGrnId'):
        document_type = "grn"
    elif extracted_document.get('extractedPoId'):
        document_type = "po"
    elif extracted_document.get('extractedReferralLetterId'):
        document_type = "medicalReferralLetter"
    else:
        document_type = "invoice"

    document_list = get_document_list(extracted_document.get('documentUploadId'), document_type)
    documentUploadId = extracted_document.get('documentUploadId')
    document_upload = getDocumentUpload(documentUploadId)
    if documentUploadId:
        extracted_document['fileName'] = document_upload.get('fileName')
        extracted_document['inputPath'] = document_upload.get('inputPath')
    
    form_data = {}
    for key, value in extracted_document.items():
        if key not in ['merchantId']:
            form_data[key] = value

    table_data = []
    for line_item in line_items:
        table_data_item = {}
        for key, value in line_item.items():
            if key not in ['merchantId', 'extractedDocumentsId', 'documentUploadId', 'extractedGrnId', 'extractedPoId']:
                table_data_item[key] = value
        table_data.append(table_data_item)

    response = {
        'data': {
            'formData': form_data,
            'tableData': table_data,
            'document_list': document_list,
        }
    }

    return response

@tracer.capture_method
def getDocumentUpload(documentUploadId):
    response = DOCUMENT_UPLOAD_DDB_TABLE.get_item(
        Key={'documentUploadId': documentUploadId}
    ).get('Item', {})
    return response

    
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
        'body': json.dumps({"statusCode": status_code, "message": message, **payload}, cls=DecimalEncoder)
    }