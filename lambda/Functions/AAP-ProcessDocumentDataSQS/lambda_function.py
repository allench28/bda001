import json
import os

import boto3
import botocore
from decimal import Decimal
from aws_lambda_powertools import Logger, Tracer
from ddb_functions import (update_document_ddb_item,
                           update_error_document_ddb_item)
from process_function import post_process_document_data
from prompt_functions import get_document_data, get_document_type
from s3_functions import (archive_documents, get_s3_object_binary,
                          get_valid_file_extension)
from textract_functions import textract_get_document_lines
from agent_configuration_functions import get_agent_config

logger = Logger()
tracer = Tracer()

BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
EMAIL_NOTIFICATION_LAMBDA_ARN = os.environ.get('EMAIL_NOTIFICATION_LAMBDA_ARN')

EXTRACTED_DOCUMENT_TABLE = os.environ.get('EXTRACTED_DOCUMENT_TABLE')
DDB_RESOURCE = boto3.resource('dynamodb')
LAMBDA_CLIENT = boto3.client('lambda')

EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_TABLE)

UES_TEXTRACT = os.environ.get("USE_TEXTRACT")

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

@tracer.capture_lambda_handler
def lambda_handler(event, context):    
    email_recipients = []
    objectKey= None
    sourceObjectKey = None
    extractedDocumentPayload = None
    documentType = None
    file_extension = None
    input_tokens=0
    output_tokens=0
    
    use_textract = UES_TEXTRACT.upper() == "TRUE"
    
    try:
        extractedDocumentPayload = event
        objectKey = extractedDocumentPayload.get('inputS3Path')
        sourceObjectKey = extractedDocumentPayload.get('inputSourceS3Path')
        merchantId = extractedDocumentPayload.get('merchantId')
        if not sourceObjectKey:
            sourceObjectKey = objectKey
        if not BUCKET_NAME or not objectKey:
            raise ValueError("Missing required parameters: S3_BUCKET_NAME or inputS3Path")
        
        file_extension = get_valid_file_extension(objectKey)
        binary_content = get_s3_object_binary(BUCKET_NAME, objectKey)
        document_lines = None
            
        capturedDocumentType = None
        docTypeInputTokens = 0
        docTypeOutputTokens = 0
                
        if use_textract:
            document_lines, bounding_boxes_data = textract_get_document_lines(BUCKET_NAME, sourceObjectKey)
            capturedDocumentType, docTypeInputTokens, docTypeOutputTokens = get_document_type(document_lines, None, None)
        else:       
            capturedDocumentType, docTypeInputTokens, docTypeOutputTokens = get_document_type(None, binary_content, file_extension)
            
        documentType = capturedDocumentType
        input_tokens += docTypeInputTokens
        output_tokens += docTypeOutputTokens

        prompt_details, email_recipients = get_agent_config(capturedDocumentType, merchantId)
        
        document_data_list, data_input_tokens, data_output_tokens = get_document_data(prompt_details, capturedDocumentType, binary_content, file_extension, document_lines)
        input_tokens += data_input_tokens
        output_tokens += data_output_tokens
        
        processed_document_data, is_completed = post_process_document_data(document_data_list, bounding_boxes_data)
        
        # Prepare archive path
        new_key = archive_documents(BUCKET_NAME, objectKey, merchantId, sourceObjectKey )
       
        # Update extraction payload
        updated_document = update_document_ddb_item(
            extractedDocumentPayload,
            documentType,
            processed_document_data,
            file_extension,
            new_key,
            input_tokens,
            output_tokens,
            is_completed
        )
        
        document_id =  updated_document.get('extractedDocumentId')

        return create_response(200, f"Succesfully completed document {document_id}",updated_document )
        
    except ValueError as e:
        extractedDocumentPayload["errorMessage"] = f"error: {str(e)}"
        update_error_document(extractedDocumentPayload, documentType, file_extension, objectKey, sourceObjectKey, input_tokens, output_tokens, email_recipients, merchantId)
        return log_error(event, context, e, "Validation")
    except botocore.exceptions.ClientError as e:
        extractedDocumentPayload["errorMessage"] = f"error: {str(e)}"
        update_error_document(extractedDocumentPayload, documentType, file_extension, objectKey, sourceObjectKey, input_tokens, output_tokens, email_recipients, merchantId)
        return log_error(event, context, e, "AWS Service")
    except json.JSONDecodeError as e:
        extractedDocumentPayload["errorMessage"] = f"error: {str(e)}"
        update_error_document(extractedDocumentPayload, documentType, file_extension, objectKey, sourceObjectKey, input_tokens, output_tokens, email_recipients, merchantId)
        return log_error(event, context, e, "JSON parsing")
    except Exception as e:
        extractedDocumentPayload["errorMessage"] = f"error: {str(e)}"
        update_error_document(extractedDocumentPayload, documentType, file_extension, objectKey, sourceObjectKey, input_tokens, output_tokens, email_recipients, merchantId)
        return log_error(event, context, e, "Unexpected")



@tracer.capture_method
def create_response(status_code, message, payload=None):
    if not payload:
        payload = {}

    payload = {
        'statusCode': status_code,
        'body': json.dumps({"statusCode": status_code, "message": message, **payload}, cls=JSONEncoder)
    }
    
    return payload


@tracer.capture_method
def log_error(event, context, e, error_type):
    tracer.put_annotation("lambda_error", "true")
    tracer.put_annotation("lambda_name", context.function_name)
    tracer.put_metadata("event", event)
    tracer.put_metadata("message", str(e))
    logger.exception({"message": str(e)})
    return create_response(500, f"{error_type} error: {str(e)}")


@tracer.capture_method
def update_error_document(extractedDocumentPayload, documentType, file_extension, objectKey, sourceObjectKey, input_tokens, output_tokens, email_recipients, merchantId):
    new_key = archive_documents(BUCKET_NAME, objectKey, merchantId, sourceObjectKey)
    update_error_document_ddb_item(extractedDocumentPayload, documentType, file_extension, new_key, input_tokens, output_tokens)
    send_email_notification(email_recipients, "There was an issue with processing a file", extractedDocumentPayload)

@tracer.capture_method
def send_email_notification(email_recipients, subject, message):
    payload = {
        'emailRecipients': email_recipients,
        'subject': subject,
        'message': message
    }

    response = LAMBDA_CLIENT.invoke(
        FunctionName=EMAIL_NOTIFICATION_LAMBDA_ARN,
        InvocationType='Event',
        Payload=json.dumps(payload)
    )

