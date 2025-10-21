import os
import boto3
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer

logger = Logger()
tracer = Tracer()

EXTRACTED_DOCUMENT_TABLE = os.environ.get('EXTRACTED_DOCUMENT_TABLE')
DDB_RESOURCE = boto3.resource('dynamodb')
EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_TABLE)

COMPLETED_STATUS = "COMPLETED"
NON_COMPLETED_STATUS = "NON_COMPLETED"
ERROR_STATUS = "ERROR"

@tracer.capture_method
def update_document_ddb_item(extractedDocumentPayload, documentType, document_data_list,file_extension, new_key,input_tokens, output_tokens, is_completed=False):
    documentStatus = COMPLETED_STATUS if is_completed else NON_COMPLETED_STATUS
    current_datetime = str(datetime.utcnow().isoformat())[:-3]+"Z"
    
    extractedDocumentPayload['documentType'] = documentType
    extractedDocumentPayload['data'] = document_data_list
    extractedDocumentPayload['updatedAt'] = current_datetime
    extractedDocumentPayload['conversionStatus'] = True
    extractedDocumentPayload['documentStatus'] = documentStatus
    extractedDocumentPayload['archiveS3Path'] = new_key
    extractedDocumentPayload['fileType'] = file_extension
    extractedDocumentPayload['inputTokens'] = input_tokens  
    extractedDocumentPayload['outputTokens'] = output_tokens
    extractedDocumentPayload['merchantId'] = extractedDocumentPayload['merchantId']
    
    start = extractedDocumentPayload['createdAt']
    end = extractedDocumentPayload['updatedAt']
    processing_time = calculate_seconds_between_timestamps(start,end)
    extractedDocumentPayload['processingTime'] = processing_time
    
    update_extracted_document(extractedDocumentPayload)
    logger.info("Document information updated in DynamoDB")
    return extractedDocumentPayload
    
@tracer.capture_method
def update_error_document_ddb_item(extractedDocumentPayload, documentType,file_extension, new_key, input_tokens, output_tokens):
    extractedDocumentPayload['conversionStatus'] = True
    extractedDocumentPayload['documentStatus'] = ERROR_STATUS
    extractedDocumentPayload['documentType'] = documentType
    extractedDocumentPayload['fileType'] = file_extension
    extractedDocumentPayload['archiveS3Path'] = new_key
    extractedDocumentPayload['inputTokens'] = input_tokens
    extractedDocumentPayload['outputTokens'] = output_tokens  
    extractedDocumentPayload['merchantId'] = extractedDocumentPayload['merchantId']
     
    update_extracted_document(extractedDocumentPayload)
    logger.info("Document information updated in DynamoDB")
    return extractedDocumentPayload

@tracer.capture_method
def update_extracted_document(extractedDocumentPayload):
    EXTRACTED_DOCUMENT_DDB_TABLE.put_item(Item=extractedDocumentPayload)

@tracer.capture_method
def calculate_seconds_between_timestamps(timestamp1: str, timestamp2: str) -> str:
    """
    Calculate the number of seconds between two ISO 8601 timestamps and return as whole number.
    """
    try:
        dt1 = datetime.fromisoformat(timestamp1.replace('Z', '+00:00'))
        dt2 = datetime.fromisoformat(timestamp2.replace('Z', '+00:00'))
        
        # Calculate the time difference in seconds and round up to nearest integer
        time_difference = dt2 - dt1
        seconds = str(round(time_difference.total_seconds()))
        return seconds
        
    except ValueError as e:
        raise ValueError("Invalid timestamp format. Please use ISO 8601 format (e.g., '2024-12-12T12:58:50.342Z')") from e