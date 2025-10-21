import os
import boto3
import json
import time
import pandas as pd
from decimal import Decimal
import io
import copy
from aws_lambda_powertools import Logger, Tracer
from botocore.exceptions import NoCredentialsError, ClientError
import csv
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import uuid
from bedrock_function import promptBedrock
import re
import defaultPrompts

DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
EXTRACTED_REFERRAL_LETTER_TABLE = os.environ.get('EXTRACTED_REFERRAL_LETTER_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
BDA_PROCESSING_BUCKET = os.environ.get('BDA_PROCESSING_BUCKET')

S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs', region_name='us-east-1')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_REFERRAL_LETTER_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_REFERRAL_LETTER_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)


logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        day = datetime.now().strftime('%Y_%m_%d')

        total_input_tokens = 0
        total_output_tokens = 0

        for record in event.get('Records', []):
            body = json.loads(record.get('body', '{}'))
            invocation_id = body.get('invocation_id')
            result_json_list = body.get('result_json_list', [])
            source_file_name = body.get('source_file_name')
            merchantId = body.get('merchant_id')
            documentUploadId = body.get('document_upload_id')
            file_path = body.get('file_path')
            
            updatedMappedJsonData = []
            
            if not result_json_list:
                continue

            mappedJsonData = processJsonResult(result_json_list)

            for jsonData in mappedJsonData:
                logger.info(f'Data: {jsonData}')

                createExtractedReferralLetterRecord(jsonData, merchantId, documentUploadId, source_file_name, file_path, now)

                updatedMappedJsonData.append(jsonData)
                
            logger.info(f'UPDATED MAPPED JSON: {updatedMappedJsonData}')
            updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData, now)
 
        return {
            "status": True,
            "body": "Data extraction process completed."
        }
    
    except NoCredentialsError:
        return {"status": False, "body": "AWS credentials not available"}
    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        updateFailedDocumentUploadStatus(documentUploadId, "System Error")
        createFailedExtractedReferralRecord(documentUploadId, source_file_name, file_path, now, merchantId)
        return {
            "status": True,
            'body': "The server encountered an unexpected condition that prevented it from fulfilling your request."
        }

@tracer.capture_method
def processJsonResult(result_json_list):
    mappedJsonData = []

    field_mapping = {
        "patientDiagnosis": ["patientDiagnosis"],
        "patientEmail": ["patientEmail"],
        "patientIdentificationNumber": ["patientIdentificationNumber"],
        "patientName": ["patientName"],
        "patientPhoneNumber": ["patientPhoneNumber"],
        "patientReasonForReferral": ["patientReasonForReferral"],
        "preliminaryWorkUps": ["preliminaryWorkUps"],
        "receivingAddress": ["receivingAddress"],
        "receivingDepartment": ["receivingDepartment"],
        "receivingDoctorEmail": ["receivingDoctorEmail"],
        "receivingDoctorName": ["receivingDoctorName"],
        "receivingDoctorPhoneNumber": ["receivingDoctorPhoneNumber"],
        "referringAddress": ["referringAddress"],
        "referringDepartment": ["referringDepartment"],
        "referringDoctorEmail": ["referringDoctorEmail"],
        "referringDoctorPosition": ["referringDoctorPosition"],
        "referringDoctorName": ["referringDoctorName"],
        "referringDoctorPhoneNumber": ["referringDoctorPhoneNumber"],
    }

    for file_key in result_json_list:
        try:
            response = S3_CLIENT.get_object(Bucket=BDA_PROCESSING_BUCKET, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)

            explainability_info_list = data.get('explainability_info', [])
            
            row = {}
            extracted_data = {}
            row['boundingBoxes'] = {}  
            row['confidenceScores'] = {}
            service_table_data = None
            
            # First, extract raw data from explainability info
            for explanation_obj in explainability_info_list:
                for key, value in explanation_obj.items():
                    if key == "service_table":
                        service_table_data = value
                    else:
                        # Store the raw data for later processing
                        extracted_data[key] = value

            logger.info(f'EXTRACTED DATA: {extracted_data}')
            
            # Now process the extracted data according to field_mapping
            for column, possible_keys in field_mapping.items():
                value = ""
                row['boundingBoxes'][column] = []
                row["confidenceScores"][column] = []  
                
                for key in possible_keys:
                    if key in extracted_data:
                        field_info = extracted_data[key]
                        if isinstance(field_info, list) and len(field_info) > 0:
                            # Handle list fields like preliminaryWorkUps, patientDiagnosis
                            values = []
                            confidences = []
                            all_geometries = []
                            
                            for item in field_info:
                                if isinstance(item, dict):
                                    item_value = item.get('value', '')
                                    if item_value:
                                        values.append(item_value)
                                    
                                    item_confidence = item.get('confidence', 0)
                                    if item_confidence:
                                        confidences.append(item_confidence)
                                    
                                    item_geometry = item.get('geometry', [])
                                    all_geometries.extend(item_geometry)
                            
                            # Combine values with comma delimiter
                            value = ', '.join(values)
                            
                            # Calculate average confidence
                            confidence_score = sum(confidences) / len(confidences) if confidences else 0
                            
                            # Process combined geometry - find top-left and calculate combined dimensions
                            geometry_data = all_geometries
                            
                        else:
                            # Handle single object fields
                            value = field_info.get('value', '')
                            geometry_data = field_info.get('geometry', [])
                            confidence_score = field_info.get('confidence', 0)
                        row['confidenceScores'][column].append(float(confidence_score))
                        
                        if geometry_data:
                            # Find the topmost-leftmost bounding box and calculate combined dimensions
                            min_top = float('inf')
                            min_left = float('inf')
                            max_right = 0
                            max_bottom = 0
                            page_number = geometry_data[0].get('page', '') if geometry_data else ''
                            
                            for geo_data in geometry_data:
                                bounding_box = geo_data.get('boundingBox', {})
                                top = bounding_box.get('top', 0)
                                left = bounding_box.get('left', 0)
                                width = bounding_box.get('width', 0)
                                height = bounding_box.get('height', 0)
                                
                                # Track the boundaries
                                min_top = min(min_top, top)
                                min_left = min(min_left, left)
                                max_right = max(max_right, left + width)
                                max_bottom = max(max_bottom, top + height)
                            
                            # Calculate combined dimensions
                            combined_width = max_right - min_left if max_right > min_left else 0
                            combined_height = max_bottom - min_top if max_bottom > min_top else 0
            
                            # Convert to Decimal for DynamoDB
                            decimal_box = {
                                'width': Decimal(str(combined_width)),
                                'height': Decimal(str(combined_height)),
                                'left': Decimal(str(min_left)),
                                'top': Decimal(str(min_top)),
                                'page': page_number  # Add page to each bounding box
                            }
                            
                            # Add bounding box to the appropriate field array
                            row['boundingBoxes'][column].append(decimal_box)
                        
                        
                        break  # Break after finding the first matching key
                
                # Set the actual value for this field
                row[column] = value

            confidence_scores = row.get('confidenceScores')
            # Flatten all confidence score lists and calculate average
            all_scores = []
            for score_list in confidence_scores.values():
                if isinstance(score_list, list):
                    all_scores.extend([float(score) for score in score_list if score])
                elif score_list:  # Handle case where it might be a single value
                    all_scores.append(float(score_list))

            row["confidenceScore"] = sum(all_scores) / len(all_scores) if all_scores else 0
            mappedJsonData.append(row)

        except Exception as e:
            logger.error(f"Error processing file {file_key}: {str(e)}")
            # Continue with next file if there's an error

    logger.info(f'MAPPED JSON DATA: {mappedJsonData}')
    return mappedJsonData

@tracer.capture_method
def createExtractedReferralLetterRecord(jsonData, merchantId, documentUploadId, source_file_name, file_path, now):
    extractedReferralLetterId = str(uuid.uuid4())

    extractedReferralLetterPayload = {
        "extractedReferralLetterId": extractedReferralLetterId,
        "merchantId": merchantId,
        "patientDiagnosis": jsonData.get("patientDiagnosis"),
        "patientEmail": jsonData.get("patientEmail"),
        "patientIdentificationNumber": jsonData.get("patientIdentificationNumber"),
        "patientName": jsonData.get("patientName"),
        "patientPhoneNumber": jsonData.get("patientPhoneNumber"),
        "patientReasonForReferral": jsonData.get("patientReasonForReferral"),
        "preliminaryWorkUps": jsonData.get("preliminaryWorkUps"),
        "receivingAddress": jsonData.get("receivingAddress"),
        "receivingDepartment": jsonData.get("receivingDepartment"),
        "receivingDoctorEmail": jsonData.get("receivingDoctorEmail"),
        "receivingDoctorName": jsonData.get("receivingDoctorName"),
        "receivingDoctorPhoneNumber": jsonData.get("receivingDoctorPhoneNumber"),
        "referringAddress": jsonData.get("referringAddress"),
        "referringDepartment": jsonData.get("referringDepartment"),
        "referringDoctorPosition": jsonData.get("referringDoctorPosition"),
        "referringDoctorEmail": jsonData.get("referringDoctorEmail"),
        "referringDoctorName": jsonData.get("referringDoctorName"),
        "referringDoctorPhoneNumber": jsonData.get("referringDoctorPhoneNumber"),
        'boundingBoxes': jsonData.get('boundingBoxes'),
        'status': 'Success',
        "exceptionStatus": 'N/A',
        "filePath": file_path,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "remarks": "",
        "sourceFile": source_file_name,
        "condidenceScore": jsonData.get("confidenceScore"),
        "confidenceScores": jsonData.get("confidenceScores"),
        "documentUploadId": documentUploadId
    }

    extractedReferralLetterPayload = convert_floats_to_decimals(extractedReferralLetterPayload)
    EXTRACTED_REFERRAL_LETTER_DDB_TABLE.put_item(Item=extractedReferralLetterPayload)
    jsonData["extractedReferralLetterId"] = extractedReferralLetterId

    return jsonData

@tracer.capture_method
def documentUploadStatusCheck(document_upload_id):
    all_extracted_documents = EXTRACTED_REFERRAL_LETTER_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id)
    ).get('Items', [])

    all_statuses = [extracted_document.get('exceptionStatus') for extracted_document in all_extracted_documents]

    prompt = defaultPrompts.DOCUMENT_UPLOAD_STATUS_CHECK_PROMPT.format(all_statuses=all_statuses)

    exception_status, input_tokens, output_tokens = promptBedrock(prompt)
    exception_status = json.loads(exception_status)
    logger.info(f'DOCUMENT EXCEPTION STATUS CHECK RESULT: {exception_status}')
    return exception_status, input_tokens, output_tokens

@tracer.capture_method
def updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData, now):
    # Collect valid confidence scores (ensure they're not 0 or None)
    confidence_scores = []
    for mappedJson in updatedMappedJsonData:
        score = mappedJson.get("confidenceScore", 0)
        confidence_scores.append(float(score))

    # Calculate average (avoid division by zero)
    if confidence_scores and any(confidence_scores):
        avg_confidence_score = round((sum(confidence_scores) / len(confidence_scores)) * 100)
    else:
        avg_confidence_score = 0
    avg_confidence_score_decimal = convert_floats_to_decimals(avg_confidence_score)

    # Convert for DynamoDB
    confidence_scores_decimal = [convert_floats_to_decimals(score) for score in confidence_scores]

    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={
            'documentUploadId': documentUploadId,
        },
        UpdateExpression="set #status_attr = :status, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy, avgConfidenceScore = :avgConfidenceScore, confidenceScoreList = :confidenceScoreList",
        ExpressionAttributeNames={
            '#status_attr': 'status'
        },
        ExpressionAttributeValues={
            ':status': "Success",
            ':exceptionStatus': "N/A",
            ':updatedAt': now,
            ':updatedBy': "System",
            ':avgConfidenceScore': avg_confidence_score_decimal,
            ':confidenceScoreList': confidence_scores_decimal 
        }
    )

    return True

@tracer.capture_method
def updateFailedDocumentUploadStatus(documentUploadId, now):
    logger.info('Updating document upload status to Fail')
    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={
            'documentUploadId': documentUploadId,
        },
        UpdateExpression="set #status_attr = :status, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt",
        ExpressionAttributeNames={
            '#status_attr': 'status',

        },
        ExpressionAttributeValues={
            ':status': "Fail",
            ':exceptionStatus': "Processing Failed",
            ':updatedAt': now
        }
    )

@tracer.capture_method
def createFailedExtractedReferralRecord(documentUploadId, source_file_name, file_path, now, merchantId):
    logger.info('failed extracted referral letter record creation')
    extractedReferralLetterId = str(uuid.uuid4())

    failedExtractedReferralPayload = {
        "extractedReferralLetterId": extractedReferralLetterId,
        "merchantId": merchantId,
        "patientDiagnosis": "-",
        "patientEmail": "-",
        "patientIdentificationNumber": "-",
        "patientName": "-",
        "patientPhoneNumber": "-",
        "patientReasonForReferral": "-",
        "preliminaryWorkUps": "-",
        "receivingAddress": "-",
        "receivingDepartment": "-",
        "receivingDoctorEmail": "-",
        "receivingDoctorName": "-",
        "receivingDoctorPhoneNumber": "-",
        "referringAddress": "-",
        "referringDepartment": "-",
        "referringDoctorEmail": "-",
        "referringDoctorPosition": "-",
        "referringDoctorName": "-",
        "referringDoctorPhoneNumber": "-",
        'boundingBoxes': {},
        'status': 'Fail',
        "exceptionStatus": "Processing Failed",
        "filePath": file_path,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "remarks": "",
        "sourceFile": source_file_name,
        "condidenceScore": 0,
        "confidenceScores": {},
        "documentUploadId": documentUploadId
    }

    EXTRACTED_REFERRAL_LETTER_DDB_TABLE.put_item(Item=failedExtractedReferralPayload)
    pass


@tracer.capture_method
def convert_floats_to_decimals(obj):
    """
    Recursively convert all float values in a nested structure to Decimal
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimals(item) for item in obj]
    else:
        return obj
