import os
import boto3
import json
import time
import urllib.parse
from datetime import datetime, timedelta
import uuid
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import BadRequestException
from botocore.exceptions import NoCredentialsError, ClientError
from boto3.dynamodb.conditions import Key
import random

BDA_RUNTIME_ENDPOINT = os.environ.get('BDA_RUNTIME_ENDPOINT')
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")
DATA_AUTOMATION_PROJECT_ARN = os.environ.get("DATA_AUTOMATION_PROJECT_ARN")
DATA_AUTOMATION_PROFILE_ARN = os.environ.get("DATA_AUTOMATION_PROFILE_ARN")
SQS_QUEUE = os.environ.get("SQS_QUEUE")
GRN_SQS_QUEUE = os.environ.get("GRN_SQS_QUEUE")
PO_SQS_QUEUE = os.environ.get("PO_SQS_QUEUE", "")
BR_SQS_QUEUE = os.environ.get("BR_SQS_QUEUE")
BDA_CONFIGURATION_TABLE = os.environ.get("BDA_CONFIGURATION_TABLE")
DOCUMENT_UPLOAD_TABLE = os.environ.get("DOCUMENT_UPLOAD_TABLE")
EXTRACTED_DOCUMENTS_TABLE = os.environ.get("EXTRACTED_DOCUMENTS_TABLE")
TIMELINE_TABLE = os.environ.get("TIMELINE_TABLE")
EXTRACTED_GRN_TABLE = os.environ.get("EXTRACTED_GRN_TABLE")
EXTRACTED_PO_TABLE = os.environ.get("EXTRACTED_PO_TABLE")
EXTRACTED_REFERRAL_LETTER_TABLE = os.environ.get("EXTRACTED_REFERRAL_LETTER_TABLE", "")
BR_MERCHANT_ID = os.environ.get("BR_MERCHANT_ID")
STEP_FUNCTION_ARN = os.environ.get("STEP_FUNCTION_ARN")
MEDICAL_REFERRAL_LETTER_SQS_QUEUE = os.environ.get("MEDICAL_REFERRAL_LETTER_SQS_QUEUE")
FM_MERCHANT_ID = os.environ.get("FM_MERCHANT_ID")

CHUNK_SIZE = 5

BDA_RUNTIME = boto3.client('bedrock-data-automation-runtime', region_name='us-east-1', endpoint_url=BDA_RUNTIME_ENDPOINT)
S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client("sqs", region_name='us-east-1')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')
STEP_FUNCTION_CLIENT = boto3.client('stepfunctions', region_name='us-east-1')

BDA_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(BDA_CONFIGURATION_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
EXTRACTED_GRN_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_TABLE)
EXTRACTED_PO_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_REFERRAL_LETTER_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_REFERRAL_LETTER_TABLE)

logger = Logger()
tracer = Tracer()

"""
This is function is triggered by S3 bucket file upload to an input folder, then it uses Bedrock Data Automation to extract data from the file and save the result in an output folder.
"""

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        if event.get('retryPollingBDAWaitTime'):
            event = event.get('event')
        
        output_files_list = []
        isRetry = event.get('isRetry', False)

        ## Check if the event is a retry event
        if isRetry:
            retry_count = event.get('retryCount', 0)
            invocation_arn = event.get('invocationArn')
            s3_key = event.get('s3Key')
            document_payload = event.get('documentPayload')
            
            file_name = document_payload.get('fileName')
            documentUploadId = document_payload.get('documentUploadId')
            documentType = document_payload.get('documentType')
            merchantId = document_payload.get('merchantId')
            fileKey = document_payload.get('filePath')
                
            invocation_id, output_json_list, new_retry_count, trigger_retry = retryBdaJobPolling(invocation_arn, s3_key, retry_count, document_payload)

            if not output_json_list and not trigger_retry:
                exceptionStatus = "Processing Timeout"
                create_timeout_documents(documentUploadId, documentType, file_name, merchantId, fileKey, exceptionStatus)
                return {
                    "statusCode": 200,
                    "body": json.dumps("Data extraction process failed.")
                }
            
            elif not output_json_list and trigger_retry:
                logger.info(f'4: {event}')
                startStepFunctionExecution(invocation_arn, s3_key, document_payload, new_retry_count)
                return {
                    "statusCode": 200,
                    "body": json.dumps("Data extraction process retried.")
                }
            
            output_files_list.extend([{
                'file_name': file_name,
                'output_json_list': output_json_list
            }])

            sendToSQS(invocation_id, output_json_list, file_name, fileKey, merchantId, documentUploadId, output_files_list, documentType)
            new_key = fileKey.replace("input/", "processed/")
            moveObject(OUTPUT_BUCKET, fileKey, new_key)


            return {
                "statusCode": 200,
                "body": json.dumps("Data extraction process completed.")
            }

        
        for record in event.get('Records', []):
            logger.info(record)
            bucketName = record.get('s3').get('bucket').get('name')
            fileKey = record.get('s3').get('object').get('key')

            merchantId = fileKey.split('/')[1]           
            documentUploadId = fileKey.split('/')[2]

            documentUpload = getDocumentUpload(documentUploadId)
            documentType = documentUpload.get('documentType')
            
            # Validate if the file is in the input folder
            if not fileKey.startswith("input/"):
                continue
            
            # Validate if the file is a PDF
            if not fileKey.lower().endswith('.pdf'):
                print(f"Skipping non-PDF file: {fileKey}")
                continue

            object_key = urllib.parse.unquote_plus(fileKey)
            object_key = object_key.replace('+', ' ')
            file_name = object_key.split('/')[-1]

            if merchantId == FM_MERCHANT_ID:
                continue

            if merchantId == BR_MERCHANT_ID:
                continue

            if object_key[-3:] == 'pdf':
                bdaConfig = getBdaConfig(merchantId, documentType)
                project_id = bdaConfig.get('projectId')
                profile_id = bdaConfig.get('profileId')
                data_automation_project_arn = f"{DATA_AUTOMATION_PROJECT_ARN}/{project_id}"
                data_automation_profile_arn = f"{DATA_AUTOMATION_PROFILE_ARN}/{profile_id}"
                invocation_arn, invocation_id, output_json_list, retry_count, trigger_retry = extractDataUnified(bucketName, object_key, data_automation_project_arn, data_automation_profile_arn)
                
                if not output_json_list and not trigger_retry:
                    exceptionStatus = "Extraction Failed"
                    create_timeout_documents(documentUploadId, documentType, file_name, merchantId, fileKey, exceptionStatus)
                    continue
                
                ## Check if the output_json_list is empty and trigger a retry
                elif not output_json_list and trigger_retry:
                    document_payload = {
                        'documentUploadId': documentUploadId,
                        'documentType': documentType,
                        'fileName': file_name,
                        'merchantId': merchantId,
                        'filePath': fileKey
                    }
                    startStepFunctionExecution(invocation_arn, object_key, document_payload, retry_count)
                    continue

                
                output_files_list.extend([{
                    'file_name': file_name,
                    'output_json_list': output_json_list
                }])

                sendToSQS(invocation_id, output_json_list, file_name, fileKey, merchantId, documentUploadId, output_files_list, documentType)
                new_key = object_key.replace("input/", "processed/")
                moveObject(bucketName, object_key, new_key)
                    
            else:
                continue

        return {
            "statusCode": 200,
            "body": json.dumps("Data extraction process completed.")
        }
    
    except BadRequestException as ex:
        return {
            'statusCode': 400,
            'body': json.dumps({'status': False, 'message': str(ex)})
        }
    
    except NoCredentialsError:
        return {"statusCode": 500, "body": json.dumps({'status': False, 'message': 'AWS credentials not available'})}
    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {
            'statusCode': 500,
            'body': json.dumps({'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."})
        }
    
@tracer.capture_method
def getDocumentUpload(documentUploadId):
    documentUpload = DOCUMENT_UPLOAD_DDB_TABLE.get_item(
        Key={'documentUploadId': documentUploadId}
    ).get('Item')

    if not documentUpload:
        raise BadRequestException("Document upload not found for the merchant")
    
    return documentUpload

@tracer.capture_method
def createTimeOutExtractedDocument(documentUploadId, source_file_name, merchantId, fileKey, now, exceptionStatus):
    timeoutExtractedDocumentPayload = {
        "extractedDocumentsId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "invoiceNumber": "-",
        "invoiceDate": "-",
        "documentType": "invoice",
        "supplierName": "-",
        "supplierAddress": "-",
        "supplierCode": "-",
        "buyerName": "-",
        "buyerAddress": "-",
        "buyerCode": "-",
        "purchaseOrderNo": "-",
        "paymentTerms": "-",
        "currency": "-",
        "totalInvoiceAmount": 0,
        "taxType": "-",
        "taxRate": "-",
        "taxAmount": 0,
        "dueDate": "-",
        "remarks": "",
        'boundingBoxes': "{}",
        "documentStatus": "Exceptions",
        "exceptionStatus": exceptionStatus,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "sourceFile": source_file_name,
        "filePath": fileKey,
        "confidenceScore": 0,
        "documentUploadId": documentUploadId
    }
    
    EXTRACTED_DOCUMENTS_DDB_TABLE.put_item(
        Item=timeoutExtractedDocumentPayload
    )
    return timeoutExtractedDocumentPayload

@tracer.capture_method
def createTimeOutExtractedGrn(documentUploadId, source_file_name, merchantId, fileKey, now, exceptionStatus):
    timeoutExtractedGrnPayload = {
        "extractedGrnId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "grnNumber": "-",
        "grnDate": "-",
        "documentType": "grn",
        "supplierName": "-",
        "supplierAddress": "-",
        "supplierCode": "-",
        "purchaseOrderNo": "-",
        "statusOfGoodsReceived": "-",
        "remarks": "",
        'boundingBoxes': "{}",
        "documentStatus": "Exceptions",
        "exceptionStatus": exceptionStatus,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "sourceFile": source_file_name,
        "filePath": fileKey,
        "confidenceScore": 0,
        "documentUploadId": documentUploadId
    }
    
    EXTRACTED_GRN_DDB_TABLE.put_item(
        Item=timeoutExtractedGrnPayload
    )
    return timeoutExtractedGrnPayload

@tracer.capture_method
def createTimeOutExtractedPo(documentUploadId, source_file_name, merchantId, fileKey, now, exceptionStatus):
    timeoutExtractedPoPayload = {
        "extractedPoId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "poNumber": "-",
        "poDate": "-",
        "documentType": "po",
        "deliveryAddress": "-",
        "requestDeliveryDate": "-",
        "paymentTerms": "-",
        "supplierName": "-",
        "supplierAddress": "-",
        "supplierCode": "-",
        "buyerName": "-",
        "buyerAddress": "-",
        "buyerCode": "-",
        "currency": "-",
        "totalAmountWithTax": 0,
        "taxType": "-",
        "taxRate": "-",
        "totalTaxAmount": 0,
        "remarks": "",
        'boundingBoxes': "{}",
        "documentStatus": "Exceptions",
        "exceptionStatus": exceptionStatus,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "sourceFile": source_file_name,
        "filePath": fileKey,
        "confidenceScore": 0,
        "documentUploadId": documentUploadId
    }
    
    EXTRACTED_PO_DDB_TABLE.put_item(
        Item=timeoutExtractedPoPayload
    )
    return timeoutExtractedPoPayload

@tracer.capture_method
def createTimeOutExtractedReferralLetter(documentUploadId, source_file_name, merchantId, fileKey, now, exceptionStatus):
    timeoutExtractedReferralLetterPayload = {
        "extractedPoId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "patientDiagnosis": "-",
        "patientEmail": "-",
        "patientIdentificationNumber": "-",
        "patientName": "-",
        "patientPhoneNumber": "-",
        "patientReasonForReferral": "-",
        "preliminaryWorkUps": "-",
        "receivingDoctorDepartment": "-",
        "receivingDoctorEmail": "-",
        "receivingDoctorName": "-",
        "receivingDoctorPhoneNumber": "-",
        "sendingDoctorAddress": "-",
        "sendingDoctorDepartment": "-",
        "sendingDoctorEmail": "-",
        "sendingDoctorName": "-",
        "sendingDoctorPhoneNumber": "-",
        "remarks": "",
        'boundingBoxes': "{}",
        "status": "Exceptions",
        "exceptionStatus": exceptionStatus,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "sourceFile": source_file_name,
        "filePath": fileKey,
        "confidenceScore": 0,
        "confidenceScores": {},
        "documentUploadId": documentUploadId
    }
    
    EXTRACTED_REFERRAL_LETTER_DDB_TABLE.put_item(
        Item=timeoutExtractedReferralLetterPayload
    )
    return timeoutExtractedReferralLetterPayload

@tracer.capture_method
def createTimeoutTimelineRecord(merchantId, invoiceData, now):
    title = "Document Processing Failed"
    description = invoiceData.get('exceptionStatus')
    
    timelinePayload = {
        "timelineId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "timelineForId": invoiceData.get("extractedDocumentsId"),
        "title": title,
        "type": invoiceData.get("documentType", "invoice"),
        "description": description,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "invoiceNumber": invoiceData.get("invoiceNumber", "-"),
        "supplierName": invoiceData.get("supplierName", "-")
    }
    TIMELINE_DDB_TABLE.put_item(Item=timelinePayload)

@tracer.capture_method
def updateDocumentUploadStatus(documentUploadId, status, exceptionStatus):
    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={'documentUploadId': documentUploadId},
        UpdateExpression="SET #Status = :status, exceptionStatus = :exceptionStatus",
        ExpressionAttributeNames={"#Status": "status"},
        ExpressionAttributeValues={
            ':status': status,
            ':exceptionStatus': exceptionStatus
        }
    )
    
@tracer.capture_method
def extractDataUnified(bucket_name, key, data_automation_project_arn, data_automation_profile_arn):
    retry_count = 0
    retry = False
    invocation_arn = startBdaExtractionUnified(bucket_name, key, data_automation_project_arn, data_automation_profile_arn)
    result = pollBdaJobUnified(invocation_arn, key, True)
    final_status = result.get('status')    
    
    if final_status not in ['Success', 'Succeeded']:
        ## Retry Poll for BDA job status if still InProgress
        if final_status in ['InProgress']:
            retry = True
        ## Exit retry if the job status is Failed
        elif final_status in ['Failed']:
            retry = False
            return None, None, [], retry_count, retry

    time.sleep(3)

    invocation_id = invocation_arn.split('/')[-1]
    expected_output_prefix = f"output/{invocation_id}/"
    result_json_list = []
    try:
        response = S3_CLIENT.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=expected_output_prefix)
        if response.get('Contents'):
            response['Contents'] = sorted(response['Contents'], key=lambda x: x['Key'])
            for obj in response.get('Contents'):
                if "custom_output" in obj.get('Key') and obj.get('Key').endswith('result.json'):
                    result_json_list.append(obj.get('Key'))
    
    except Exception as ex:
        logger.exception({"message": str(ex)})

    return invocation_arn, invocation_id, result_json_list, retry_count, retry

@tracer.capture_method
def startBdaExtractionUnified(bucket_name, key, data_automation_project_arn, data_automation_profile_arn):
    input_s3_uri = f"s3://{bucket_name}/{key}"
    output_uri = f"s3://{OUTPUT_BUCKET}/output"
        
    response = BDA_RUNTIME.invoke_data_automation_async(
        dataAutomationConfiguration={"dataAutomationProjectArn": data_automation_project_arn},
        dataAutomationProfileArn=data_automation_profile_arn,
        inputConfiguration={"s3Uri": input_s3_uri},
        outputConfiguration={"s3Uri": output_uri}
    )
    
    invocation_arn = response.get('invocationArn')
    if not invocation_arn:
        raise ClientError(f"Failed to start Bedrock Data Automation extraction for the file '{key}'. No invocationArn was returned in the response: {response}")
    
    return invocation_arn

@tracer.capture_method
def pollBdaJobUnified(invocation_arn, s3_key, retry=False, max_retries=20, delay=3):
    if not invocation_arn:
        raise ClientError(f"Get Data Automation Status was called with an invalid invocation_arn=None for the file: {s3_key}")

    for _ in range(max_retries):
        resp = BDA_RUNTIME.get_data_automation_status(invocationArn=invocation_arn)
        status = resp.get('status')

        if status in ['Success', 'Failed']:
            return resp
        elif status == 'InProgress':
            time.sleep(delay)
        else:
            resp["status"] = "Failed"
            return resp

    # If still InProgress after max_retries
    if not retry:
        resp["status"] = "Failed"
        resp["message"] = "Max retries exceeded"
    return resp

@tracer.capture_method
def moveObject(bucket_name, source_key, destination_key):
    copy_source = {'Bucket': bucket_name, 'Key': source_key}
    S3_CLIENT.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=destination_key)
    S3_CLIENT.delete_object(Bucket=bucket_name, Key=source_key)


@tracer.capture_method
def sendToSQS(invocation_id, result_json_list, file_name, file_key, merchant_id, document_upload_id, output_files_list, document_type):
    payload = {
        'invocation_id': invocation_id,
        'result_json_list': result_json_list,
        'source_file_name': file_name,
        'merchant_id': merchant_id,
        'document_upload_id': document_upload_id,
        'file_path': file_key
    }
    payload = json.dumps(payload)

    

    if document_type == 'invoice':
        if merchant_id == BR_MERCHANT_ID:
            SQS_CLIENT.send_message(
                QueueUrl=BR_SQS_QUEUE,
                MessageBody=payload
            )
            logger.info('Sent message to BR SQS queue')
        else:
            SQS_CLIENT.send_message(
                QueueUrl=SQS_QUEUE,
                MessageBody=payload
            )
            logger.info('Sent message to SQS queue')
    elif document_type == 'grn':
        SQS_CLIENT.send_message(
            QueueUrl=GRN_SQS_QUEUE,
            MessageBody=payload
        )
        logger.info('Sent message to GRN SQS queue')
    elif document_type == 'po':
        SQS_CLIENT.send_message(
            QueueUrl=PO_SQS_QUEUE,
            MessageBody=payload
        )
        logger.info('Sent message to PO SQS queue')
    elif document_type == 'medicalReferralLetter':
        SQS_CLIENT.send_message(
            QueueUrl=MEDICAL_REFERRAL_LETTER_SQS_QUEUE,
            MessageBody=payload
        )
        logger.info('Sent message to Medical SQS queue')



@tracer.capture_method
def getBdaConfig(merchantId, bdaProjectType):
    response = BDA_CONFIGURATION_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchantId),
        FilterExpression=Key('bdaProjectType').eq(bdaProjectType)
    ).get('Items')

    if not response:
        raise BadRequestException("Bedrock Data Automation configuration not found for the merchant")
    
    return response[0]

@tracer.capture_method
def startStepFunctionExecution(invocationArn, s3Key, document_payload, retryCount=0):
    # random from 1-2
    startTime = datetime.now() + timedelta(minutes=random.randint(1, 2))
    payload = {
        'retryPollingBDAWaitTime': startTime.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'event': {
            'isRetry': True,
            'invocationArn': invocationArn,
            's3Key': s3Key,
            'retryCount': retryCount,
            'documentPayload': document_payload
        }
    }
    
    executionId = str(uuid.uuid4())
    STEP_FUNCTION_CLIENT.start_execution(
        stateMachineArn = STEP_FUNCTION_ARN,
        name = executionId,
        input = json.dumps(payload)
    )
    return executionId

@tracer.capture_method
def retryBdaJobPolling(invocation_arn, s3_key, polling_retry_count, document_payload):
    retry = True
    if polling_retry_count > 3:
        retry = False
    
    result = pollBdaJobUnified(invocation_arn, s3_key, retry)
    final_status = result.get('status')
    
    ## Poll for BDA job status if still InProgress
    if final_status in ['InProgress']:
        if retry:
            executionId = startStepFunctionExecution(invocation_arn, s3_key, document_payload, polling_retry_count)
            polling_retry_count += 1
        return None, [], polling_retry_count, retry
    
    ## Exit retry is the job status is Failed
    elif final_status in ['Failed']:
        return None, [], polling_retry_count, False
        
    
    ## Continue if the job status is Success or Succeeded
    time.sleep(3)
    invocation_id = invocation_arn.split('/')[-1]
    expected_output_prefix = f"output/{invocation_id}/"
    result_json_list = []
    try:
        response = S3_CLIENT.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=expected_output_prefix)
        if response.get('Contents'):
            response['Contents'] = sorted(response['Contents'], key=lambda x: x['Key'])
            for obj in response.get('Contents'):
                if "custom_output" in obj.get('Key') and obj.get('Key').endswith('result.json'):
                    result_json_list.append(obj.get('Key'))
    
    except Exception as ex:
        logger.exception({"message": str(ex)})
    return invocation_id, result_json_list, polling_retry_count, False


@tracer.capture_method
def create_timeout_documents(documentUploadId, documentType, file_name, merchantId, fileKey, exceptionStatus):
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    if documentType == 'invoice':
        timeoutExtractedDocument = createTimeOutExtractedDocument(documentUploadId, file_name, merchantId, fileKey, now, exceptionStatus)
        createTimeoutTimelineRecord(merchantId, timeoutExtractedDocument, now)
    elif documentType == 'grn':
        timeoutExtractedDocument = createTimeOutExtractedGrn(documentUploadId, file_name, merchantId, fileKey, now, exceptionStatus)
    elif documentType == 'po':
        timeoutExtractedDocument = createTimeOutExtractedPo(documentUploadId, file_name, merchantId, fileKey, now, exceptionStatus)
    elif documentType == 'medicalReferralLetter':
        timeoutExtractedDocument = createTimeOutExtractedReferralLetter(documentUploadId, file_name, merchantId, fileKey, now, exceptionStatus)

    updateDocumentUploadStatus(documentUploadId, "Fail", exceptionStatus)