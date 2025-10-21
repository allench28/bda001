import boto3
import time
import uuid
import os
import json
import email
import base64
import requests
import pandas as pd
from requests_aws4auth import AWS4Auth
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import ResourceNotFoundException, BadRequestException
from urllib.parse import unquote_plus
from urllib.parse import unquote
from typing import List
from typing import Dict
from bedrock_function import promptBedrock
import re
import extract_msg

S3_BUCKET_NAME = os.environ.get('S3_BUCKET')
EXTRACTED_EMAIL_TABLE = os.environ.get('EXTRACTED_EMAIL_TABLE')
ROUTED_CONTENT_TABLE = os.environ.get('ROUTED_CONTENT_TABLE')
SQS_ANALYSIS_QUEUE_URL = os.environ.get('SQS_ANALYSIS_QUEUE_URL')
SQS_ATTACHMENT_QUEUE_URL = os.environ.get('SQS_ATTACHMENT_QUEUE_URL')
SKILL_MATRIX_TABLE = os.environ.get('SKILL_MATRIX_TABLE')
MODEL_ID = os.environ.get('MODEL_ID')
MERCHANT_ID = os.environ.get('MERCHANT_ID')
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
SNS_ROLE_ARN = os.environ.get("SNS_ROLE_ARN")
ROOT_OUTPUT_PREFIX = os.environ.get("ROOT_OUTPUT_PREFIX")
EXTRACTED_ATTACHMENT_PREFIX = os.environ.get("EXTRACTED_ATTACHMENT_PREFIX")
STEP_FUNCTION_ARN = os.environ.get("STEP_FUNCTION_ARN")

SKILL_MATRIX_FILEKEY = f'presales/input/{MERCHANT_ID}/skill-matrix/SkillMatrix.csv'

SQS_CLIENT = boto3.client('sqs')
S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource("dynamodb")
TEXTRACT_CLIENT = boto3.client("textract")
STEP_FUNCTION = boto3.client('stepfunctions')

EXTRACTED_EMAIL_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_EMAIL_TABLE)
ROUTED_CONTENT_DDB_TABLE = DDB_RESOURCE.Table(ROUTED_CONTENT_TABLE)
SKILL_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(SKILL_MATRIX_TABLE)

logger = Logger()
tracer = Tracer()

SUPPORTED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png'}

@tracer.capture_method
def start_textract_extraction(
    bucket, filepath, tag, sns_topic, sns_role, output_prefix
):  
    response = TEXTRACT_CLIENT.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": filepath}},
        JobTag=tag.replace(" ", "_"),
        NotificationChannel={
            "RoleArn": sns_role,
            "SNSTopicArn": sns_topic,
        },
        OutputConfig={"S3Bucket": bucket, "S3Prefix": output_prefix},
        # f'{output_prefix}/{tag}'
    )
    logger.info(response)

    return response


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        logger.info(event)
        for record in event["Records"]:
            filepath = unquote_plus(
                record["s3"]["object"]["key"]
            )
            filepathPrefix = filepath.split("/")[3]
            sourceFile = filepath.split("/")[4]

            # Ensure file from email bucket
            if filepathPrefix == "email":
                file_ext = filepath.lower().split('.')[-1]
                senderEmail, recipientEmail, ccList, subject, emailBody, emailSentDate, attachments = extractEmailData(
                    S3_BUCKET_NAME, filepath, file_ext)

                logger.info(senderEmail)
                logger.info(recipientEmail)
                logger.info(ccList)
                logger.info(subject)
                logger.info(emailBody)
                logger.info(emailSentDate)
                    
                # Map extracted data to ExtractedEmail table
                extractedEmailId = str(uuid.uuid4())
                now = datetime.now().strftime('%d-%m-%YT%H:%M:%S.%fZ')
                # Map extracted data to ExtractedEmail table
                extracted_email_item = {
                    'extractedEmailId': extractedEmailId,
                    'senderEmail': senderEmail,
                    'recipientEmail': recipientEmail,
                    'subject': subject,
                    'emailSentDate': emailSentDate,
                    'emailBody': emailBody,
                    'ccList': ccList,
                    "sourceFile": sourceFile,
                    "merchantId": MERCHANT_ID,
                    'createdAt': now,
                    'createdBy': "System",
                    'updatedAt': now,
                    'updatedBy': "System",
                }
                EXTRACTED_EMAIL_DDB_TABLE.put_item(Item=extracted_email_item)

                if attachments:
                    response = STEP_FUNCTION.start_execution(
                        stateMachineArn=STEP_FUNCTION_ARN,
                        input=json.dumps({
                            'filepath': filepath,
                            'extractedEmailId': extractedEmailId,
                            'merchantId': MERCHANT_ID
                        })
                    )
                    logger.info("Start step function")
                else:
                    payload = {
                        "filepath": filepath,
                        'extractedEmailId': extractedEmailId,
                        'merchantId': MERCHANT_ID,
                        'triggerType': 'no_attachment'
                    }
                    logger.info("Send to SQS")
                    logger.info(payload)
                    archiveFile(filepath)
                    response = sendToSQS(payload, SQS_ANALYSIS_QUEUE_URL)

    except (ResourceNotFoundException, BadRequestException) as ex:
        if str(ex) == 'Email is not registered as merchant!':
            pass
            # continue
            # return sendErrorMail(senderEmail)
            return {'status': True, 'message': 'Send error email success'}
        else:
            return {
                'status': False,
                'message': str(ex)
            }
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}

@tracer.capture_method
def extractEmailData(S3_BUCKET_NAME, fileKey, file_ext):
    """
    Extracts sender, recipient, cc, subject, body, sent date, and attachments from an .eml or .msg file in S3.
    Returns: senderEmail, recipientEmail, ccList, subject, emailBody, emailSentDate, attachments
    """
    # Download the email file from S3
    response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey)
    emailRawBytes = response['Body'].read()

    senderEmail = recipientEmail = subject = emailBody = emailSentDate = None
    ccList = []
    attachments = []

    def extract_email(addr):
        # Use re.search to extract email inside <...>, else return as is
        if addr:
            match = re.search(r'<(.*?)>', addr)
            return match.group(1) if match else addr
        return None

    def extract_cc_list(cc_field):
        if cc_field:
            return [extract_email(cc.strip()) for cc in re.split(r';|,', cc_field)]
        return []

    if file_ext.lower() == 'msg':
        temp_path = '/tmp/temp_email.msg'
        with open(temp_path, 'wb') as temp_file:
            temp_file.write(emailRawBytes)

        msg = extract_msg.Message(temp_path)
        senderEmail = extract_email(msg.sender)
        recipientEmail = extract_email(msg.to)
        ccList = extract_cc_list(msg.cc)
        subject = msg.subject
        emailBody = msg.body
        emailSentDate = msg.date.isoformat()

        for att in msg.attachments:
            attachments.append({
                'filename': att.longFilename or att.shortFilename,
                'content': att.data
            })
            break

    elif file_ext.lower() == 'eml':
        msg = BytesParser(policy=policy.default).parsebytes(emailRawBytes)
        senderEmail = extract_email(msg.get('From'))
        recipientEmail = extract_email(msg.get('To'))
        ccList = extract_cc_list(msg.get('Cc'))
        subject = msg.get('Subject')
        emailSentDate = msg.get('Date').isoformat()

        # Prefer plain text body, fallback to HTML
        emailBody = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain" and part.get_content_disposition() != "attachment":
                    emailBody = part.get_content()
                    break
            if not emailBody:
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        emailBody = part.get_content()
                        break
        else:
            emailBody = msg.get_content()

        for part in msg.iter_attachments():
            if part.get_content_disposition() == 'attachment':
                attachments.append({
                    'filename': part.get_filename(),
                    'content': part.get_payload(decode=True)
                })
                break

    return senderEmail, recipientEmail, ccList, subject, emailBody, emailSentDate, attachments

@tracer.capture_method
def sendToSQS(payload, sqs):
    payloadJson = json.dumps(payload, default=decimalDefault)
    response = SQS_CLIENT.send_message(
        QueueUrl=sqs,
        MessageBody=payloadJson
    )
    return response


@tracer.capture_method
def decimalDefault(obj):
    """Helper function for JSON serialization of Decimal types"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


@tracer.capture_method
def archiveFile(pathKey: str):
    """
    Archive the file by copying it to the archive folder in S3
    """
    # key = pathKey.replace('+', ' ')
    copy_source = {
        'Bucket': S3_BUCKET_NAME,
        'Key': pathKey
    }
    newKey = pathKey.replace(f"input/", "archive/")
    S3_CLIENT.copy_object(Bucket=S3_BUCKET_NAME,
                          CopySource=copy_source, Key=newKey)
    S3_CLIENT.delete_object(Bucket=S3_BUCKET_NAME, Key=pathKey)
