import boto3
import time
import uuid
import os
import io
import zipfile
import sys
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
from bedrock_function import promptBedrock, checkCompanyImageFooter
import re
import extract_msg


S3_BUCKET_NAME = os.environ.get('S3_BUCKET')
EXTRACTED_EMAIL_TABLE = os.environ.get('EXTRACTED_EMAIL_TABLE')
EXTRACTED_ATTACHMENT_PREFIX = os.environ.get('EXTRACTED_ATTACHMENT_PREFIX')
ROUTED_CONTENT_TABLE = os.environ.get('ROUTED_CONTENT_TABLE')
SQS_ANALYSIS_QUEUE_URL = os.environ.get('SQS_ANALYSIS_QUEUE_URL')
SKILL_MATRIX_TABLE = os.environ.get('SKILL_MATRIX_TABLE')
MODEL_ID = os.environ.get('MODEL_ID')
MERCHANT_ID = os.environ.get('MERCHANT_ID')
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
SNS_ROLE_ARN = os.environ.get("SNS_ROLE_ARN")
ROOT_OUTPUT_PREFIX = os.environ.get("ROOT_OUTPUT_PREFIX")

SKILL_MATRIX_FILEKEY = f'presales/input/{MERCHANT_ID}/skill-matrix/SkillMatrix.csv'

SQS_CLIENT = boto3.client('sqs')
S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource("dynamodb")
TEXTRACT_CLIENT = boto3.client("textract")

EXTRACTED_EMAIL_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_EMAIL_TABLE)
ROUTED_CONTENT_DDB_TABLE = DDB_RESOURCE.Table(ROUTED_CONTENT_TABLE)
SKILL_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(SKILL_MATRIX_TABLE)

logger = Logger()
tracer = Tracer()

IMG_EXTENSIONS = {'jpg', 'jpeg', 'png'}
PDF_EXTENSIONS = {'pdf'}
DOC_EXTENSIONS = {'docx', 'doc'}
SUPPORTED_EXTENSIONS = {'jpg', 'jpeg', 'png', 'pdf'}

ATTACHMENT_PROMPT = """
TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    1. Analysis Report (Your analysis on the attachment)

2. You are provided:
    a. An attachment content in {email_input} for analysis

3. Standardize and clean the data:
    - Standardize email content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

ANALYSIS TASK
    1. Analyze the attachment to identify the mentioned products, brands, and price amount. 

INPUT:
<email_input>
{email_input}
</email_input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "analysisReport": "The attachment shows that...."
}}

"""

CHECK_RELEVANT_IMG_PROMPT = """
TASK:
1. Analyze the email content in the input JSON Object and determine the following key information:
    1. Analysis Report (Your analysis on the attachment)
    2. Relevant (Whether the image provides relevant information)

2. You are provided:
    a. An attachment content for analysis

3. Standardize and clean the data:
    - Standardize email content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

ANALYSIS TASK
    1. Analyze the attachment to extract any mentioned products, brands, price amount, and end user information.
    2. Irrelevant image are typically found at the bottom of corporate emails such as examples below: 
        - Company footer image 
        - Logo image 
        - Human portrait
        - Legal disclaimer
        - Important notice 
        - Banner image
        - Advertisement
    3. Assess whether the image appears to be an irrelevant image and return isRelevant as false ONLY IF the image is appeared to be irrelevant
    4. Clearly state whether the image is relevant or not
    5. Explain your reasoning for the classification (e.g., presence of logos only, banner, advertisment, absence of product information, typical footer patterns).

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "isRelevant": True or False
    "analysisReport": "The attachment shows that...."
}}
"""

# CHECK_COMPANY_FOOTER_PROMPT = """
# TASK:
# 1. Analyze the email content in the input JSON Object and determine the following key information:
#     1. Analysis Report (Your analysis on the attachment)
#     2. Footer (Whether the image is a company footer image)

# 2. You are provided:
#     a. An attachment content in {att_input} for analysis

# 3. Standardize and clean the data:
#     - Standardize email content: remove extra spaces, normalize casing and formatting.
#     - For missing values, use reasonable defaults or indicate the missing data in your analysis.
#     - Use fuzzy matching to handle near-identical descriptions or naming variations.

# ANALYSIS TASK
#     1. Analyze the attachment to identify the mentioned products, brands, and price amount.
#     2. If the attachment is an image, determine if it is a company footer image (such as a logo, legal disclaimer, or standard footer graphic commonly found at the bottom of company emails). Clearly state in your analysis if the image appears to be a company footer image or not, and explain your reasoning.

# INPUT:
# <att_input>
# {att_input}
# </att_input>

# OUTPUT FORMAT:
# Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
# DO NOT remove any keys from the input JSON object:
# {{
#     "footer": True or False
#     "analysisReport": "The attachment shows that...."
# }}
# """

@tracer.capture_method
def create_analyze_attachment_prompt(document_text):

    prompt = ATTACHMENT_PROMPT.format(
        email_input=json.dumps(document_text, default=str),
    )
    result, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result = clean_analysisResult(result)
    return cleaned_result, input_tokens, output_tokens

@tracer.capture_method
def check_company_footer_prompt(attachment, file_ext):

    prompt = CHECK_RELEVANT_IMG_PROMPT.format(
        # att_input=json.dumps(attachment, default=str),
    )
    result, input_tokens, output_tokens = checkCompanyImageFooter(prompt, attachment, file_ext)
    # logger.info(result)
    cleaned_result = clean_analysisResult(result)
    return cleaned_result #, input_tokens, output_tokens

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        logger.info(event)
        job_ids = []
        valid_attachments_for_analysis = []
        textract_attachments = []

        filepath = event['filepath']
        file_ext = filepath.lower().split('.')[-1]
        extractedEmailId = event['extractedEmailId']

        print(filepath)
        sourceFile = filepath.split("/")[4]
        sourceFilePath = sourceFile.rsplit('.', 1)[0]
        attachments = extractAttachment(S3_BUCKET_NAME, filepath, file_ext)

        extracted_attachment = f'{EXTRACTED_ATTACHMENT_PREFIX}/{sourceFilePath}'

        for attachment in attachments:
            filename = attachment['filename']
            content = attachment['content']
            logger.info(filename)

            file_ext = os.path.splitext(filename)[1][1:].lower()
            print('file_extension:', file_ext)

            if file_ext == 'zip':
                print(f"Unzipping {filename}...")
            
                zip_bytes = io.BytesIO(content)
                with zipfile.ZipFile(zip_bytes) as zipf:
                    for inner_file in zipf.namelist():
                        if zipf.getinfo(inner_file).is_dir():
                            continue  # skip folders

                        inner_ext = os.path.splitext(inner_file)[1][1:].lower()
                        print(f"Inner file: {inner_file}, ext: {inner_ext}")

                        if inner_ext not in SUPPORTED_EXTENSIONS:
                            print(f'{inner_file} in ZIP is not supported')
                            continue

                        inner_content = zipf.read(inner_file)

                        if inner_ext in IMG_EXTENSIONS:
                            check_result = check_company_footer_prompt(inner_content, inner_ext)
                            logger.info(check_result)
                            isRelevant = check_result.get('isRelevant', '')
                            if not isRelevant:
                                print(f"Image relevance in ZIP: {isRelevant}")
                                continue

                        valid_attachments_for_analysis.append({
                            'filename': inner_file,
                            'content': inner_content
                        })

                        file_key = f"{extracted_attachment}/{inner_file}"
                        S3_CLIENT.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=inner_content)
                        print(f'{S3_BUCKET_NAME}/{file_key}')
                        continue

            if file_ext not in SUPPORTED_EXTENSIONS:
                print(f"{filename} is not supported")
                continue
                
            # Regular file (not zip)
            if file_ext in IMG_EXTENSIONS:
                check_result = check_company_footer_prompt(content, file_ext)
                logger.info(check_result)
                isRelevant = check_result.get('isRelevant', '')
                if not isRelevant:
                    print(f'Image relevance is: {isRelevant}')
                    continue

            valid_attachments_for_analysis.append(attachment)

            file_key = f"{extracted_attachment}/{filename}"
            S3_CLIENT.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=content)
            print(f'{S3_BUCKET_NAME}/{file_key}')


        if valid_attachments_for_analysis:
            for att in valid_attachments_for_analysis:
                filename = att['filename']
                print('VALID:', att['filename'])
                attachment_file_ext = os.path.splitext(filename)[1][1:].lower()

                filename_standard = filename.replace(" ", "_")
                attachment_prefix = f"{extracted_attachment}/{filename}"
                output_prefix = f"{ROOT_OUTPUT_PREFIX}/{sourceFile}/{filename_standard}"

            # if attachment_file_ext in PDF_EXTENSIONS or attachment_file_ext in IMG_EXTENSIONS:
                response = start_textract_extraction(
                    S3_BUCKET_NAME,
                    attachment_prefix,
                    output_prefix,
                )

                print(response['JobId'])

                textract_attachments.append({
                    "filepath": filepath,
                    'document': output_prefix,
                    'jobId': response['JobId'],
                    'extractedEmailId': extractedEmailId
                })
            # print(textract_attachments)

            # Update DynamoDB if there are valid attachments
            if textract_attachments:
                ddb_response = EXTRACTED_EMAIL_DDB_TABLE.update_item(
                    Key={'extractedEmailId': extractedEmailId},
                    UpdateExpression='SET AnalysisResult = :op',
                    ExpressionAttributeValues={':op': textract_attachments}  
                )
                print(f"Payload size: {sys.getsizeof(textract_attachments)} bytes")
                return textract_attachments
        else:
            payload = {
                "filepath": filepath,
                "extractedEmailId": extractedEmailId,
                "merchantId": MERCHANT_ID
            }
            logger.info("Send to SQS")
            response = sendToSQS(payload)
            
    except (ResourceNotFoundException, BadRequestException) as ex:
        if str(ex) == 'Email is not registered as merchant!':
            pass
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
def start_textract_extraction(
    bucket, filepath, output_prefix
):
    response = TEXTRACT_CLIENT.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": filepath}},
        # JobTag=tag,
        # NotificationChannel={
        #     "RoleArn": sns_role,
        #     "SNSTopicArn": sns_topic,
        # },
        OutputConfig={"S3Bucket": bucket, "S3Prefix": output_prefix},
    )
    logger.info(response)

    return response

@tracer.capture_method
def process_textract_output(file_content):
    # file_content_str = file_content.decode("utf-8")
    # textract_result = json.loads(file_content_str)

    document_lines = []

    for block in file_content["Blocks"]:
        text = block.get("Text")
        if isinstance(text, str) and text.strip():
            document_lines.append(text)

    # document_text = "\n".join(document_lines)
    document_text = " ".join(document_lines)

    return document_text

@tracer.capture_method
def extractAttachment(S3_BUCKET_NAME, fileKey, file_ext):
    """
    Extracts sender, recipient, cc, subject, body, sent date, and attachments from an .eml or .msg file in S3.
    Returns: senderEmail, recipientEmail, ccList, subject, emailBody, emailSentDate, attachments
    """
    # Download the email file from S3
    response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey)
    emailRawBytes = response['Body'].read()

    attachments = []
    if file_ext.lower() == 'msg':
        temp_path = '/tmp/temp_email.msg'
        with open(temp_path, 'wb') as temp_file:
            temp_file.write(emailRawBytes)

        msg = extract_msg.Message(temp_path)
        for att in msg.attachments:
            attachments.append({
                'filename': att.longFilename or att.shortFilename,
                'content': att.data
            })

    elif file_ext.lower() == 'eml':
        msg = BytesParser(policy=policy.default).parsebytes(emailRawBytes)
        for part in msg.iter_attachments():
            if part.get_content_disposition() == 'attachment':
                attachments.append({
                    'filename': part.get_filename(),
                    'content': part.get_payload(decode=True)
                })

    return attachments

@tracer.capture_method
def sendToSQS(payload):
    payloadJson = json.dumps(payload, default=decimalDefault)
    response = SQS_CLIENT.send_message(
        QueueUrl=SQS_ANALYSIS_QUEUE_URL,
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
def clean_analysisResult(analysis_data) -> Dict:
    try:
        json_patterns = [
            r'```(?:json)?\s*([\s\S]*?)\s*```',  # group(1) is the content
        ]

        json_str = None
        for pattern in json_patterns:
            json_match = re.search(pattern, analysis_data)
            if json_match:
                # Use group(1) for the first pattern, group(0) for the second
                if pattern.startswith('```'):
                    json_str = json_match.group(1)
                    json_str = json_str.strip()
                    # Remove leading 'json' if present
                    json_str = re.sub(r'^\s*json\s*', '',
                                      json_str, flags=re.IGNORECASE)
                else:
                    json_str = json_match.group(0)
                if json_str:
                    break

        if not json_str:
            start_idx = analysis_data.find('{')
            end_idx = analysis_data.rfind('}') + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = analysis_data[start_idx:end_idx]
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
            else:
                logger.exception(
                    {"message": "Could not locate valid JSON content by brackets"})

        if json_str:
            try:
                # logger.info(json_str)
                analysis_data = sanitizeAndParseJson(json_str)
                return analysis_data
            except json.JSONDecodeError as je:
                logger.exception({"message": f"JSON parsing error: {str(je)}"})

        fallback_response = constructFallbackResponse(analysis_data)
        return fallback_response

    except Exception as e:
        logger.exception(
            {"message": f"Exception in clean_analysisResult: {str(e)}"})
        return constructFallbackResponse(analysis_data)


@tracer.capture_method
def sanitizeAndParseJson(json_str):
    try:
        # First attempt to parse as is
        return json.loads(json_str)
    except json.JSONDecodeError:
        # If it fails, try to fix common issues

        # 1. Replace newlines in string values
        # This regex finds strings inside quotes and replaces newlines with spaces
        pattern = r'("(?:\\.|[^"\\])*")'

        def replace_newlines(match):
            return match.group(0).replace('\n', ' ')

        sanitized_str = re.sub(pattern, replace_newlines, json_str)

        # 2. Remove trailing commas in objects and arrays
        sanitized_str = re.sub(r',\s*}', '}', sanitized_str)
        sanitized_str = re.sub(r',\s*\]', ']', sanitized_str)

        try:
            # Try parsing the sanitized string
            return json.loads(sanitized_str)
        except json.JSONDecodeError as e:
            # If still failing, try a more brute force approach
            # Remove all newlines and excess whitespace
            compressed_str = re.sub(r'\s+', ' ', json_str).strip()

            try:
                return json.loads(compressed_str)
            except json.JSONDecodeError:
                # If all else fails, provide a more helpful error message
                raise ValueError(
                    f"Could not parse JSON even after sanitization. Original error: {str(e)}")

@tracer.capture_method
def constructFallbackResponse(result: str) -> Dict:
    """Construct fallback response when parsing fails"""
    return result

"""
# job_id = message_data.get('JobId')
# job_tag = message_data.get('JobTag')
# s3_bucket = message_data.get('DocumentLocation', {}).get('S3Bucket')

# output_key = f"{ROOT_OUTPUT_PREFIX}/{job_tag}/{job_id}/"
# extracted_attachment_content = []
# # List all objects in the output folder
# response = S3_CLIENT.list_objects_v2(Bucket=s3_bucket, Prefix=output_key)
# for obj in response.get('Contents', []):
#     key = obj['Key']
#     # Skip .s3_access_check and any non-numbered files
#     if key.endswith('.s3_access_check'):
#         continue
#     filename = os.path.basename(key)
#     if not filename.isdigit():
#         continue

#     # Fetch and decode the Textract output file
#     file_obj = S3_CLIENT.get_object(Bucket=s3_bucket, Key=key)
#     body_bytes = file_obj['Body'].read()
#     body_str = body_bytes.decode('utf-8')
#     textract_data = json.loads(body_str)
#     extracted_attachment_text = process_textract_output(textract_data)
#     extracted_attachment_content.extend(extracted_attachment_text)

# # Join all lines with line breaks to preserve structure
# full_text = "\n".join(extracted_attachment_content)


# Now pass full_text to Bedrock
# result, input_tokens, output_tokens = create_determine_product_and_brand_prompt(full_text, DATA_CENTER_BRAND_MAPPING, SOFTWARE_BRAND_MAPPING, EN_N_COLLABS_BRAND_MAPPING, SECURITY_BRAND_MAPPING, CISCO_SECURE_PORTFOLIO_BRAND_MAPPING)
# print(result)
# print(input_tokens)
# print(output_tokens)

"""
"""
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        logger.info(event)
        job_ids = []
        textract_attachments = []

        for record in event['Records']:
            body = record['body']
            body_content = json.loads(body)
            filepath = body_content['filepath']
            file_ext = filepath.lower().split('.')[-1]
            extractedEmailId = body_content['extractedEmailId']

            print(filepath)
            sourceFile = filepath.split("/")[4]
            sourceFilePath = sourceFile.rsplit('.', 1)[0]
            attachments = extractAttachment(S3_BUCKET_NAME, filepath, file_ext)

            extracted_attachment = f'{EXTRACTED_ATTACHMENT_PREFIX}/{sourceFilePath}'

            for attachment in attachments:
                logger.info(attachment['filename'])  
                file_key = f"{extracted_attachment}/{attachment['filename']}"
                S3_CLIENT.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=attachment['content']) 
                print(f'{S3_BUCKET_NAME}/{file_key}')

            for att in attachments:
                filename = att['filename']
                filename_standard = filename.replace(" ", "_")
                attachment_prefix = f"{extracted_attachment}/{filename}"
                output_prefix = f"{ROOT_OUTPUT_PREFIX}/{filename_standard}"

                response = start_textract_extraction(
                    S3_BUCKET_NAME,
                    attachment_prefix,
                    output_prefix,
                )

                textract_attachments.append(output_prefix)
                job_ids.append(response['JobId'])

            # Update DynamoDB
            ddb_response = EXTRACTED_EMAIL_DDB_TABLE.update_item(
                Key={'extractedEmailId': extractedEmailId},
                UpdateExpression='SET AnalysisResult = :op',
                ExpressionAttributeValues={':op': textract_attachments}  
            )
            logger.info(ddb_response)

        return {
            'JobIds': job_ids,
            'bucket': S3_BUCKET_NAME,
            'documents': textract_attachments,
        }
"""