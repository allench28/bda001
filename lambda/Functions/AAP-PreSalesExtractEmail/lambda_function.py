import boto3
import time
import uuid
import os
import json
# import docx
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
import mapping_data
import lambda_prompt
import re
import extract_msg

S3_BUCKET_NAME = os.environ.get('S3_BUCKET')
EXTRACTED_EMAIL_TABLE = os.environ.get('EXTRACTED_EMAIL_TABLE')
EMAIL_ANALYSIS_RESULT_TABLE = os.environ.get('EMAIL_ANALYSIS_RESULT_TABLE')
ROUTED_CONTENT_TABLE = os.environ.get('ROUTED_CONTENT_TABLE')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
SKILL_MATRIX_TABLE = os.environ.get('SKILL_MATRIX_TABLE')
MODEL_ID = os.environ.get('MODEL_ID')
MERCHANT_ID = os.environ.get('MERCHANT_ID')

SKILL_MATRIX_FILEKEY = f'presales/input/{MERCHANT_ID}/skill-matrix/SkillMatrix.csv'

SQS_CLIENT = boto3.client('sqs')
S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource("dynamodb")

EXTRACTED_EMAIL_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_EMAIL_TABLE)
SKILL_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(SKILL_MATRIX_TABLE)
ROUTED_CONTENT_DDB_TABLE = DDB_RESOURCE.Table(ROUTED_CONTENT_TABLE)
EMAIL_ANALYSIS_RESULT_DDB_TABLE = DDB_RESOURCE.Table(EMAIL_ANALYSIS_RESULT_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_method
def bedrock_analyze_team_vendor_prompt(email_body_brand_input, attachment_brand_input, team):

    prompt = lambda_prompt.ANALYZE_TEAM_VENDOR_PROMPT.format(
        team=json.dumps(team, default=str),
        brandInput=json.dumps(email_body_brand_input, default=str),
        attachmentBrandInput=json.dumps(attachment_brand_input, default=str)
    )
    result, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result = clean_analysisResult(result)
    return cleaned_result


@tracer.capture_method
def bedrock_analyze_product_supp_ku_prompt(analysis_input, input_type):

    if input_type == 'attachment':
        prompt = lambda_prompt.ANALYZE_ATTACHMENT_PRODUCT_SUPP_KU_PROMPT.format(
            att_input=json.dumps(analysis_input, default=str)
        )
        result, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result = clean_analysisResult(result)
        return cleaned_result
    else:
        prompt = lambda_prompt.ANALYZE_EMAILBODY_PRODUCT_SUPP_KU_PROMPT.format(
            att_input=json.dumps(analysis_input, default=str)
        )
        result, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result = clean_analysisResult(result)
        return cleaned_result

@tracer.capture_method
def bedrock_analyze_product_brand_prompt(analysis_input, input_type, data_center, software, enCollabs, security, cisco):

    if input_type == 'attachment':
        prompt1 = lambda_prompt.ANALYZE_ATTACHMENT_PRODUCT_BRAND_PROMPT.format(
            dataCenterBrandMapping=json.dumps(data_center, default=str),
            softwareBrandMapping=json.dumps(software, default=str),
            enCollabsBrandMapping=json.dumps(enCollabs, default=str),
            securityBrandMapping=json.dumps(security, default=str),
            ciscoSecurePortfolioBrandMapping=json.dumps(cisco, default=str),
            att_input=json.dumps(analysis_input, default=str)
        )
        prompt2 = lambda_prompt.ANALYZE_ATTACHMENT_PRODUCT_SUPP_KU_PROMPT.format(
            att_input=json.dumps(analysis_input, default=str)
        )
        result1, input_tokens, output_tokens = promptBedrock(prompt1)
        result2, input_tokens, output_tokens = promptBedrock(prompt2)
        cleaned_result1 = clean_analysisResult(result1)
        cleaned_result2 = clean_analysisResult(result2)
        cleaned_result = cleaned_result1 | cleaned_result2
        return cleaned_result
    else:
        prompt1 = lambda_prompt.ANALYZE_EMAILBODY_PRODUCT_BRAND_PROMPT.format(
            dataCenterBrandMapping=json.dumps(data_center, default=str),
            softwareBrandMapping=json.dumps(software, default=str),
            enCollabsBrandMapping=json.dumps(enCollabs, default=str),
            securityBrandMapping=json.dumps(security, default=str),
            ciscoSecurePortfolioBrandMapping=json.dumps(cisco, default=str),
            email_input=json.dumps(analysis_input, default=str)
        )
        prompt2 = lambda_prompt.ANALYZE_EMAILBODY_PRODUCT_SUPP_KU_PROMPT.format(
            email_input=json.dumps(analysis_input, default=str)
        )
        result1, input_tokens, output_tokens = promptBedrock(prompt1)
        result2, input_tokens, output_tokens = promptBedrock(prompt2)
        cleaned_result1 = clean_analysisResult(result1)
        cleaned_result2 = clean_analysisResult(result2)
        cleaned_result = cleaned_result1 | cleaned_result2
        return cleaned_result

@tracer.capture_method
def bedrock_analyze_generic_prompt(analysis_input, department):

    if department == 'Astar':
        prompt = lambda_prompt.ANALYZE_GENERIC_PROMPT.format(
            dataCenterBrandMapping=json.dumps(mapping_data.ASTAR_DATA_CENTER, default=str),
            softwareBrandMapping=json.dumps(mapping_data.ASTAR_SOFTWARE, default=str),
            enCollabsBrandMapping=json.dumps(mapping_data.ASTAR_EN_COLLABS, default=str),
            securityBrandMapping=json.dumps(mapping_data.ASTAR_SECURITY, default=str),
            att_input=json.dumps(analysis_input, default=str)
        )
    elif department == 'Pericomp':
        prompt = lambda_prompt.ANALYZE_GENERIC_PROMPT.format(
            dataCenterBrandMapping=json.dumps(mapping_data.PERICOMP_DATA_CENTER, default=str),
            softwareBrandMapping=json.dumps(mapping_data.PERICOMP_SOFTWARE, default=str),
            enCollabsBrandMapping=json.dumps(mapping_data.PERICOMP_EN_COLLABS, default=str),
            securityBrandMapping=json.dumps(mapping_data.PERICOMP_SECURITY, default=str),
            att_input=json.dumps(analysis_input, default=str)
        )
        result, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result = clean_analysisResult(result)
        return cleaned_result
    else:
        prompt1 = lambda_prompt.ANALYZE_EMAILBODY_PRODUCT_BRAND_PROMPT.format(
            dataCenterBrandMapping=json.dumps(mapping_data.PERICOMP_DATA_CENTER, default=str),
            softwareBrandMapping=json.dumps(mapping_data.SOFTWARE_BRAND_MAPPING, default=str),
            enCollabsBrandMapping=json.dumps(mapping_data.EN_N_COLLABS_BRAND_MAPPING, default=str),
            securityBrandMapping=json.dumps(mapping_data.SECURITY_BRAND_MAPPING, default=str),
            ciscoSecurePortfolioBrandMapping=json.dumps(mapping_data.CISCO_SECURE_PORTFOLIO_BRAND_MAPPING, default=str),
            email_input=json.dumps(analysis_input, default=str)
        )
        prompt2 = lambda_prompt.ANALYZE_EMAILBODY_PRODUCT_SUPP_KU_PROMPT.format(
            email_input=json.dumps(analysis_input, default=str)
        )
        result1, input_tokens, output_tokens = promptBedrock(prompt1)
        result2, input_tokens, output_tokens = promptBedrock(prompt2)
        cleaned_result1 = clean_analysisResult(result1)
        cleaned_result2 = clean_analysisResult(result2)
        cleaned_result = cleaned_result1 | cleaned_result2
        return cleaned_result
# @tracer.capture_method
# def bedrock_analyze_product_brand_prompt(analysis_input, input_type, data_center, software, enCollabs, security, cisco):

#     if input_type == 'attachment':
#         prompt = lambda_prompt.ANALYZE_ATTACHMENT_PRODUCT_BRAND_PROMPT.format(
#             dataCenterBrandMapping=json.dumps(data_center, default=str),
#             softwareBrandMapping=json.dumps(software, default=str),
#             enCollabsBrandMapping=json.dumps(enCollabs, default=str),
#             securityBrandMapping=json.dumps(security, default=str),
#             ciscoSecurePortfolioBrandMapping=json.dumps(cisco, default=str),
#             att_input=json.dumps(analysis_input, default=str)
#         )
#         result, input_tokens, output_tokens = promptBedrock(prompt)
#         cleaned_result = clean_analysisResult(result)
#         return cleaned_result
#     else:
#         prompt = lambda_prompt.ANALYZE_EMAILBODY_PRODUCT_BRAND_PROMPT.format(
#             dataCenterBrandMapping=json.dumps(data_center, default=str),
#             softwareBrandMapping=json.dumps(software, default=str),
#             enCollabsBrandMapping=json.dumps(enCollabs, default=str),
#             securityBrandMapping=json.dumps(security, default=str),
#             ciscoSecurePortfolioBrandMapping=json.dumps(cisco, default=str),
#             email_input=json.dumps(analysis_input, default=str)
#         )
#         result, input_tokens, output_tokens = promptBedrock(prompt)
#         cleaned_result = clean_analysisResult(result)
#         return cleaned_result

@tracer.capture_method
def bedrock_analyze_entity_prompt(analysis_input, input_type):

    if input_type == 'attachment':
        prompt = lambda_prompt.ANALYZE_ATTACHMENT_EU_INDUSTRY_PROMPT.format(
            att_input=json.dumps(analysis_input, default=str)
        )
        result1, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result1 = clean_analysisResult(result1)
        return cleaned_result1
    
    else:
        prompt = lambda_prompt.ANALYZE_EMAILBODY_EU_INDUSTRY_PROMPT.format(
            email_input=json.dumps(analysis_input, default=str)
        )
        result1, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result1 = clean_analysisResult(result1)

        prompt = lambda_prompt.ANALYZE_EMAILBODY_RESELLER_PROMPT.format(
            email_input=json.dumps(analysis_input, default=str)
        )
        result2, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result2 = clean_analysisResult(result2)
        return cleaned_result1, cleaned_result2


@tracer.capture_method
def bedrock_analyze_subject_tender(analysis_input, input_type, subject, emailSentDate):

    if input_type == 'attachment':
        prompt = lambda_prompt.ANALYZE_ATTACHMENT_SUBJECT_TENDER_PROMPT.format(
            att_input=json.dumps(analysis_input, default=str)
        )
        result1, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result = clean_analysisResult(result1)
        return cleaned_result
    else:
        prompt = lambda_prompt.ANALYZE_EMAILBODY_SUBJECT_TENDER_PROMPT.format(
            email_input=json.dumps(analysis_input, default=str),
            subject=json.dumps(subject, default=str),
            emailSentDate=json.dumps(emailSentDate, default=str)
        )
        result, input_tokens, output_tokens = promptBedrock(prompt)
        cleaned_result = clean_analysisResult(result)
        return cleaned_result


@tracer.capture_method
def consolidation_prompt(productBrands, attachmentProductBrands, endUser, reseller, attachmentTender, subjectTender, attachmentEndUser):

    prompt = lambda_prompt.CONSOLIDATION_PROMPT.format(
        productBrands=json.dumps(productBrands, default=str),
        attachmentProductBrands=json.dumps(attachmentProductBrands, default=str),
        endUser=json.dumps(endUser, default=str),
        reseller=json.dumps(reseller, default=str),
        attachmentTender=json.dumps(attachmentTender, default=str),
        subjectTender=json.dumps(subjectTender, default=str),
        attachmentEndUser=json.dumps(attachmentEndUser, default=str)
    )
    result, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result = clean_analysisResult(result)

    return cleaned_result

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
def queryExtractedEmailById(extractedEmailId):
    """
    Query the DynamoDB table using the extractedEmailId.
    """
    try:
        response = EXTRACTED_EMAIL_DDB_TABLE.get_item(
            Key={'extractedEmailId': extractedEmailId}  # Directly use the string
        )
        return response.get('Item')

    except Exception as ex:
        logger.exception({"message": str(ex)})
        raise BadRequestException(
            f"Failed to query extractedEmailId: {extractedEmailIds}. Error: {str(ex)}")

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # skillMatrixData = skillMatrixMapping(SKILL_MATRIX_FILEKEY)
        # storeSkillMatrixInDB(skillMatrixData)
        extractedEmailIds = []
        extracted_attachment_content = []
        full_extracted_attachment_text = []
        all_processed_items = []
        finalProductBrand = {}
        logger.info(event)
        dc_mapping = mapping_data.DATA_CENTER_BRAND_MAPPING
        software_mapping = mapping_data.SOFTWARE_BRAND_MAPPING 
        en_collabs_mapping = mapping_data.EN_N_COLLABS_BRAND_MAPPING
        security_mapping = mapping_data.SECURITY_BRAND_MAPPING
        cisco_mapping = mapping_data.CISCO_SECURE_PORTFOLIO_BRAND_MAPPING
        team_mapping = mapping_data.TEAM_BRAND_MAPPING

        # Attachment data exists
        if isinstance(event, list):
            print("Found Email with Attachment")
            if event[0]['Processed'][0]['triggerType'] == 'attachment':
                extractedEmailId = event[0]['Processed'][0]['extractedEmailId']
                filepath = event[0]['Processed'][0]['filepath']
                sourceFile = filepath.split('/')[4]
                for msg in event:
                    if "Processed" in msg:
                        # Extend our master list with all items from this "Processed" list
                        all_processed_items.extend(msg["Processed"])
                    # items = msg['Processed']
                for item in all_processed_items:
                    logger.info(item['document'])
                    filePath = item['document'] + '/' + item['jobId']
                    response = S3_CLIENT.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=filePath)
                    for obj in response.get('Contents', []):
                        key = obj['Key']
                        if key.endswith('.s3_access_check'):
                            continue
                        filename = os.path.basename(key)
                        logger.info(f'{filePath}/{key}')
                        # Fetch and decode the Textract output file
                        file_obj = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                        body_bytes = file_obj['Body'].read()
                        body_str = body_bytes.decode('utf-8')
                        textract_data = json.loads(body_str)
                        extracted_attachment_text = process_textract_output(textract_data)
                        extracted_attachment_content.append(extracted_attachment_text)
                        logger.info(extracted_attachment_content)
                    # Join all lines with line breaks to preserve structure
                    full_extracted_attachment_text = "\n".join(extracted_attachment_content)
                extractedEmailData = queryExtractedEmailById(extractedEmailId)
                # logger.info(extractedEmailData)
                # logger.info(full_extracted_attachment_text)

            emailBody = extractedEmailData['emailBody']
            recipientEmail = extractedEmailData['recipientEmail']
            subject = extractedEmailData['subject']
            emailSentDate = extractedEmailData['emailSentDate']
            senderEmail = extractedEmailData['senderEmail']

            # Analyze end user, industry, and reseller
            endUser, reseller = bedrock_analyze_entity_prompt(
                emailBody, 'text')
            attachmentEndUser = bedrock_analyze_entity_prompt(
                full_extracted_attachment_text, 'attachment')
            # logger.info(endUser)
            # logger.info(reseller)

            # Analyze products and brands
            productBrands = bedrock_analyze_product_brand_prompt(
                emailBody, 'text', dc_mapping, software_mapping, en_collabs_mapping, security_mapping, cisco_mapping)

            attachmentProductBrands = bedrock_analyze_product_brand_prompt(
                full_extracted_attachment_text, 'attachment', dc_mapping, software_mapping, en_collabs_mapping, security_mapping, cisco_mapping)
            logger.info(productBrands)
            logger.info(attachmentProductBrands)

            # Generic product found
            if attachmentProductBrands['generalProduct'] != '-' or productBrands['generalProduct'] != '-':
                # Merge general products from attachment and email content
                merged_general_products = safe_list(attachmentProductBrands.get('generalProduct')) + \
                          safe_list(productBrands.get('generalProduct'))
                # Analyze with second brand mapping data
                fallback_brand = bedrock_analyze_generic_prompt(
                    merged_general_products, productBrands['entity'])
                if productBrands['brand'] != '-' or attachmentProductBrands['brand'] != '-':
                    teamVendor = bedrock_analyze_team_vendor_prompt(
                        productBrands['brand'], attachmentProductBrands['brand'], team_mapping) 
                    productBrands = merge_brand(productBrands, attachmentProductBrands)
                    productBrands = merge_brand(productBrands, teamVendor)
                # else:
                #     # productBrands['brand'] = safe_list(productBrands['brand']) # Convert '-' to empty list
                print("Fallback brand")
                logger.info(fallback_brand)
                finalProductBrand = merge_brand(productBrands, fallback_brand)
                print("After fallback brand")
                logger.info(finalProductBrand)
            else:                
                print("No need to fall back")
                # Analyze vendor and team
                teamVendor = bedrock_analyze_team_vendor_prompt(
                    productBrands['brand'], attachmentProductBrands['brand'], team_mapping)
                finalProductBrand = merge_brand(productBrands, teamVendor)
                logger.info(finalProductBrand)

            # Analyze subject and tender
            attachmentTender = bedrock_analyze_subject_tender(
                full_extracted_attachment_text, 'attachment', subject, emailSentDate)
            subjectTender = bedrock_analyze_subject_tender(
                emailBody, 'text', subject, emailSentDate)
            logger.info(subjectTender)
            logger.info(attachmentTender)
            logger.info(attachmentEndUser)

            # Consolidate analysis results 
            consolidatedData = consolidation_prompt(
                finalProductBrand, attachmentProductBrands, endUser, reseller, attachmentTender, attachmentEndUser, subjectTender)
            logger.info(consolidatedData)

            # Map extracted data to ExtractedEmail table
            emailAnalysisResultId = str(uuid.uuid4())
            now = datetime.now().strftime('%d-%m-%YT%H:%M:%S.%fZ')
            extractedEmailIds.append(extractedEmailId)
            # Map extracted data to ExtractedEmail table
            email_analysis_item = {
                'emailAnalysisResultId': emailAnalysisResultId,
                'extractedEmailId': extractedEmailId,
                'senderEmailAddress': consolidatedData.get('redirectorAddress', '-'),
                'subject': consolidatedData.get('subject', '-'),
                'emailSentDate': consolidatedData.get('emailSentDate', '-'),
                'product': consolidatedData.get('productName', '-'),
                'productMYR': consolidatedData.get('chineseProducts', '-'),
                'suppMYR': consolidatedData.get('chineseProductSupport', '-'),
                'productUSD': consolidatedData.get('westernProducts', '-'),
                'suppUSD': consolidatedData.get('westernProductSupport', '-'),
                'kuServicesMYR': consolidatedData.get('kuServices', '-'),
                'brand': consolidatedData.get('brand', '-'),
                'vendor': consolidatedData.get('vendor', '-'),
                'reseller': consolidatedData.get('reseller', '-'),
                'endUserName': consolidatedData.get('endUserName', '-'),
                'industry': consolidatedData.get('industry', '-'),
                'team': consolidatedData.get('team', '-'),
                "tender": consolidatedData.get('isTender', False),
                "sourceFile": sourceFile,
                "merchantId": MERCHANT_ID,
                'createdAt': now,
                'createdBy': "System",
                'updatedAt': now,
                'updatedBy': "System",
            }
            EMAIL_ANALYSIS_RESULT_DDB_TABLE.put_item(Item=email_analysis_item)
            logger.info("email_analysis_item:", email_analysis_item)
            payload = {
                'emailAnalysisResultId': emailAnalysisResultId,
            }
            response = sendToSQS(payload)
        
        # Only Email Text Content
        else:
            print("Found a Content only Email")
            for record in event["Records"]:
                message_body = record.get('body', '{}')
                if type(message_body) == str:
                    message = json.loads(message_body)
                else:
                    message = message_body

                filepath = unquote_plus(
                    message["filepath"]
                )
                filepathPrefix = filepath.split("/")[3]
                sourceFile = filepath.split("/")[4]

                extractedEmailId = message.get('extractedEmailId')
                extractedEmailData = queryExtractedEmailById(extractedEmailId)
                emailBody = extractedEmailData['emailBody']
                recipientEmail = extractedEmailData['recipientEmail']
                subject = extractedEmailData['subject']
                emailSentDate = extractedEmailData['emailSentDate']
                senderEmail = extractedEmailData['senderEmail']

                # Analyze end user, industry, and reseller
                endUser, reseller = bedrock_analyze_entity_prompt(
                    emailBody, 'text')
                # logger.info(endUser)
                # logger.info(reseller)

                # Analyze products and brands
                productBrands = bedrock_analyze_product_brand_prompt(
                    emailBody, 'text', dc_mapping, software_mapping, en_collabs_mapping, security_mapping, cisco_mapping)
                logger.info(productBrands)
   
                # Generic product found
                if productBrands['generalProduct'] != '-':
                    fallback_brand = bedrock_analyze_generic_prompt(
                        productBrands['generalProduct'], productBrands['entity'])
                    if productBrands['brand'] != '-':
                        teamVendor = bedrock_analyze_team_vendor_prompt(
                            productBrands['brand'], [], team_mapping) 
                        productBrands = merge_brand(productBrands, teamVendor)
                    # else:
                    #     # productBrands['brand'] = safe_list(productBrands['brand']) # Convert '-' to empty list
                    print("Fallback brand")
                    logger.info(fallback_brand)
                    finalProductBrand = merge_brand(productBrands, fallback_brand)
                    print("After fallback brand")
                    logger.info(finalProductBrand)
                else:
                    print("No need to fall back")
                    # Analyze vendor and team
                    teamVendor = bedrock_analyze_team_vendor_prompt(
                        productBrands['brand'], [], team_mapping) 
                    finalProductBrand = merge_brand(productBrands, teamVendor)
                    logger.info(finalProductBrand)

                # Analyze subject and tender
                subjectTender = bedrock_analyze_subject_tender(
                    emailBody, 'text', subject, emailSentDate)
                logger.info(subjectTender)

                # Consolidate analysis results 
                consolidatedData = consolidation_prompt(
                    finalProductBrand, [], endUser, reseller, [], [], subjectTender)

                # Map extracted data to ExtractedEmail table
                emailAnalysisResultId = str(uuid.uuid4())
                now = datetime.now().strftime('%d-%m-%YT%H:%M:%S.%fZ')
                extractedEmailIds.append(extractedEmailId)
                # Map extracted data to ExtractedEmail table
                email_analysis_item = {
                    'emailAnalysisResultId': emailAnalysisResultId,
                    'extractedEmailId': extractedEmailId,
                    'senderEmailAddress': consolidatedData.get('redirectorAddress', '-'),
                    'subject': consolidatedData.get('subject', '-'),
                    'emailSentDate': consolidatedData.get('emailSentDate', '-'),
                    'product': consolidatedData.get('productName', '-'),
                    'productMYR': consolidatedData.get('chineseProducts', '-'),
                    'suppMYR': consolidatedData.get('chineseProductSupport', '-'),
                    'productUSD': consolidatedData.get('westernProducts', '-'),
                    'suppUSD': consolidatedData.get('westernProductSupport', '-'),
                    'kuServicesMYR': consolidatedData.get('kuServices', '-'),
                    'brand': consolidatedData.get('brand', '-'),
                    'vendor': consolidatedData.get('vendor', '-'),
                    'reseller': consolidatedData.get('reseller', '-'),
                    'endUserName': consolidatedData.get('endUserName', '-'),
                    'industry': consolidatedData.get('industry', '-'),
                    'team': consolidatedData.get('team', '-'),
                    "tender": consolidatedData.get('isTender', False),
                    "sourceFile": sourceFile,
                    "merchantId": MERCHANT_ID,
                    'createdAt': now,
                    'createdBy': "System",
                    'updatedAt': now,
                    'updatedBy': "System",
                }
                EMAIL_ANALYSIS_RESULT_DDB_TABLE.put_item(Item=email_analysis_item)
                logger.info("email_analysis_item:", email_analysis_item)
                payload = {
                    'emailAnalysisResultId': emailAnalysisResultId,
                }
                response = sendToSQS(payload)

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
def extractInfoFromEmail(S3_BUCKET_NAME, fileKey):
    response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey)
    emailRawBytes = response['Body'].read()

    with open('/tmp/temp_email.msg', 'wb') as temp_file:
        temp_file.write(emailRawBytes)

    msg = extract_msg.Message('/tmp/temp_email.msg')
    msg_message = msg.body

    senderEmail = re.search(
        r'<(.*?)>', msg.sender).group(1) if '<' in msg.sender else msg.sender
    recipientEmail = re.search(
        r'<(.*?)>', msg.to).group(1) if '<' in msg.to else msg.to
    ccList = [re.search(r'<(.*?)>', cc).group(1)
              if '<' in cc else cc for cc in re.split(r';|,', msg.cc)] if msg.cc else []
    subject = msg.subject
    emailBody = msg_message
    emailSentDate = msg.date

    return senderEmail, recipientEmail, ccList, subject, emailBody, emailSentDate


@tracer.capture_method
def skillMatrixMapping(fileKey) -> List[Dict]:
    response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey)
    csvContent = pd.read_csv(
        response['Body'], dtype=str, encoding='utf-8').to_dict('records')

    skillMatrixContent = []

    # Field mapping for the CSV columns
    field_mapping = {
        'Name': 'name',
        'Role': 'role',
        'Email': 'email',
        'Primary Brand': 'primaryBrand',
        'Secondary Brand': 'secondaryBrand',
        'Team': 'team'
    }

    # Iterate through each row in the CSV
    for record in csvContent:
        # Clean and normalize the record
        record = {key.strip(): (value.strip() if isinstance(
            value, str) else value) for key, value in record.items()}

        # Map the record to the desired format
        mappedRecord = {}
        for key, mappedKey in field_mapping.items():
            cellValue = getCellValue(record, key)
            if key == 'Primary Brand' or key == 'Secondary Brand':
                if isinstance(cellValue, str):
                    # Split by comma and clean each item
                    cleaned_list = [v.strip()
                                    for v in cellValue.split(",") if v.strip()]
                elif isinstance(cellValue, list):
                    cleaned_list = [str(v).strip() for v in cellValue if v]
                else:
                    cleaned_list = []
                mappedRecord[mappedKey] = cleaned_list
            else:
                mappedRecord[mappedKey] = str(getCellValue(record, key, "-"))

        # Append the mapped record to the list
        skillMatrixContent.append(mappedRecord)

    return skillMatrixContent


@tracer.capture_method
def storeSkillMatrixInDB(skillMatrixData):
    for record in skillMatrixData:
        skillMatrixId = str(uuid.uuid4())
        now = datetime.now().strftime('%d-%m-%YT%H:%M:%S.%fZ')

        payload = {
            'skillMatrixId': skillMatrixId,
            'roleName': record.get('role', ''),
            'name': record.get('name', ''),
            'emailAddress': record.get('email', ''),
            'team': record.get('team', ''),
            'primaryBrand': record.get('primaryBrand', []),
            'secondaryBrand': record.get('secondaryBrand', []),
            'createdAt': now,
            'createdBy': "System",
            'updatedAt': now,
            'updatedBy': "System"
        }
        SKILL_MATRIX_DDB_TABLE.put_item(Item=payload)


@tracer.capture_method
def sendToSQS(payload):
    payloadJson = json.dumps(payload, default=decimalDefault)
    response = SQS_CLIENT.send_message(
        QueueUrl=SQS_QUEUE_URL,
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

# Get value from cell


@tracer.capture_method
def getCellValue(row, column, default=None):
    cell = row[column]
    if not pd.isna(cell):
        return cell
    else:
        return default

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

def merge_brand(b1, b2):
    merged = {}
    if not b1 and not b2:
        return {}
    if not b1:
        return b2
    if not b2:
        return b1
    keys = set(b1) | set(b2)
    for key in keys:
        v1 = b1.get(key)
        v2 = b2.get(key)
        # If both are '-'
        if v1 == '-' and v2 == '-':
            merged[key] = '-'
        # If one is '-' and the other is not
        elif v1 == '-':
            merged[key] = v2
        elif v2 == '-':
            merged[key] = v1
        # If both are lists
        elif isinstance(v1, list) and isinstance(v2, list):
            merged_list = list(set(v1 + v2))
            merged[key] = merged_list if merged_list else '-'
        # If both are dicts
        elif isinstance(v1, dict) and isinstance(v2, dict):
            merged_dict = {**v1, **v2}
            merged[key] = merged_dict if merged_dict else '-'
        else:
            # Scalar fallback: prefer v1 if exists
            merged[key] = v1 if v1 is not None else (v2 if v2 is not None else '-')
    return merged

# Helper to convert '-' to empty list
def safe_list(value):
    return value if isinstance(value, list) else []