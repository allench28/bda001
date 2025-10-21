import boto3
import time
import uuid
import os
import json
import email
import base64
import requests
from requests_aws4auth import AWS4Auth
from datetime import datetime
from decimal import Decimal
from PyPDF2 import PdfWriter, PdfReader
from PyPDF2.errors import PdfReadError
from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools import Logger, Tracer
from email.header import decode_header, make_header
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from custom_exceptions import ResourceNotFoundException, BadRequestException
from PIL import Image
import re

# EXTRACTED_DOC_TEMPLATE_TABLE = os.environ.get("EXTRACTED_DOC_TEMPLATE_TABLE")
INPUT_BUCKET = os.environ.get('S3_BUCKET') 
DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
INBOX_MONITORING_TABLE = os.environ.get('INBOX_MONITORING_TABLE')

# ES_DOMAIN_ENDPOINT = os.environ.get('ES_DOMAIN_ENDPOINT') 
# STEPFUNCTIONARN = os.environ.get('STEPFUNCTIONARN')
# EMAIL_FROM = os.environ.get('EMAIL_FROM')
# ACCOUNT_TABLE = os.environ.get('ACCOUNT_TABLE')
# ENV_FLAG = os.environ.get('ENV_FLAG')
# MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
# MERCHANT_CROSS_ACCOUNT_TABLE = os.environ.get('MERCHANT_CROSS_ACCOUNT_TABLE')
# SQS_URL = os.environ.get('SQS_URL')

# TEXTRACT_CLIENT = boto3.client('textract')
# STS_CLIENT = boto3.client('sts')
S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource("dynamodb")
# DDB_RESOURCE = boto3.resource("dynamodb")
# STEPFUNCTION_CLIENT = boto3.client('stepfunctions')
# SES_CLIENT = boto3.client('ses')
# SQS = boto3.client('sqs')

# EXTRACTED_DOC_TEMPLATE_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOC_TEMPLATE_TABLE)
# ACCOUNT_DDB_TABLE = DDB_RESOURCE.Table(ACCOUNT_TABLE)
# MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
# MERCHANT_CROSS_ACCOUNT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_CROSS_ACCOUNT_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
INBOX_MONITORING_DDB_TABLE = DDB_RESOURCE.Table(INBOX_MONITORING_TABLE)


# SERVICE = 'es'
# CREDENTIALS = boto3.Session().get_credentials()
# REGION = 'ap-southeast-1'
# AWSAUTH = AWS4Auth(CREDENTIALS.access_key, CREDENTIALS.secret_key, REGION, SERVICE, session_token=CREDENTIALS.token)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    senderEmail=""
    try:
        # logger.info(f"EVENT: {event}")
        filepath = event["Records"][0]["s3"]["object"]["key"]
        # merchant_id = filepath.split("/")[1]
        # logger.info("FILEPATH:  "+filepath)
        filepathPrefix = filepath.split("/")[0]

        if filepathPrefix == "email":
            senderEmail, recipientEmail, decodedPdfFilepathList, account, isPDF = extractInfoFromEmail(INPUT_BUCKET, filepath)
            if not isPDF:
                return {'status': True, 'message': 'Wait for ec2 to process file to pdf format.'}
            for decodedPdfFilepath in decodedPdfFilepathList:
                attachmentName = decodedPdfFilepath.get('decodedPdfFilepath').split("/")[-1]
                attachmentName = attachmentName.replace("PDF", "pdf")
                attachmentName = attachmentName.replace(" ", "_")
                attachmentName = attachmentName.replace("(", "")
                attachmentName = attachmentName.replace(")", "")
                attachmentName = attachmentName.replace("&", "")
                attachmentName = attachmentName.replace("\n", "")
                
                merchant_id = account.get('merchantId')
                # logger.info(merchant_id)
                dateToday = datetime.now().strftime('%Y%m%d')

                generated_uuid = str(uuid.uuid4())

                # if ENV_FLAG == '1':
                #     checkIfCrossAcc(merchantId)

                merchantConfig = getMerchantConfiguration(merchant_id)
                customLogics = merchantConfig.get('customLogics', {})
                skipDocumentSplitter = customLogics.get('skipDocumentSplitter', False)

                if skipDocumentSplitter:
                    s3Path = f'input/{merchant_id}/{generated_uuid}/{attachmentName}'
                elif account.get('documentType') == "medicalReferralLetter":
                    s3Path = f'input/{merchant_id}/{generated_uuid}/{attachmentName}'

                else:
                    if decodedPdfFilepath.get('pdfType') == 'singlePage':
                        s3Path = f'email_document/{merchant_id}/{generated_uuid}/{attachmentName}'

                    elif decodedPdfFilepath.get('pdfType') == 'multiPage':
                        s3Path = f'email_document/{merchant_id}/{generated_uuid}/{attachmentName}'

                # logger.info(f"Uploading to s3: {s3Path}")

                S3_CLIENT.upload_file(decodedPdfFilepath.get('decodedPdfFilepath'), INPUT_BUCKET, s3Path)

                # logger.info(f"Uploaded to s3: {s3Path}")

                now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

                # Create DocumentUpload record
                document_upload_item = {
                    'documentUploadId': generated_uuid,
                    'merchantId': merchant_id,
                    # 'userId': account.get('accountId'),  # Using accountId as userId
                    'fileName': attachmentName,
                    'folder': 'email',
                    'inputPath': s3Path,
                    'source': 'email',
                    'inputSource': senderEmail,
                    'documentType': account.get('documentType'),  # Will be determined in later processing
                    'avgConfidenceScore': '',  # Will be updated after processing
                    'confidenceScoreList': '',
                    'status': 'In Progress',
                    'exceptionStatus': '',
                    'errorPath': '',
                    'invalidPath': '',
                    'createdAt': now,
                    'createdBy': 'email-processor',
                    'updatedAt': now,
                    'updatedBy': 'email-processor'
                }

                # Create the record in DynamoDB
                DOCUMENT_UPLOAD_DDB_TABLE.put_item(Item=document_upload_item)
                # logger.info(f"Created DocumentUpload record: {generated_uuid}")


                executionId = str(uuid.uuid4())

                
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

# @tracer.capture_method
# def sendErrorMail(senderEmail, txtContent = None):
#     try:
#         # Create a MIMEMultipart class, and set up the From, To, Subject fields
#         msg = MIMEMultipart()
#         msg["Subject"] = "Document Failed To Process"
#         msg["From"] = EMAIL_FROM
#         msg["To"] = senderEmail
#         if not txtContent:
#             txtContent = """\
#             <html>
#                 <head></head>
#                 <body>
#                     The document that you sent to smart eye has failed to be processed.<br>
#                     This email is not registered. Please use a valid merchant email to send the document.<br><br>
#                     Thank you.<br><br>
#                     <i>This is an auto generated email. Please do not reply to it.</i>
#                 </body>
#             </html>
#             """

#         # Set message body
#         body = MIMEText(txtContent, 'html')
#         msg.attach(body)

#         # Convert message to string and send
#         SES_CLIENT.send_raw_email(
#             Destinations=[senderEmail],
#             RawMessage={"Data": msg.as_string()}
#         )
#         return {'status': True, 'message': 'Send error email success'}
#     # Display an error if something goes wrong.
#     except Exception as ex:
#         logger.warning({"message": str(ex)})
#         return {'status': False, 'message': "Failed to send error email."}

# @tracer.capture_method
# def checkIfCrossAcc(merchantId):
#     global INPUT_BUCKET, S3_CLIENT
#     merchant = getMerchant(merchantId)
#     if merchant.get('isCrossAccount'):
#         crossAccountMerchant = getCrossAccountMerchant(merchantId)
#         roleArn = crossAccountMerchant[0].get('roleArn')
#         bucketName = crossAccountMerchant[0].get('textractInputS3BucketName')
        
#         customerAccount = STS_CLIENT.assume_role(
#             RoleArn=roleArn,
#             RoleSessionName="cross_acct_lambda"
#         )
        
#         ACCESS_KEY = customerAccount['Credentials']['AccessKeyId']
#         SECRET_KEY = customerAccount['Credentials']['SecretAccessKey']
#         SESSION_TOKEN = customerAccount['Credentials']['SessionToken']
#         INPUT_BUCKET = bucketName
        
#         S3_CLIENT = boto3.client(
#             's3',
#             aws_access_key_id=ACCESS_KEY,
#             aws_secret_access_key=SECRET_KEY,
#             aws_session_token=SESSION_TOKEN,
#         )

# @tracer.capture_method
# def getMerchant(merchantId):
#     merchant = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchantId}).get('Item')
#     if merchant:
#         return merchant
        
#     raise NotFoundError('Merchant Not Found')

# @tracer.capture_method
# def getCrossAccountMerchant(merchantId):
#     crossAccountMerchant = MERCHANT_CROSS_ACCOUNT_DDB_TABLE.query(
#         IndexName='gsi-merchantId',
#         KeyConditionExpression=Key('merchantId').eq(merchantId)).get('Items')
#     if crossAccountMerchant and len(crossAccountMerchant)>0:
#         return crossAccountMerchant
    
#     raise NotFoundError("Cross Account Merchant Not Found!")

@tracer.capture_method
def extractInfoFromEmail(bucketName, filepath):
    S3Response = S3_CLIENT.get_object(Bucket=bucketName, Key=filepath)
    # logger.info("S3Response obtained")
    emailRawBytes = S3Response['Body'].read()
    emailRawString = emailRawBytes.decode('utf-8')
    emailObject = email.message_from_string(emailRawString)
    fromEmail = emailObject['From'].split("<")[-1].split(">")[0]
    toEmail = emailObject['To'].split("<")[-1].split(">")[0]
    decodedPdfFilepathList = []
    isImage = False

    account = getAccount(toEmail)
    # logger.info(account)
    # print("account", account)

    # merchantId = fromEmail.split('@')[0]

    # account = {"merchantId": merchantId, "accountId": merchantId}
    # logger.info(f"Account: {account}")

    # if not account:
    #     return fromEmail, toEmail, [], account, None
    
    if emailObject.is_multipart():
        for part in emailObject.walk():
            contentType = part.get_content_type()
            isImage = False
            if contentType.__contains__('word') or contentType.__contains__('excel') or contentType.__contains__('sheet'):
                response = sendSQSToEc2(bucketName, filepath)
                if response.get('status') == False:
                    raise BadRequestError(response.get('message'))
                else:
                    return "", "", "", "", False
            
            if contentType.__contains__('image'):
                attachmentName = str(make_header(decode_header(part.get_filename())))
                attachment = base64.b64decode(part.get_payload())
                decodedImgFilepath = f'/tmp/{attachmentName}'
                
                with open(decodedImgFilepath, "wb") as f:
                    f.write(attachment)
                decodedPdfFilepath = imgToPdf(decodedImgFilepath)
                contentType = 'application/pdf'
                isImage = True
                
            if contentType == 'application/pdf':
                
                if not isImage: # image was decoded above, skip if is passed by image
                    attachmentName = str(make_header(decode_header(part.get_filename())))
                    attachmentName = attachmentName.replace("PDF", "pdf")
                    attachment = base64.b64decode(part.get_payload())
                    decodedPdfFilepath = f'/tmp/{attachmentName}'
                    with open(decodedPdfFilepath, "wb") as f:
                        f.write(attachment)

                try:
                    inputPdf = PdfReader(decodedPdfFilepath)
                    # logger.info(inputPdf)
                except PdfReadError:
                    txtContent = f"""\
<html>
    <head></head>
    <body>
        The document with name {attachmentName} that you sent to smart eye has failed to be processed.<br>
        Please kindly check if the file is corrupted.<br><br>
        Thank you.<br><br>
        <i>This is an auto generated email. Please do not reply to it.</i>
    </body>
</html>
"""
                    # sendErrorMail(fromEmail, txtContent)
                    
                outputPdf = PdfWriter()
                
                try:
                    numPages = len(inputPdf.pages) 
                except:
                    # to default to single page and prevent running the pdf size checking below
                    numPages = 0
                
                if numPages > 1:
                    payload = {
                        'pdfType': 'multiPage',
                        'decodedPdfFilepath': decodedPdfFilepath
                    }
                else:
                    payload = {
                        'pdfType': 'singlePage',
                        'decodedPdfFilepath': decodedPdfFilepath
                    }

                decodedPdfFilepathList.append(payload)
                
                amountToCrop = 0
                for i in range(numPages):
                    page = inputPdf.pages[i]
                    upperRightMax = list(page.cropbox.upper_right)[1]
                    upperLeftMax = list(page.cropbox.upper_left)[1]

                    if upperLeftMax == upperRightMax > 810:
                        # amountToCrop = 3
                        amountToCrop = int(float(upperLeftMax) * 0.0036)
                        page.cropbox.upper_right = (list(page.cropbox.upper_right)[0], Decimal(list(page.cropbox.upper_right)[1] - amountToCrop))
                        page.cropbox.upper_left = (list(page.cropbox.upper_left)[0], Decimal(list(page.cropbox.upper_left)[1] - amountToCrop))
                        # page.cropbox.lower_right = (list(page.cropbox.lower_right)[0], Decimal(list(page.cropbox.lower_right)[1] + amountToCrop))
                        # page.cropbox.lower_left = (list(page.cropbox.lower_left)[0], Decimal(list(page.cropbox.lower_left)[1] +amountToCrop ))

                    outputPdf.add_page(page)
                        
                if amountToCrop > 0:

                    with open(decodedPdfFilepath, "wb") as f:
                        outputPdf.write(f)
        return fromEmail, toEmail, decodedPdfFilepathList, account, True

# @tracer.capture_method
# def queryMerchantIdWithEmail(esPath, ownerEmail):
#     esUrl = f'https://{ES_DOMAIN_ENDPOINT}/{esPath}'
#     headers = {
#             'Content-Type': "application/json",
#             'User-Agent': "PostmanRuntime/7.20.1",
#             'Accept': "*/*",
#             'Cache-Control': "no-cache",
#             'Postman-Token': "1ae2b03c-ac6c-45f4-9b37-4f95b9b0102c,b678f18f-3ebe-458e-b63b-6ced7b74851f",
#             'Host': ES_DOMAIN_ENDPOINT,
#             'Accept-Encoding': "gzip, deflate",
#             'Connection': "keep-alive",
#             'cache-control': "no-cache"
#         }

#     query = {
#                 'bool': {
#                     'must': [
#                         {'match_phrase': {'ownerEmail': ownerEmail}},
#                     ]
#                 }
#             }
    
#     limit = 1

#     payload = {
#         "size": limit,
#         "query": query,
#         "sort" : [{"createdAt": "desc"}]
#     }
    
#     response = requests.request("GET", esUrl, data=json.dumps(payload), headers=headers, auth=AWSAUTH)
#     responseText = json.loads(response.text)
#     queryHits = responseText.get('hits', {}).get('hits', [])
    
#     if len(queryHits):
#         merchant = queryHits[0].get('_source', {})
#         merchantId = merchant.get('merchantId')
#         return merchantId
    
#     else:
#         raise NotFoundError('Email is not registered as merchant!')

def getAccount(emailAddress):
    accountResp = INBOX_MONITORING_DDB_TABLE.query(
        IndexName='gsi-email',
        KeyConditionExpression=Key('email').eq(emailAddress),
    )

    if len(accountResp.get('Items'))>0:
        return accountResp.get('Items')[0]

    return None
    
# @tracer.capture_method
# def sendSQSToEc2(bucketName, filePath):
#     response = False
#     payload = {}
#     payload['filePath'] = filePath
#     payload['bucketName'] = bucketName
    
#     response = SQS.send_message(
#         QueueUrl = SQS_URL,
#         MessageBody = json.dumps(payload)
#     )
#     if response is None:
#         return {'status': False, 'message': 'Error occured while Sending filePath and bucketName to SQS.'}
#     elif not response:
#         return {'status': False, 'message': 'No info to be sent to SQS'}

#     return {'status': True, 'message': 'Success'}
        
@tracer.capture_method
def imgToPdf(decodedImgFilepath):
    image_1 = Image.open(decodedImgFilepath)
    im_1 = image_1.convert('RGB')
    
    imageFormat = ["jpeg", "jpg", "png", "jfif"]
    for formatType in imageFormat:
        if re.search(formatType, decodedImgFilepath, flags=re.IGNORECASE):
            convertedFileName = re.sub(formatType, "pdf", decodedImgFilepath, flags=re.IGNORECASE)
    im_1.save(convertedFileName)
    return convertedFileName

@tracer.capture_method
def getMerchantConfiguration(merchantId):
    """
    Get merchant configuration once and return structured data
    """
    response = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchantId})
    merchant = response.get('Item', {})
    
    # Extract all necessary fields
    customLogics = merchant.get('customLogics', {})
    
    merchant_config = {
        'merchantId': merchantId,
        'customLogics': customLogics,
    }
        
    return merchant_config