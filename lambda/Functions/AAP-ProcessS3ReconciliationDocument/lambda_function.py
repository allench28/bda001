import os
import uuid
import boto3
import json
import pandas as pd
import time
import re
import requests
from requests_aws4auth import AWS4Auth
from typing import Dict
from decimal import Decimal
from typing import List
from dateutil import parser
from datetime import datetime
from zoneinfo import ZoneInfo
import urllib.parse
from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException

# Open Search Environment Variables
ES_DOMAIN_ENDPOINT = os.environ.get('ES_DOMAIN_ENDPOINT')

# DDB Table Environment Variables
SALES_STATEMENT_TABLE = os.environ.get("SALES_STATEMENT_TABLE")
ODOO_PAYMENT_TABLE = os.environ.get("ODOO_PAYMENT_TABLE")
BANK_STATEMENT_TABLE = os.environ.get("BANK_STATEMENT_TABLE")
PAYMENT_TRANSFER_REPORT_TABLE = os.environ.get("PAYMENT_GATEWAY_REPORT_TABLE")
PAYMENT_TRANSACTION_TABLE = os.environ.get("PAYMENT_TRANSACTION_TABLE")
STORE_TABLE = os.environ.get("STORE_TABLE")
DOCUMENT_UPLOAD_TABLE = os.environ.get("DOCUMENT_UPLOAD_TABLE")

# S3 Bucket and SQS Queue Environment Variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
CREATE_DOCUMENT_SQS_QUEUE_URL = os.environ.get('CREATE_DOCUMENT_SQS_QUEUE_URL')
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME')

CREDENTIALS = boto3.Session().get_credentials()
DDB_RESOURCE = boto3.resource('dynamodb')
LAMBDA_CLIENT = boto3.client('lambda')
S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs')

# DDB Table Object Initialization
SALES_STATEMENT_DDB_TABLE = DDB_RESOURCE.Table(SALES_STATEMENT_TABLE)
ODOO_PAYMENT_DDB_TABLE = DDB_RESOURCE.Table(ODOO_PAYMENT_TABLE)
BANK_STATEMENT_DDB_TABLE = DDB_RESOURCE.Table(BANK_STATEMENT_TABLE)
PAYMENT_TRANSFER_REPORT_DDB_TABLE = DDB_RESOURCE.Table(PAYMENT_TRANSFER_REPORT_TABLE)
PAYMENT_TRANSACTION_DDB_TABLE = DDB_RESOURCE.Table(PAYMENT_TRANSACTION_TABLE)
STORE_DDB_TABLE = DDB_RESOURCE.Table(STORE_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)

ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-1', 'es', session_token=CREDENTIALS.token)

# To memoize the branch code lookups process
BRANCH_CODE_CACHE = {}

logger = Logger()
tracer = Tracer()

# File Path Key
SALES_STATEMENT_KEY = 'POS/Sales/'
ODOO_PAYMENT_KEY = 'POS/Odoo/'
BANK_STATEMENT_KEY = 'bank-statement/'
PAYMENT_TRANSFER_REPORT_KEY = 'payment-gateway/'
PAYMENT_TRANSACTION_KEY = 'payment-transaction/'
STORE_KEY = 'store/'
CREDITCARD_SETTLEMENT_KEY = 'credit-card-settlement/'

# Merchant id
MERCHANT_ID = "6b8a78e2-95fe-403b-8008-e5f7c1a631fc" # Genting

PAYMENT_METHOD_MAPPING = {
    "Debit/Credit Card - RHB Terminal": "RHB_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - CIMB Terminal": "CIMB_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - Bank Rakyat Terminal": "BANK_RAKYAT_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - Ambank Terminal": "AMBANK_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - Affin Terminal": "AFFIN_DEBIT_CREDIT_CARD",
    "CIMB Bonus Point": "CIMB_BONUS_POINT",
    "DuitNow QR": "DUITNOW_QR",
    "Ambank QR": "AMBANK_QR",
    "ShopeeFood": "SHOPEE_FOOD",
    "ShopBack": "SHOPEE_BACK",
    "Retail Cash": "CASH",
    "GrabPay": "GRABPAY",
    "Shopee Pay": "SHOPEE_PAY",
    "Touch N Go": "TNG",
    "Affin QR Code": "AFFIN_QR_CODE",
    "GrabFood / GrabCake - Campaign Testing": "GRABFOOD_GRABCAKE_CAMPAIGN_TESTING",
    "GrabFood / GrabCake": "GRABFOOD",
    "FoodPanda": "FOODPANDA",
    "DeliverEat": "DELIVEREAT",
    # "WhatsApp": "WHATSAPP",
    # "Air-Asia Food": "AIRASIA_FOOD",
    # "Mall Voucher": "MALL_VOUCHER",
    # "BR Voucher": "BR_VOUCHER",
    # "Payment To HQ": "PAYMENT_TO_HQ",
    # "Sunway Pals": "SUNWAY_PALS",
    # "Setel Deliver2Me": "SETEL_DELIVER2ME",
    # "OneShop": "ONESHOP",
    # "Redemption OneCard": "REDEMPTION_ONECARD",
    # "1Pay E-Wallet": "1PAY_EWALLET",
    # "KM App": "KM_APP",
    # "Redemption Genting": "REDEMPTION_GENTING",
    # "Redemption Sunway": "REDEMPTION_SUNWAY",
    # "Mobile App": "MOBILE_APP",
    # "Kiosk": "KIOSK",
    # "WebComm": "WEBCOMM",
    # "IPay88": "IPAY88",
    # "Voucher Redemption (Product)": "VOUCHER_REDEMPTION_PRODUCT",
    # "Voucher Redemption (Cash)": "VOUCHER_REDEMPTION_CASH",
    # "Alipay": "ALIPAY",
    # "Debit/Credit Card - MBB Terminal": "DEBIT_CREDIT_CARD_MBB",
    # "PrimePay": "PRIMEPAY",
    # "Maybank Treatspoints": "MAYBANK_TREAT_POINTS",
    # "HSBC Birthday - Free Junior Scoop": "HSBC_BIRTHDAY_FREE_JUNIOR_SCOOP",
    # "Lazada E-Wallet": "LAZADA_EWALLET",
    # "Mesra Card": "MESRA_CARD",
}

BANK_NAME_CODE = {
    "CIMB_I": "CIMB_I",
    "CIMB": "CIMB"
}

TRANSACTION_STATUS = {
    'Transferred': 'SUCCESS',
    'Failed': 'FAILED',
    'Completed': 'SUCCESS',
    'Cancelled': 'FAILED'
}

TRANSACTION_AMOUNT_TYPE = {
    'C': 'CREDIT',
    'B': 'DEBIT'
}

TRANSACTION_TYPE = {
    'Payment': 'SALE',
    'Refund': 'REFUND',
}

GATEWAY_TRANSACTION_TYPE = {
    'GrabPay': 'GRABPAY',
    'GrabFood': 'GRABFOOD',
    'GrabMart': 'GRABMART'
}

GATEWAY_TRANSACTION_GATEGORY = {
    "Payment": "PAYMENT",
    "Adjustment": "ADJUSTMENT",
    "Advertisement": "ADVERTISEMENT",
    "Refund": "REFUND",
    "Voucher": "VOUCHER"
}

PAYMENT_METHOD = {
    "RPP": "CREDIT_CARD",
    "GrabPay Wallet": "EWALLET",
    "Cashless": "CASHLESS",
    "Cash": "CASH",
    "Cashless - DBMY": "CASHLESS_DBMY",
    "Cashless - GrabPay Wallet": "CASHLESS_GRABPAY_WALLET",
    "Cashless - MasterCard Credit Card": "CASHLESS_MASTERCARD_CREDIT_CARD",
    "Cashless - PayLater": "CASHLESS_PAYLATER",
    "Cashless - Visa Credit Card": "CASHLESS_VISA_CREDIT_CARD",
    "PayLater Instalments": "PAYLATER_INSTALMENTS",
    "PayLater Postpaid": "PAYLATER_POSTPAID"
}

SALES_TYPE = {
    "FALSE": "SALES",  
    "TRUE": "NON_SALES"
}

IS_CANCEL_RECEIPT = {
    "FALSE": "PAID",
    "TRUE": "CANCELLED"    
}

@tracer.capture_lambda_handler
def lambda_handler(event, context):

    try:
        for record in event.get("Records", [{}]):
            objectKey = record.get('s3').get('object').get('key')
            unicodeString = objectKey.replace('+', ' ')
            objectKeyOriginal = urllib.parse.unquote(unicodeString)
            if MERCHANT_ID not in objectKeyOriginal:
                raise BadRequestException("Invalid file path. Please check the file path and try again.")

            fileName = objectKeyOriginal.split("/")[-1]
            documentUploadId = None

            # Determine document type and create initial record
            if SALES_STATEMENT_KEY in objectKeyOriginal:
                documentUploadId = createDocumentUploadRecord("sales", objectKeyOriginal, fileName)
                updateDocumentUploadStatus(documentUploadId, "IN_QUEUE")
                
                try:
                    # NEW: Queue file and start Glue job instead of direct processing
                    queuePath = queueFileForProcessing(objectKeyOriginal, documentUploadId)
                    
                    runId = startProcessingGlueJob(documentUploadId, queuePath, "sales", fileName, SALES_STATEMENT_TABLE)
                    
                    # Archive original file
                    archiveFile(objectKeyOriginal)
                    
                    # Update status to processing
                    updateDocumentUploadStatus(documentUploadId, "PROCESSING", glueJobRunId=runId)
                    
                except Exception as ex:
                    updateDocumentUploadStatus(documentUploadId, "FAILED", str(ex))
                    raise ex

            elif ODOO_PAYMENT_KEY in objectKeyOriginal:
                documentUploadId = createDocumentUploadRecord("erp", objectKeyOriginal, fileName)
                updateDocumentUploadStatus(documentUploadId, "IN_QUEUE")
                
                try:
                    # NEW: Queue file and start Glue job instead of direct processing
                    queuePath = queueFileForProcessing(objectKeyOriginal, documentUploadId)
                    
                    runId = startProcessingGlueJob(documentUploadId, queuePath, "erp", fileName, ODOO_PAYMENT_TABLE)
                    
                    # Archive original file
                    archiveFile(objectKeyOriginal)
                    
                    # Update status to processing
                    updateDocumentUploadStatus(documentUploadId, "PROCESSING", glueJobRunId=runId)
                    
                except Exception as ex:
                    updateDocumentUploadStatus(documentUploadId, "FAILED", str(ex))
                    raise ex

            elif BANK_STATEMENT_KEY in objectKeyOriginal:
                documentUploadId = createDocumentUploadRecord("bank", objectKeyOriginal, fileName)
                updateDocumentUploadStatus(documentUploadId, "IN_PROGRESS")
                
                try:
                    bankStatementDataMap = readBankCsvFromS3(objectKeyOriginal)
                    createBankRecord(bankStatementDataMap, objectKeyOriginal, fileName)
                    # triggerReconciliation(MERCHANT_ID, bankStatementDataMap)
                    updateDocumentUploadStatus(documentUploadId, "COMPLETED", totalRecords=len(bankStatementDataMap))
                    archiveFile(objectKeyOriginal)
                except Exception as ex:
                    updateDocumentUploadStatus(documentUploadId, "FAILED", str(ex))
                    raise ex

            elif PAYMENT_TRANSACTION_KEY in objectKeyOriginal:
                documentUploadId = createDocumentUploadRecord("transaction", objectKeyOriginal, fileName)
                updateDocumentUploadStatus(documentUploadId, "IN_QUEUE")
                
                try:
                    # NEW: Queue file and start Glue job instead of direct processing
                    queuePath = queueFileForProcessing(objectKeyOriginal, documentUploadId)
                    
                    runId = startProcessingGlueJob(documentUploadId, queuePath, "transaction", fileName, PAYMENT_TRANSACTION_TABLE)
                    
                    # Archive original file
                    archiveFile(objectKeyOriginal)
                    
                    # Update status to processing
                    updateDocumentUploadStatus(documentUploadId, "PROCESSING", glueJobRunId=runId)
                    
                except Exception as ex:
                    updateDocumentUploadStatus(documentUploadId, "FAILED", str(ex))
                    raise ex
            elif CREDITCARD_SETTLEMENT_KEY in objectKeyOriginal:
                documentUploadId = createDocumentUploadRecord("credit_card_settlement", objectKeyOriginal, fileName)
                updateDocumentUploadStatus(documentUploadId, "IN_QUEUE")
                
                try:
                    # NEW: Queue file and start Glue job instead of direct processing
                    queuePath = queueFileForProcessing(objectKeyOriginal, documentUploadId)
                    
                    runId = startProcessingGlueJob(documentUploadId, queuePath, "credit_card_settlement", fileName, PAYMENT_TRANSACTION_TABLE)
                    
                    # Archive original file
                    archiveFile(objectKeyOriginal)
                    
                    # Update status to processing
                    updateDocumentUploadStatus(documentUploadId, "PROCESSING", glueJobRunId=runId)
                    
                except Exception as ex:
                    updateDocumentUploadStatus(documentUploadId, "FAILED", str(ex))
                    raise ex

            elif PAYMENT_TRANSFER_REPORT_KEY in objectKeyOriginal:
                documentUploadId = createDocumentUploadRecord("transfer", objectKeyOriginal, fileName)
                updateDocumentUploadStatus(documentUploadId, "IN_PROGRESS")
                
                try:
                    paymentTransferDataMap = readPaymentTransferReportCsvFromS3(objectKeyOriginal)
                    createPaymentTransferReportRecord(paymentTransferDataMap, objectKeyOriginal, fileName)
                    updateDocumentUploadStatus(documentUploadId, "COMPLETED", totalRecords=len(paymentTransferDataMap))
                    archiveFile(objectKeyOriginal)
                except Exception as ex:
                    updateDocumentUploadStatus(documentUploadId, "FAILED", str(ex))
                    raise ex
            elif STORE_KEY in objectKeyOriginal:
                storeDataMap = readStoreCsvFromS3(objectKeyOriginal)
                createStoreRecord(storeDataMap)
                archiveFile(objectKeyOriginal)

            # if [SALES_STATEMENT_KEY, BANK_STATEMENT_KEY, PAYMENT_TRANSFER_REPORT_KEY, PAYMENT_TRANSACTION_KEY, STORE_KEY] in objectKeyOriginal:
            #     archiveFile(objectKeyOriginal)

            else:
                # If file doesn't match any of the known document types, create a record with FAILED status
                documentUploadId = createDocumentUploadRecord("unknown", objectKeyOriginal, fileName)
                updateDocumentUploadStatus(documentUploadId, "FAILED", "Unsupported document type")

        return {
            "status": 200,
            "body": 'Successfully processed the files.'
        }

    except (BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException) as ex:
        logger.error(f"Custom error: {str(ex)}")
        return {
            "status": 400,
            "body": str(ex)
        }

    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {
            "status": 500,
            'body': "The server encountered an unexpected condition that prevented it from fulfilling your request."
        }

@tracer.capture_method
def extractDateTime(date_value: str, time_value: str) -> str:
    # Parse the date and time
    # Handle Transaction Date with 7 or 8 digits
    if len(date_value) == 7:
        # Parse the date in the format '5122024' (DMMYYYY)
        day = date_value[:1]
        month = date_value[1:3]
        year = date_value[3:]
    elif len(date_value) == 8:
        # Parse the date in the format '05122024' (DDMMYYYY)
        day = date_value[:2]
        month = date_value[2:4]
        year = date_value[4:]
    else:   
        date_value = "-"

    # Handle Transaction Time with 4, 5, or 6 digits
    if len(time_value) == 4:
        # Format time as HH:MM
        formatted_time = f"{time_value[:2]}:{time_value[2:]}"
    elif len(time_value) == 5:
        # Format time as HH:MM:SS with seconds defaulted to 0 and add leading 0 to single digit hour
        formatted_time = f"0{time_value[:1]}:{time_value[1:3]}:{time_value[3:]}"
    elif len(time_value) == 6:
        # Format time as HH:MM:SS
        formatted_time = f"{time_value[:2]}:{time_value[2:4]}:{time_value[4:]}"
    else:
        formatted_time = "-"
    
    return day, month, year, date_value, formatted_time

@tracer.capture_method
def formatDateTime(date: str, time: str) -> str:
    """
    Formats the date and time strings into a single datetime string.
    
    Supports multiple date formats including:
    - Standard formats parsed by dateutil.parser
    - dd/mm/yy format (e.g., 25/12/22)
    - dd/mm/yyyy format (e.g., 25/12/2022)
    
    Args:
        date (str): The date string.
        time (str): The time string.
        
    Returns:
        str: The formatted datetime string in ISO 8601 format with Z suffix.
    """
    if not date:
        return "-"
    
    try:
        # Check for dd/mm/yy or dd/mm/yyyy format
        if isinstance(date, str) and '/' in date:
            parts = date.split('/')
            if len(parts) == 3:
                day, month, year = parts
                
                # Handle 2-digit years
                if len(year) == 2:
                    current_year = datetime.now().year
                    century = current_year // 100
                    year_int = int(year)
                    
                    # If the 2-digit year is greater than 50 years in the future,
                    # assume it's from the previous century
                    if year_int + century * 100 > current_year + 50:
                        year = str((century - 1) * 100 + year_int)
                    else:
                        year = str(century * 100 + year_int)
                
                # Explicitly construct the date in yyyy-mm-dd format
                date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        # Continue with existing logic
        if date and time:
            combined_datetime = f"{date} {time}"
            parsed_datetime = parser.parse(combined_datetime)
            return parsed_datetime.astimezone().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        elif date:
            parsed_datetime = parser.parse(date)
            adjusted_date = parsed_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
            return adjusted_date.astimezone().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except Exception as e:
        logger.warning(f"Error parsing date '{date}' and time '{time}': {str(e)}")
        return "-"
        
    return "-"

@tracer.capture_method        
def extract_vendor_ref(text: str) -> str:
    if isinstance(text, str):
        parts = text.split()
        if len(parts) >= 3 and parts[0] == "GP":
            return parts[1]  # Return the middle value
    return text  # Return the original value if conditions not met

# Read and clean Store/Outlet data
@tracer.capture_method
def readStoreCsvFromS3(fileKey: str) -> List[Dict]:
    try:
        # Fetch the CSV file from S3
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey.replace('+', ' '))
        csvContent = pd.read_csv(response['Body'], dtype=str, encoding='utf-8').to_dict('records')

        mappedStoreContent = []
        seenStoreNumber = set()  # To track duplicate Store Number

        # Field mapping for the CSV columns
        field_mapping = {
            'Outlet Name': 'branchName',
            'Outlet Code': 'branchCode',
            'Outlet Code ®': 'alternativeBranchCode',
            'Payment Merchant': 'paymentMerchant',
            'Sales Channel': 'salesChannel',
            'Bank Name': 'bankName',
            'Terminal ID': 'terminalId',
            'Payment Method': 'paymentMethod',
            'Merchant ID': 'merchantId',
        }
        
        # Iterate through each row in the CSV
        for record in csvContent:
            # Clean and normalize the record
            record = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            storeNumber = record.get('No')

            # Skip rows with duplicate No
            if storeNumber in seenStoreNumber:
                continue
            seenStoreNumber.add(storeNumber)

            # Map the record to the desired format
            mappedRecord = {}
            for key, mappedKey in field_mapping.items():
                    mappedRecord[mappedKey] = str(getCellValue(record, key))

            # Append the mapped record to the list
            mappedStoreContent.append(mappedRecord)

        return mappedStoreContent

    except Exception as ex:
        logger.exception(f"Error reading Store CSV from S3: {str(ex)}")
        return []

# Read and Clean Odoo data 
@tracer.capture_method
def readOdooPaymentReportCsvFromS3(fileKey: str) -> List[Dict]:
    """
    Reads and processes the Odoo Payment Report CSV file from S3.

    Args:
        fileKey (str): The S3 key of the Odoo Payment Report CSV file.

    Returns:
        List[Dict]: A list of dictionaries containing the processed Odoo Payment Report data.
    """
    try:
        # Fetch the CSV file from S3
        response = S3_CLIENT.get_object(
            Bucket=S3_BUCKET_NAME, Key=fileKey.replace('+', ' '))
        csvContent = pd.read_csv(
            response['Body'], dtype=str, encoding='utf-8').to_dict('records')

        mappedOdooPaymentReportContent = []

        # Field mapping for the CSV columns
        field_mapping = {
            'Outlet Code': 'branchCode',
            'Analytic Account/Display Name': 'erpDisplayName',
            'Date': 'reportDateTime',
            'Amount': 'amount',
            'Label': 'transactionDateTime'
        }

        # Iterate through each row in the CSV
        for record in csvContent:
            # Clean and normalize the record
            record = {key.strip(): (value.strip() if isinstance(
                value, str) else value) for key, value in record.items()}

            # Map the record to the desired format
            mappedRecord = {}
            for key, mappedKey in field_mapping.items():
                if key == 'Date':
                    # Parse and format date-time fields
                    date_value = getCellValue(record, key)
                    order_date_time = formatDateTime(date_value, None)
                    mappedRecord[mappedKey] = order_date_time

                elif key == 'Outlet Code':
                    # Extract branchCode from 'Analytic Account/Display Name' if it matches the pattern R-XXXX
                    displayName = record.get('Analytic Account/Display Name', '')
                    match = re.search(r'R-([A-Z0-9]{4})', displayName)
                    if match:
                        mappedRecord['branchCode'] = match.group(1)  # Extract the 4 characters after R-
                    else:
                        mappedRecord['branchCode'] = "-"
                elif key == 'Amount':
                    credit_value = getCellValue(record, 'Credit', None)
                    debit_value = getCellValue(record, 'Debit', None)

                    if debit_value:
                        # If 'Debit' has a value, map it to 'amount'
                        mappedRecord[mappedKey] = Decimal(debit_value)
                        mappedRecord['amountType'] = "DEBIT"

                    elif credit_value:
                        # If 'Credit' has a value, map it to 'amount'
                        mappedRecord[mappedKey] = Decimal(credit_value)
                        mappedRecord['amountType'] = "CREDIT"
                    else:
                        # If both are empty, set 'amount' to 0
                        mappedRecord[mappedKey] = Decimal('0.00')
                elif key == 'Label':
                    # Parse and format date-time fields
                    label_value = getCellValue(record, key)

                    date_value = extractAndConvertDate(label_value)

                    transaction_date_time = formatDateTime(date_value, None)

                    mappedRecord[mappedKey] = transaction_date_time
                    mappedRecord['label'] = label_value
                else:
                    mappedRecord[mappedKey] = str(getCellValue(record, key, "-"))

                # Set 'type' based on file name
                lower_file_name = fileKey.lower()
                if 'grabpay' in lower_file_name:
                    mappedRecord['type'] = 'GRABPAY'
                elif 'grabfood' in lower_file_name:
                    mappedRecord['type'] = 'GRABFOOD'

            # Append the mapped record to the list
            mappedOdooPaymentReportContent.append(mappedRecord)

        return mappedOdooPaymentReportContent

    except Exception as ex:
        logger.exception(f"Error reading Odoo Payment Report CSV from S3: {str(ex)}")
        return []

# Read and clean POS data
@tracer.capture_method
# def readPosCsvFromS3(salesFileKey: str, odooFileKey: str) -> List[Dict]:
def readPosCsvFromS3(salesFileKey: str) -> List[Dict]:
    try:
        # Fetch the CSV file from S3
        salesResponse = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=salesFileKey.replace('+', ' '))
        salesCsvContent = pd.read_csv(salesResponse['Body'], dtype=str).to_dict('records')

        mappedPOSContent = []
        seenInvoiceNumbers = set()  # To track duplicate invoice numbers

        # Field mapping for the CSV columns
        sales_field_mapping = {
            'Outlet': 'branchName',
            'Outlet Code': 'branchCode',
            'Order Date': 'orderDateTime',
            'System Order ID': 'systemOrderId',
            'Invoice Number': 'invoiceNumber',
            'Is Cancel Receipt': 'salesStatus',
            'Non-Sale': 'salesType',
            'Payment Amount': 'totalPayableAmount',
            'Net Sales After Payment Rounding': 'totalSalesAmount',
            'Tax After Discount': 'totalTaxAmount',
            "Debit/Credit Card - RHB Terminal": "RHB_DEBIT_CREDIT_CARD",
            "Debit/Credit Card - CIMB Terminal": "CIMB_DEBIT_CREDIT_CARD",
            "Debit/Credit Card - Bank Rakyat Terminal": "BANK_RAKYAT_DEBIT_CREDIT_CARD",
            "Debit/Credit Card - Ambank Terminal": "AMBANK_DEBIT_CREDIT_CARD",
            "Debit/Credit Card - Affin Terminal": "AFFIN_DEBIT_CREDIT_CARD",
            "CIMB Bonus Point": "CIMB_BONUS_POINT",
            "DuitNow QR": "DUITNOW_QR",
            "Ambank QR": "AMBANK_QR",
            "ShopeeFood": "SHOPEE_FOOD",
            "ShopBack": "SHOPEE_BACK",
            "Retail Cash": "CASH",
            "GrabPay": "GRABPAY",
            "Shopee Pay": "SHOPEE_PAY",
            "Touch N Go": "TNG",
            "Affin QR Code": "AFFIN_QR_CODE",
            "GrabFood / GrabCake - Campaign Testing": "GRABFOOD_GRABCAKE_CAMPAIGN_TESTING",
            "GrabFood / GrabCake": "GRABFOOD",
            "FoodPanda": "FOODPANDA",
            "DeliverEat": "DELIVEREAT",
            "WhatsApp": "WHATSAPP",
            "Air-Asia Food": "AIRASIA_FOOD",
            "Mall Voucher": "MALL_VOUCHER",
            "BR Voucher": "BR_VOUCHER",
            "Payment To HQ": "PAYMENT_TO_HQ",
            "Sunway Pals": "SUNWAY_PALS",
            "Setel Deliver2Me": "SETEL_DELIVER2ME",
            "OneShop": "ONESHOP",
            "Redemption OneCard": "REDEMPTION_ONECARD",
            "1Pay E-Wallet": "1PAY_EWALLET",
            "KM App": "KM_APP",
            "Redemption Genting": "REDEMPTION_GENTING",
            "Redemption Sunway": "REDEMPTION_SUNWAY",
            "Mobile App": "MOBILE_APP",
            "Kiosk": "KIOSK",
            "WebComm": "WEBCOMM",
            "IPay88": "IPAY88",
            "Voucher Redemption (Product)": "VOUCHER_REDEMPTION_PRODUCT",
            "Voucher Redemption (Cash)": "VOUCHER_REDEMPTION_CASH",
            "Alipay": "ALIPAY",
            "Debit/Credit Card - MBB Terminal": "DEBIT_CREDIT_CARD_MBB",
            "PrimePay": "PRIMEPAY",
            "Maybank Treatspoints": "MAYBANK_TREAT_POINTS",
            "HSBC Birthday - Free Junior Scoop": "HSBC_BIRTHDAY_FREE_JUNIOR_SCOOP",
            "Lazada E-Wallet": "LAZADA_EWALLET",
            "Mesra Card": "MESRA_CARD"
        }

        # Fields that need to be converted to integers or floats
        float_fields = [
            # 'Payment Amount',
            'Tax After Discount',
            'Debit',
            'Credit',
            'Net Sales After Payment Rounding',
            'Tax After Discount'
        ]

        # Iterate through each row in the SALES CSV
        for record in salesCsvContent:
            # Clean and normalize the record
            record = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in record.items()}

            invoiceNumber = record.get('Invoice Number')
            # Skip rows with duplicate invoice numbers
            if invoiceNumber in seenInvoiceNumbers:
                continue
            seenInvoiceNumbers.add(invoiceNumber)
 
            # Map the record to the desired format
            mappedRecord = {}
            for key, mappedKey in sales_field_mapping.items():
                if key in float_fields:
                    value = getCellValue(record, key, 0)
                    if isinstance(value, str):
                        # Remove commas from numeric values
                        value = value.replace(',', '')
                    # Convert to Decimal for precision
                    
                    mappedRecord[mappedKey] = Decimal(str(value))
                
                # elif key == 'Analytic Account':
                #     # Extract branchCode from 'Analytic Account' if it matches the pattern R-XXXX
                #     displayName = record.get('Analytic Account', '')
                #     match = re.search(r'R-([A-Z0-9]{4})', displayName)
                #     if match:
                #         mappedRecord['branchCode'] = match.group(1)  # Extract the 4 characters after R-
                #     else:
                #         mappedRecord['branchCode'] = "-"

                elif key == 'Order Date':
                    order_date = getCellValue(record, 'Order Date')
                    order_time = getCellValue(record, 'Order Time')
                    order_date_time = formatDateTime(order_date, order_time)
                    mappedRecord['orderDateTime'] = order_date_time
                
                elif key in PAYMENT_METHOD_MAPPING:
                    value = getCellValue(record, key)
                    if value:
                        mappedRecord['paymentMethod'] = PAYMENT_METHOD_MAPPING[key]

                        if isinstance(value, str):
                            # Remove commas from numeric values
                            value = value.replace(',', '')

                        mappedRecord['totalPayableAmount'] = Decimal(str(value))
                
                elif key == 'Is Cancel Receipt':
                    value = getCellValue(record, key, "FALSE")
                    mappedRecord[mappedKey] = IS_CANCEL_RECEIPT.get(value, "PAID")
                
                elif key == 'Non-Sale':
                    value = getCellValue(record, key, "FALSE")
                    mappedRecord[mappedKey] = SALES_TYPE.get(value, "SALES")
                else:
                    mappedRecord[mappedKey] = str(
                        getCellValue(record, key, "-"))

            # Append the mapped record to the list
            mappedPOSContent.append(mappedRecord)

        return mappedPOSContent

    except Exception as ex:
        logger.exception(f"Error reading POS CSV from S3: {str(ex)}")
        return []

# Read and clean Bank data
@tracer.capture_method
def readBankCsvFromS3(fileKey: str) -> List[Dict]:
    try:
        # Fetch the CSV file from S3
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey.replace('+', ' '))
        csvContent = pd.read_csv(response['Body'], dtype=str).to_dict('records')

        mappedBankContent = []
        seenBankRefs = set()  # To track duplicate Document Reference Number

        # Field mapping for the CSV columns
        field_mapping = {
            'Transaction Date': 'transactionDateTime',  
            'Sender Name': 'statementType',
            'Transaction Amount': 'transactionAmount',
            'Transaction Amount Type': 'transactionType',
            'Other Payment Details': 'vendorRef',
            'Account Number': 'accountNumber',
            'Record Sequence Number': 'sequence',
            'Document Reference Number': 'bankRef',
        }   # Account Number, Record Sequence Number, Document Reference Number must be valid integers
        
        float_fields = ['Transaction Amount']

        # Iterate through each row in the CSV
        for record in csvContent:
            # Clean and normalize the record
            record = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            bankRef = record.get('Document Reference Number')

            # Skip rows with duplicate Document Reference Number
            if bankRef in seenBankRefs:
                continue
            seenBankRefs.add(bankRef)

            # Map the record to the desired format
            mappedRecord = {}
            for key, mappedKey in field_mapping.items():
                
                if key in float_fields:
                    value = getCellValue(record, key, 0)
                    if isinstance(value, str):
                        # Remove commas from numeric values
                        value = value.replace(',', '')
                    # Convert to Decimal for precision
                    mappedRecord[mappedKey] = Decimal(str(value))

                elif key == 'Other Payment Details':
                    mappedRecord[mappedKey] = extract_vendor_ref(getCellValue(record, key))
                
                elif key == 'Transaction Date':
                    date_value = getCellValue(record, key)
                    # Get the Transaction Time value
                    time_value = getCellValue(record, 'Transaction Time')
                    if date_value and time_value:
                        day, month, year, date_value, formatted_time = extractDateTime(date_value, time_value)
                        if(date_value != "-" and formatted_time != "-"):    
                            # # Combine date and time
                            mappedRecord[mappedKey] = formatDateTime(f"{year}-{month}-{day}", formatted_time)   
                    else:
                        mappedRecord[mappedKey] = "-"

                elif key == 'Transaction Amount Type':
                    # Map 'Credit' or 'Debit' based on 'Transaction Amount Type'
                    transaction_amount_type = getCellValue(record, 'Transaction Amount Type', '-')
                    mappedRecord[mappedKey] = TRANSACTION_AMOUNT_TYPE.get(transaction_amount_type, '-')
                else:
                    mappedRecord[mappedKey] = str(getCellValue(record, key, "-"))

            # Append the mapped record to the list
            mappedBankContent.append(mappedRecord)

        return mappedBankContent

    except Exception as ex:
        logger.exception(f"Error reading Bank CSV from S3: {str(ex)}")
        return []

# Read and clean Transaction data
@tracer.capture_method
def readPaymentTransactionCsvFromS3(fileKey: str) -> List[Dict]:
    try:
        # Fetch the CSV file from S3
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey.replace('+', ' '))
        csvContent = pd.read_csv(response['Body'], dtype=str).to_dict('records')

        mappedPaymentTransactionContent = []
        seenTransactionIds = set()  # To track duplicate transaction IDs

        # Field mapping for the CSV columns
        field_mapping = {
            'Store Name': 'branchName',
            'Store ID': 'branchUUID',
            'Transaction ID': 'transactionId',
            'Type': 'gatewayTransactionType',
            'Category': 'gatewayTransactionCategory',
            'Status': 'status',
            'Created On': 'paymentDateTime',
            'Transfer Date': 'settlementDateTime',
            'Settlement ID': 'settlementId',
            'Net Sales': 'salesNetAmount',
            'Net MDR': 'processingFee',
            'Total': 'creditAmount',
            'Payment Method': 'paymentMethod',
            'Cancelled by': 'cancelBy',
            'Cancellation Reason': 'cancelReason',
            'Reason for Refund': 'refundReason'
        }

        # Fields that need to be converted to integers or floats
        float_fields = [
            'Net Sales',
            'Net MDR',
            'Total'
        ]

        # Iterate through each row in the CSV
        for record in csvContent:
            # Clean and normalize the record
            record = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            transactionId = record.get('Transaction ID')

            # Skip rows with duplicate transaction IDs
            if transactionId in seenTransactionIds:
                continue
            seenTransactionIds.add(transactionId)

            # Map the record to the desired format
            mappedRecord = {}
            for key, mappedKey in field_mapping.items():
                
                if key in float_fields:
                    value = getCellValue(record, key, 0)
                    if isinstance(value, str):
                        # Remove commas from numeric values
                        value = value.replace(',', '')
                    # Convert to Decimal for precision
                    mappedRecord[mappedKey] = Decimal(str(value))
                
                elif key in ['Created On', 'Transfer Date']:
                    # Parse and format date-time fields
                    date_value = getCellValue(record, key)
                    if date_value:
                        parsed_date = parser.parse(date_value)
                        # Format as UTC ISO 8601
                        mappedRecord[mappedKey] = parsed_date.astimezone().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    else:
                        mappedRecord[mappedKey] = "-"
                
                # Check if 'Reason for Refund' field is present
                # If 'Reason for Refund' present means it is a refund transaction
                elif key == 'Reason for Refund':
                    reason_for_refund = getCellValue(record, key, None)
                    if reason_for_refund is None:
                        mappedRecord['transactionType'] = TRANSACTION_TYPE['Payment']
                    else:
                        mappedRecord['transactionType'] = TRANSACTION_TYPE['Payment']
                
                elif key == 'Type':
                    type = getCellValue(record, key, None)
                    mappedRecord['gatewayTransactionType'] = GATEWAY_TRANSACTION_TYPE.get(type, '-')
               
                elif key == 'Category':
                    category = getCellValue(record, key, None)
                    mappedRecord['gatewayTransactionCategory'] = GATEWAY_TRANSACTION_GATEGORY.get(category, '-')
                
                elif key == 'Payment Method':
                    payment_method = getCellValue(record, key, None)
                    mappedRecord[mappedKey] = PAYMENT_METHOD.get(payment_method, '-')
                
                elif key == 'Status':
                    status = getCellValue(record, key, None)
                    mappedRecord[mappedKey] = TRANSACTION_STATUS.get(status, '-')
                
                else:
                    mappedRecord[mappedKey] = str(getCellValue(record, key, "-"))

            # Append the mapped record to the list
            mappedPaymentTransactionContent.append(mappedRecord)

        return mappedPaymentTransactionContent

    except Exception as ex:
        logger.exception(f"Error reading Payment Transaction CSV from S3: {str(ex)}")
        return []

# Read and clean Gateway Report data
@tracer.capture_method
def readPaymentTransferReportCsvFromS3(gatewayFileKey: str) -> List[Dict]:
    try:
        # Fetch the CSV file from S3
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=gatewayFileKey.replace('+', ' '))
        csvContent = pd.read_csv(response['Body'], dtype=str).to_dict('records')

        mappedPaymentGatewayReportContent = []
        seenSettlementIds = set()  # To track duplicate settlement IDs


        # Field mapping for the CSV columns
        field_mapping = {
            'Date': 'paymentReportDateTime',
            'Store ID': 'branchUUID',
            'Store Name': 'branchName',
            'Settlement ID': 'settlementId',
            'Net Total': 'totalCreditAmount',
            'Transfer Date': 'settlementDateTime',
            'Bank Statement Code': 'vendorRef',
            'Bank Name': 'bankName',
            'Bank Account': 'bankAccount'
        }

        # Fields that need to be converted to integers or floats
        float_fields = ['Net Total']

        # Iterate through each row in the CSV
        for record in csvContent:
            # Clean and normalize the record
            record = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            settlementId = record.get('Settlement ID')

            # Skip rows with duplicate settlement IDs
            if settlementId in seenSettlementIds:
                continue
            seenSettlementIds.add(settlementId)

            # Map the record to the desired format
            mappedRecord = {}
            for key, mappedKey in field_mapping.items():
                if key in float_fields:
                    value = getCellValue(record, key, 0)
                    if isinstance(value, str):
                        # Remove commas from numeric values
                        value = value.replace(',', '')
                    # Convert to Decimal for precision
                    mappedRecord[mappedKey] = Decimal(str(value))
                
                elif key in ['Date', 'Transfer Date']:
                    # Parse and format date-time fields
                    date_value = getCellValue(record, key)
                    if date_value:
                        parsed_date = parser.parse(date_value)
                        mappedRecord[mappedKey] = parsed_date.astimezone().strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Format as UTC ISO 8601
                    else:
                        mappedRecord[mappedKey] = "-"
                else:
                    mappedRecord[mappedKey] = str(getCellValue(record, key, "-"))

            # Append the mapped record to the list
            mappedPaymentGatewayReportContent.append(mappedRecord)
            
        return mappedPaymentGatewayReportContent

    except Exception as ex:
        logger.exception(f"Error reading Payment Gateway Report CSV from S3: {str(ex)}")
        return []

@tracer.capture_method
def createStoreRecord(storeDataList: List[Dict]):
    """
    Inserts each dictionary from the list into the DynamoDB table as a record.

    Args:
        storeDataList (List[Dict]): List of dictionaries containing POS data.
    """
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    for record in storeDataList:
        # Add additional metadata to the record if needed
        record['storeId'] = str(uuid.uuid4())  # Unique ID for the record
        record['merchantId'] = MERCHANT_ID
        record['createdAt'] = now
        record['createdBy'] = "System"
        record['updatedAt'] = now
        record['updatedBy'] = "System"

        # Insert the record into the DynamoDB table
        STORE_DDB_TABLE.put_item(Item=record)



@tracer.capture_method
def createOdooPaymentReportRecord(odooPaymentReportList: List[Dict], fileKey: str, targetSourceFile: str):
    """
    Inserts each dictionary from the list into the DynamoDB table as a record.

    Args:
        odooPaymentReportList (List[Dict]): List of dictionaries containing Odoo Payment Report data.
        fileKey (str): The file path of the source file.
        targetSourceFile (str): The name of the source file.
    """
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Current UTC timestamp
    documentUploadId = str(uuid.uuid4())  # Unique ID for the record

    for record in odooPaymentReportList:
        record['paymentReportErpId'] = str(uuid.uuid4())
        record['merchantId'] = MERCHANT_ID
        record['documentUploadId'] = documentUploadId
        record['filePath'] = fileKey
        record['sourceFile'] = targetSourceFile
        record['createdAt'] = now
        record['createdBy'] = "System"
        record['updatedAt'] = now
        record['updatedBy'] = "System"

        ODOO_PAYMENT_DDB_TABLE.put_item(Item=record)
        

@tracer.capture_method
def getPosRecord(invoiceNumber: str) -> Dict:
    """
    Fetches a record from the DynamoDB table based on the invoice number.

    Args:
        invoiceNumber (str): The invoice number to search for.

    Returns:
        Dict: The record corresponding to the invoice number.
    """
    response = SALES_STATEMENT_DDB_TABLE.query(
        IndexName='gsi-merchantId-invoiceNumber',
        KeyConditionExpression=Key('merchantId').eq(MERCHANT_ID) & Key('invoiceNumber').eq(invoiceNumber),
    ).get('Items', [])
    if not response:
        return False
    return True
    
@tracer.capture_method
def createPosRecord(posDataList: List[Dict], fileKey: str, targetSourceFile: str):
    """
    Inserts each dictionary from the list into the DynamoDB table as a record.

    Args:
        posDataList (List[Dict]): List of dictionaries containing POS data.
    """
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    documentUploadId = str(uuid.uuid4())  # Unique ID for the record
    for record in posDataList:
        invoiceNumber = record.get('invoiceNumber')
        # Check if the invoice number already exists in the database
        if getPosRecord(invoiceNumber):
            continue
        # Add additional metadata to the record if needed
        record['salesStatementId'] = str(uuid.uuid4())  # Unique ID for the record
        record['merchantId'] = MERCHANT_ID
        record['currency'] = "MYR"
        record['taxCode'] = "GST"
        record['documentUploadId'] = documentUploadId  # Unique ID for the record

        record['filePath'] = fileKey
        record['sourceFile'] = targetSourceFile
        record['createdAt'] = now
        record['createdBy'] = "System"
        record['updatedAt'] = now
        record['updatedBy'] = "System"

        # Insert the record into the DynamoDB table
        SALES_STATEMENT_DDB_TABLE.put_item(Item=record)

@tracer.capture_method
def getBankRecord(bankRef: str) -> Dict:
    """
    Fetches a record from the DynamoDB table based on the bank reference number.

    Args:
        bankRef (str): The bank reference number to search for.

    Returns:
        Dict: The record corresponding to the bank reference number.
    """
    response = BANK_STATEMENT_DDB_TABLE.query(
        IndexName='gsi-merchantId-bankRef',
        KeyConditionExpression=Key('merchantId').eq(MERCHANT_ID) & Key('bankRef').eq(bankRef),
    ).get('Items', [])
    if not response:
        return False
    return True

@tracer.capture_method
def createBankRecord(bankDataList: List[Dict], fileKey: str, targetSourceFile: str):
    """
    Inserts each dictionary from the list into the DynamoDB table as a record.

    Args:
        bankDataList (List[Dict]): List of dictionaries containing bank data.
        fileKey (str): The file path of the source file.
        targetSourceFile (str): The name of the source file.
    """
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    documentUploadId = str(uuid.uuid4())  # Unique ID for the record
    for record in bankDataList:
        bankRef = record.get('bankRef')
        # Check if the bank reference number already exists in the database
        if getBankRecord(bankRef):
            continue
        # Add additional metadata to the record if needed
        record['bankStatementId'] = str(uuid.uuid4())  # Unique ID for the record
        record['merchantId'] = MERCHANT_ID
        record['bankName'] = "CIMB Bank Berhad"
        record['bankNameCode'] = "CIMB_I"
        record['currency'] = "MYR"
        record['documentUploadId'] = documentUploadId
        record['filePath'] = fileKey
        record['sourceFile'] = targetSourceFile
        record['createdAt'] = now
        record['createdBy'] = "System"
        record['updatedAt'] = now
        record['updatedBy'] = "System"

        # Insert the record into the DynamoDB table
        BANK_STATEMENT_DDB_TABLE.put_item(Item=record)

@tracer.capture_method
def getPaymentTransactionRecord(transactionId: str) -> Dict:
    """
    Fetches a record from the DynamoDB table based on the payment transaction ID.

    Args:
        paymentTransactionId (str): The payment transaction ID to search for.

    Returns:
        Dict: The record corresponding to the payment transaction ID.
    """
    response = PAYMENT_TRANSACTION_DDB_TABLE.query(
        IndexName='gsi-merchantId-transactionId',
        KeyConditionExpression=Key('merchantId').eq(MERCHANT_ID) & Key('transactionId').eq(transactionId),
    ).get('Items', [])
    if not response:
        return False
    return True
    
@tracer.capture_method
def createPaymentTransactionRecord(paymentTransactionList: List[Dict], fileKey: str, targetSourceFile: str):
    """
    Inserts each dictionary from the list into the DynamoDB table as a record.

    Args:
        paymentTransactionList (List[Dict]): List of dictionaries containing payment transaction data.
        fileKey (str): The file path of the source file.
        targetSourceFile (str): The name of the source file.
    """
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Current UTC timestamp
    documentUploadId = str(uuid.uuid4())  # Unique ID for the record
    for record in paymentTransactionList:
        transactionId = record.get('transactionId')
        # Check if the payment transaction ID already exists in the database
        if getPaymentTransactionRecord(transactionId):
            continue
         # Add additional metadata to the record if needed
        record['paymentTransactionId'] = str(uuid.uuid4())  # Unique ID for the record
        record['merchantId'] = MERCHANT_ID
        record['bankName'] = "CIMB Bank Berhad"
        record['bankNameCode'] = "CIMB_I"
        record['currency'] = record.get('currency', 'MYR')  # Default currency
        
        # Store branch name in type-specific attributes based on the gateway transaction type
        gateway_type = record.get('gatewayTransactionType', '')
        branch_name = record.get('branchName', '')
        
        # Look up branch code from the store index if only branchName is available but branchCode is missing
        if 'branchName' in record and ('branchCode' not in record or not record['branchCode'] or record['branchCode'] == "-"):
            # Use the appropriate attribute for branch code lookup based on the type
            if gateway_type == 'GRABPAY':
                branch_code = lookupBranchCode(branch_name, 'grabMerchantName')
            elif gateway_type == 'GRABFOOD':
                branch_code = lookupBranchCode(branch_name, 'grabFoodName')
            elif gateway_type == 'GRABMART':
                branch_code = lookupBranchCode(branch_name, 'grabMartName')
            else:
                branch_code = lookupBranchCode(branch_name)
            
            if branch_code:
                record['branchCode'] = branch_code
        
        record['documentUploadId'] = documentUploadId
        record['filePath'] = fileKey
        record['sourceFile'] = targetSourceFile
        record['createdAt'] = now
        record['createdBy'] = "System"
        record['updatedAt'] = now
        record['updatedBy'] = "System"

        # Insert the record into the DynamoDB table
        PAYMENT_TRANSACTION_DDB_TABLE.put_item(Item=record)

@tracer.capture_method
def getPaymentGatewayReportRecord(settlementId: str) -> Dict:
    """
    Fetches a record from the DynamoDB table based on the settlement ID.

    Args:
        settlementId (str): The settlement ID to search for.

    Returns:
        Dict: The record corresponding to the settlement ID.
    """
    response = PAYMENT_TRANSFER_REPORT_DDB_TABLE.query(
        IndexName='gsi-merchantId-settlementId',
        KeyConditionExpression=Key('merchantId').eq(MERCHANT_ID) & Key('settlementId').eq(settlementId),
    ).get('Items', [])
    if not response:
        return False
    return True
    
@tracer.capture_method
def createPaymentTransferReportRecord(paymentGatewayReportList: List[Dict], fileKey: str, targetSourceFile: str):
    """
    Inserts each dictionary from the list into the DynamoDB table as a record.

    Args:
        paymentGatewayReportList (List[Dict]): List of dictionaries containing payment gateway report data.
        fileKey (str): The file path of the source file.
        targetSourceFile (str): The name of the source file.
    """
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Current UTC timestamp
    documentUploadId = str(uuid.uuid4())  # Unique ID for the record
    for record in paymentGatewayReportList:
        settlementId = record.get('settlementId')
        # Check if the settlement ID already exists in the database
        if getPaymentGatewayReportRecord(settlementId):
            continue
        # Add additional metadata to the record if needed
        record['paymentGatewayReportId'] = str(uuid.uuid4())  # Unique ID for the record
        record['merchantId'] = MERCHANT_ID
        record['currency'] = record.get('currency', 'MYR')  # Default currency
        record['documentUploadId'] = documentUploadId
        record['bankNameCode'] = "CIMB_I"
        record['filePath'] = fileKey
        record['sourceFile'] = targetSourceFile
        record['createdAt'] = now
        record['createdBy'] = "System"
        record['updatedAt'] = now
        record['updatedBy'] = "System"

        # Insert the record into the DynamoDB table
        PAYMENT_TRANSFER_REPORT_DDB_TABLE.put_item(Item=record)

# Get value from cell
@tracer.capture_method
def getCellValue(row, column, default=None):
    cell = row[column]
    if not pd.isna(cell):
        return cell
    else:
        return default
    
@tracer.capture_method
def triggerReconciliation(merchantId, bankStatementMapData):
    """
    Delivers a payload to SQS with a calculated startDate based on the earliest transactionDateTime.

    Args:
        merchantId (str): The merchant ID.
        bankStatementMapData (List[Dict]): List of bank statement data.
    """
    try:
        now = datetime.now()
        earliest_startDate_str = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")  
        earliest_startDate = datetime.strptime(earliest_startDate_str, "%Y-%m-%dT%H:%M:%S.%fZ")

        # Parse transactionDateTime and find the earliest date
        for record in bankStatementMapData:
            transaction_date_str = record.get('transactionDateTime', None)

            if transaction_date_str:
                try:
                    # Parse the transactionDateTime string into a datetime object
                    transaction_date = datetime.strptime(transaction_date_str, "%Y-%m-%dT%H:%M:%S.%fZ")

                    # Update earliest_startDate if a new earlier date is found
                    if transaction_date < earliest_startDate:
                        earliest_startDate = transaction_date
                except Exception as ex:
                    logger.warning(f"Invalid transactionDateTime format: {transaction_date_str}. Skipping.")

        startDate_str = earliest_startDate.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # Prepare the payload
        payload = {
            'merchantId': merchantId,
            'startDate': startDate_str,
            'endDate': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
        # Send SQS message
        response = sendToSQS(payload)
    except Exception as ex:
        logger.exception(f"Error delivering payload to SQS: {str(ex)}")
        
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
def archiveFile(key: str):
    """
    Archive the file by copying it to the archive folder in S3
    """
    copy_source = {
        'Bucket': S3_BUCKET_NAME,
        'Key': key
    }
    now = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    fileName = key.split("/")[-1]
    newFileName = fileName.replace(".csv", f"_{now}.csv")
    newKey = key.replace("reconciliation/input/", "reconciliation/archive/")
    newKey = newKey.replace(fileName, newFileName)
    S3_CLIENT.copy_object(Bucket=S3_BUCKET_NAME,
                          CopySource=copy_source, Key=newKey)
    S3_CLIENT.delete_object(Bucket=S3_BUCKET_NAME, Key=key)

@tracer.capture_method
def createDocumentUploadRecord(documentType: str, filePath: str = "", fileName: str = ""):
    """
    Creates a document upload record with status PENDING.
    
    Args:
        documentType (str): Type of document ("sales", "transaction", "transfer", "bank", or "erp")
        filePath (str): Path to the file in S3
        fileName (str): Name of the file
    
    Returns:
        str: The documentUploadId of the created record
    """
    valid_types = ["sales", "transaction", "transfer", "bank", "erp"]
    if documentType not in valid_types:
        logger.warning(f"Invalid document type: {documentType}")
        documentType = "sales"  # Default to sales if invalid
    
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    documentUploadId = str(uuid.uuid4())
    
    # Create the document upload record
    record = {
        'documentUploadId': documentUploadId,
        'merchantId': MERCHANT_ID,
        'documentType': documentType,
        'source': "sftp",
        'inputSource': "System",
        'avgConfidenceScore': 0,
        'confidenceScoreList': [0],
        'status': "PENDING",
        'fileName': fileName,
        'inputPath': filePath,
        'folder': "reconciliation",
        'errorPath': "",
        'invalidPath': "",
        'exceptionStatus': "",
        'totalRecords': 0,
        'createdAt': now,
        'createdBy': "System",
        'updatedAt': now,
        'updatedBy': "System"
    }
    
    # Insert the record into DynamoDB
    DOCUMENT_UPLOAD_DDB_TABLE.put_item(Item=record)
    
    return documentUploadId

@tracer.capture_method
def updateDocumentUploadStatus(documentUploadId: str, status: str, exceptionStatus: str = "", **kwargs):
    """
    Updates the status of a document upload record.
    
    Args:
        documentUploadId (str): The ID of the document upload record to update
        status (str): New status ("PENDING", "FAILED", "IN_PROGRESS", or "COMPLETED")
        exceptionStatus (str): Optional error message for failed records
    """
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    update_expression = "SET #status = :status, #updatedAt = :updatedAt"
    expression_attribute_names = {
        "#status": "status",
        "#updatedAt": "updatedAt"
    }
    expression_attribute_values = {
        ":status": status,
        ":updatedAt": now
    }
    
    # Add optional fields
    for key, value in kwargs.items():
        if value is not None:
            update_expression += f", #{key} = :{key}"
            expression_attribute_names[f"#{key}"] = key
            expression_attribute_values[f":{key}"] = value
    
    if exceptionStatus and status == "FAILED":
        update_expression += ", #exceptionStatus = :exceptionStatus"
        expression_attribute_names["#exceptionStatus"] = "exceptionStatus"
        expression_attribute_values[":exceptionStatus"] = exceptionStatus

    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={'documentUploadId': documentUploadId},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )

@tracer.capture_method
def lookupBranchCode(branch_name: str, es_attribute: str = "grabMerchantName") -> str:
    """
    Looks up the branch code from OpenSearch using exact match on a specified attribute.
    Returns empty string if not found.

    Args:
        branch_name (str): The name of the branch to search for.
        es_attribute (str): The ES attribute name to match (default: "grabMerchantName").

    Returns:
        str: The branch code if found, empty string otherwise.
    """
    try:
        if not branch_name or branch_name == "-":
            return ""
        
        normalized_branch_name = branch_name

        # Check cache
        cache_key = f"{es_attribute}:{normalized_branch_name}"
        if cache_key in BRANCH_CODE_CACHE:
            return BRANCH_CODE_CACHE[cache_key]

        url = f'{ES_DOMAIN_ENDPOINT}/store/_search'
        headers = {'Content-Type': "application/json"}

        # Use match_phrase with slop for flexible matching
        match_payload = {
            "query": {
                "bool": {
                     "must": [
                        {"match": {"merchantId": MERCHANT_ID}},
                        {"term": {f"{es_attribute}.keyword": {
                            "value": normalized_branch_name,
                            "case_insensitive": True
                        }}}
                    ]
                }
            },
            "size": 1
        }

        response = requests.post(url, json=match_payload, headers=headers, auth=AWSAUTH)
        data = response.json()

        if data['hits']['total']['value'] > 0:
            hit = data['hits']['hits'][0]
            branch_code = hit['_source'].get('branchCode', '')
            matched_branch_name = hit['_source'].get(es_attribute, '')
            BRANCH_CODE_CACHE[cache_key] = branch_code
            return branch_code

        # Not found
        BRANCH_CODE_CACHE[cache_key] = ""
        return ""

    except Exception as ex:
        logger.exception(f"Error looking up branch code for branch name {branch_name} using '{es_attribute}': {str(ex)}")
        return ""
    

@tracer.capture_method
def extractAndConvertDate(text: str, source_timezone: str = 'Asia/Kuala_Lumpur') -> str | None:
    # Match pattern like (Txn date : 15.12.2024)
    match = re.search(r'Txn date\s*:\s*(\d{2})\.(\d{2})\.(\d{4})', text)
    if not match:
        return None

    day, month, year = match.groups()
    try:
        # Construct local datetime object
        local_dt = datetime(int(year), int(month), int(day), tzinfo=ZoneInfo(source_timezone))
        
        return local_dt.strftime('%Y-%m-%d')
    except ValueError as e:
        return None
    
@tracer.capture_method
def queueFileForProcessing(objectKey: str, documentUploadId: str) -> str:
    """Copy file to processing queue location"""
    try:
        pathParts = objectKey.split('/')
        merchantId = pathParts[2]
        documentType = pathParts[3] if len(pathParts) > 3 else "unknown"
        fileName = pathParts[-1]
        
        # Create queue path
        queuePath = f"reconciliation/queue/{merchantId}/{documentType}/documentUploadId={documentUploadId}/{fileName}"
        
        # Copy file to queue location
        copy_source = {'Bucket': S3_BUCKET_NAME, 'Key': objectKey}
        S3_CLIENT.copy_object(Bucket=S3_BUCKET_NAME, CopySource=copy_source, Key=queuePath)
        
        return queuePath
        
    except Exception as ex:
        logger.exception(f"Error queueing file: {str(ex)}")
        raise ex

@tracer.capture_method
def startProcessingGlueJob(documentUploadId: str, queuePath: str, documentType: str, fileName: str, tableName: str):
    """Start the processing Glue job"""
    try:
        GLUE_CLIENT = boto3.client('glue')
        
        response = GLUE_CLIENT.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--documentUploadIds': documentUploadId,
                '--s3Paths': f"s3://{S3_BUCKET_NAME}/{queuePath}",
                '--documentTypes': documentType,
                '--fileNames': fileName,
                '--merchantId': MERCHANT_ID,
                '--BUCKET_NAME': S3_BUCKET_NAME,
                '--DOCUMENT_UPLOAD_TABLE': DOCUMENT_UPLOAD_TABLE,
                '--SQS_QUEUE_URL': CREATE_DOCUMENT_SQS_QUEUE_URL,
                '--DDB_TABLE_NAME': tableName,
            }
        )
        
        runId = response['JobRunId']
        return runId
        
    except Exception as ex:
        logger.exception(f"Error starting processing job: {str(ex)}")
        updateDocumentUploadStatus(documentUploadId, "FAILED", str(ex))
        raise ex