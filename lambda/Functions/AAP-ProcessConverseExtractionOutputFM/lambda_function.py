import os
import uuid  
import boto3
import json
import time
import pandas as pd
import decimal
from decimal import Decimal
import io
import copy
from aws_lambda_powertools import Logger, Tracer
from botocore.exceptions import NoCredentialsError, ClientError
import csv
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
import uuid
from bedrock_function import promptBedrock
import re
import defaultPrompts
import urllib.parse
import requests
from requests_aws4auth import AWS4Auth
from typing import List, Dict, Optional


# Environment variables
AGENT_MAPPING_BUCKET = os.environ.get('AGENT_MAPPING_BUCKET')
DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
EXTRACTED_DOCUMENT_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
EXTRACTED_DOCUMENT_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_DOCUMENT_LINE_ITEM_TABLE')
EXTRACTED_PO_TABLE = os.environ.get('EXTRACTED_PO_TABLE')
EXTRACTED_PO_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_PO_LINE_ITEM_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
BDA_PROCESSING_BUCKET = os.environ.get('BDA_PROCESSING_BUCKET')
AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
N8N_SQS_QUEUE = os.environ.get('N8N_SQS_QUEUE')
SEQUENCE_NUMBER_GENERATOR_TABLE = os.environ.get('SEQUENCE_NUMBER_GENERATOR_TABLE')
ES_DOMAIN_ENDPOINT = os.environ.get('ES_DOMAIN_ENDPOINT')
SUPPLIER_INDEX = os.environ.get('SUPPLIER_INDEX')
STORE_INDEX = os.environ.get('STORE_INDEX')
LINE_ITEM_INDEX = os.environ.get('LINE_ITEM_INDEX')

# AWS clients and authentication
S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs', region_name='us-east-1')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-5')

# OpenSearch authentication setup
CREDENTIALS = boto3.Session().get_credentials()
ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-5', 'es', session_token=CREDENTIALS.token)

# DynamoDB tables
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_TABLE)
EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_LINE_ITEM_TABLE)
EXTRACTED_PO_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_PO_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
SEQUENCE_NUMBER_GENERATOR_DDB_TABLE = DDB_RESOURCE.Table(SEQUENCE_NUMBER_GENERATOR_TABLE)

logger = Logger()
tracer = Tracer()

IGNORED_EXCEPTION_FIELDS = {
    'linediscountamount', 'generalcomment', 'comment', 'expectedreceipt'
}

EXCEPTION_FIELD_MAPPING = {
    'itemcode': 'No.',
    'invoicedate': 'Order Date',
    'quantity': 'Quantity',
    'invoicenumber': 'Vendor Order No / Vendor Shipment No',
    'purchasercode': 'Purchaser Code',
    'locationcode': 'Location Code / Dim',
    'buyergroup': 'Buyer Group',
    'suppliercode': 'Buy From Vendor Number',
    'itemtype': 'Item Type',
}

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    day = datetime.now().strftime('%Y_%m_%d')

    updatedMappedJsonData = []
    totalInputTokens = 0
    totalOutputTokens = 0

    documentUploadId = None
    merchantId = None
    
    try:
        records = event.get('Records', [])
        if not records:
            return {
                "status": True,
                "body": " No records to process."
            }

        processedCount = 0
        failedCount = 0

        for recordIndex, record in enumerate(records):
            documentUploadId = None
            merchantId = None

            if not record or 'body' not in record:
                failedCount += 1
                continue

            body = json.loads(record.get('body', '{}'))
            if not isinstance(body, dict):
                failedCount += 1
                continue

            merchantId = body.get('merchantId')
            documentUploadId = body.get('documentUploadId')
            filePath = body.get('filePath') or "-"
            sourceFileName = body.get('sourceFileName')

            validationErrors = []
            if isEmptyValue(merchantId):
                validationErrors.append("merchantId is missing or empty")
            if isEmptyValue(documentUploadId):
                validationErrors.append("documentUploadId is missing or empty")
            if isEmptyValue(sourceFileName):
                validationErrors.append("sourceFileName is missing or empty")

            if validationErrors:
                handleFailedRecord(documentUploadId, f"Record {recordIndex} validation failed: {', '.join(validationErrors)}")
                failedCount += 1
                continue

            extractionResult = body.get('extractionResult', {})
            resultJsonList = [extractionResult] if extractionResult else body.get('result', [])
            if not resultJsonList or (len(resultJsonList) == 1 and not resultJsonList[0]):
                handleFailedRecord(documentUploadId, "No extraction data available")
                failedCount += 1
                continue

            merchantConfig = getMerchantConfiguration(merchantId)

            # Unsupported PO+GRN case
            if sourceFileName.split('_')[0] == 'po' and 'grn' in sourceFileName:
                payload = createUnsupportedDocumentPayload(documentUploadId, sourceFileName, now)
                invoice = createExtractedResultRecord(payload, merchantId, documentUploadId, sourceFileName, filePath, now, merchantConfig, 0, 0)
                updatedMappedJsonData.append(invoice)
                updateFailedDocumentUploadStatus(documentUploadId, "Document Type Unrecognized")
                createTimelineRecord(merchantId, invoice, now)
                processedCount += 1
                continue

            mappedJsonData = processJsonResult(resultJsonList)
            if not mappedJsonData:
                logger.warning(f"No mapped data generated for {sourceFileName}")
                handleFailedRecord(documentUploadId, "No data mapped from extraction")
                failedCount += 1
                continue

            recordInvoices = []
            for invoice in mappedJsonData:
                invoiceInputTokens = 0
                invoiceOutputTokens = 0

                ## moved quantity checking
                filteredLineItems, allItemsFiltered = processLineItemQuantity(invoice.get('lineItem', []))
                invoice['lineItem'] = filteredLineItems

                # Handle case where all line items were filtered out
                if allItemsFiltered:
                    invoice['status'] = 'Exceptions'
                    invoice['exceptionStatus'] = 'All line items had 0 quantity'
                    invoice = createExtractedResultRecord(invoice, merchantId, documentUploadId, sourceFileName, filePath, now, merchantConfig, invoiceInputTokens, invoiceOutputTokens)
                    createTimelineRecord(merchantId, invoice, now)
                    recordInvoices.append(invoice)
                    continue

                invoice, inTokens, outTokens = performMasterDataChecking(invoice, merchantConfig)
                invoiceInputTokens += inTokens
                invoiceOutputTokens += outTokens
                totalInputTokens += inTokens
                totalOutputTokens += outTokens

                invoice = performDuplicateChecking(invoice, merchantId)
                invoice, inTokens, outTokens = performStandardization(invoice, merchantConfig)
                invoice = generatePoExpiryDate(invoice)
                invoiceInputTokens += inTokens
                invoiceOutputTokens += outTokens
                totalInputTokens += inTokens
                totalOutputTokens += outTokens

                invoice = performMissingFieldChecking(invoice, merchantConfig)
                invoice = performAmountChecking(invoice, merchantConfig)
                invoice, inTokens, outTokens = performExceptionChecking(invoice, merchantConfig)
                invoiceInputTokens += inTokens
                invoiceOutputTokens += outTokens
                totalInputTokens += inTokens
                totalOutputTokens += outTokens

                ## original quantity checking - moved up

                invoice = applyInvoiceToPOExceptionLogic(invoice, merchantConfig)
                invoice = createExtractedResultRecord(invoice, merchantId, documentUploadId, sourceFileName, filePath, now, merchantConfig, invoiceInputTokens, invoiceOutputTokens)
                performInvoiceToPOConversion(invoice, merchantConfig, documentUploadId, 'sqs', now)
                createTimelineRecord(merchantId, invoice, now)

                recordInvoices.append(invoice)

            if recordInvoices:
                updatedMappedJsonData.extend(recordInvoices)
                inTokens, outTokens = updateDocumentUploadStatus(documentUploadId, recordInvoices, now)
                totalInputTokens += inTokens
                totalOutputTokens += outTokens
                processedCount += 1
            else:
                failedCount += 1

        return {
            "status": True,
            "body": f"Data extraction process completed. Processed: {processedCount}, Failed: {failedCount}",
            "processedCount": processedCount,
            "failedCount": failedCount,
            "totalRecords": len(records)
        }

    except NoCredentialsError:
        return {"status": False, "body": "AWS credentials not available"}

    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        handleFailedRecord(documentUploadId, "System Error")
        return {"status": False, 'body': f"Error processing records: {str(ex)[:200]}"}


# Helper function to check if value is empty/invalid
@tracer.capture_method
def isEmptyValue(value):
    return (value is None or 
            value == "" or 
            str(value).strip() == "" or 
            value == "-" or
            value == "null" or
            value == "undefined")

@tracer.capture_method
def handleFailedRecord(docId, message):
    """Update failed document status if ID exists."""
    if docId and not isEmptyValue(docId):
        updateFailedDocumentUploadStatus(docId, message)

@tracer.capture_method
def createUnsupportedDocumentPayload(docId, fileName, currentTime):
    """Generate payload for unsupported document types."""
    return {
        "invoiceNumber": "-",
        "invoiceDate": "-",
        "documentType": "invoice",
        "supplierName": "-",
        "supplierAddress": "-",
        "supplierCode": "-",
        "buyerName": "-",
        "buyerAddress": "-",
        "buyerCode": "-",
        "poNumber": "-",
        "paymentTerms": "-",
        "currency": "-",
        "totalAmount": 0,
        "taxType": "-",
        "taxRate": "-",
        "taxAmount": 0,
        "dueDate": "-",
        "boundingBoxes": {},
        "status": "Exceptions",
        "exceptionStatus": "Document Format Unrecognized",
        "createdAt": currentTime,
        "createdBy": "System",
        "updatedAt": currentTime,
        "updatedBy": "System",
        "sourceFile": fileName,
        "confidenceScore": 0,
        "documentUploadId": docId
    }

@tracer.capture_method
def processJsonResult(resultDataList):
    mappedJsonData = []

    fieldMapping = {
        "invoiceNumber": ["InvoiceNumber"],
        "invoiceDate": ["InvoiceDate"],
        "supplierName": ["Vendor", "VendorName"],
        "supplierAddress": ["VendorAddress"],
        "supplierAddress2": ["VendorAddress2"],
        "supplierAddress3": ["VendorAddress3"],
        "buyerName": ["Recipient"],
        "buyerAddress": ["RecipientAddress"],
        "storeLocation": ["PremiseAddress"],
        "storeCode": ["StoreCode"],
        "poNumber": ["POnumber"],
        "paymentTerms": ["PaymentTerms"],
        "currency": ["Currency"],
        "totalAmount": ["TotalBill"],
        "taxType": ["TaxType"],
        "taxRate": ["TaxRate"],
        "taxAmount": ["TaxAmount"],
        "dueDate": ["DueDate"],
        "customerReference": ["CustomerReference"],
        "expectedReceipt": ["ExpectedReceipt"],
        "generalComment": ["GeneralComment"],
    }

    tableFieldMapping = {
        "description": ["Description"],
        "unitPrice": ["UnitPrice"],
        "uom": ["UOM"],
        "quantity": ["Quantity"],
        "totalPrice": ["TotalAmountWithTax"],
        "lineDiscountAmount": ["LineDiscountAmount"],
        "comment": ["Comment"],
        "amountExclTax": ["AmountWithoutTax"],
        "taxAmount": ["TaxAmount"],
        "subTotal": ["SubTotalAmount"],
        "discountAmount": ["DiscountAmount"],
    }

    for dataItem in resultDataList:
        # Load data (direct or S3)
        if isinstance(dataItem, dict) and 'inference_result' in dataItem:
            data = dataItem
        else:
            response = S3_CLIENT.get_object(Bucket=BDA_PROCESSING_BUCKET, Key=dataItem)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)

        jsonResult = data.get('inference_result', data)
        if isinstance(jsonResult, str):
            parsedData = json.loads(jsonResult)
        else:
            parsedData = jsonResult

        confidenceScore = data.get('matched_blueprint', {}).get('confidence',
                           data.get('confidenceScore', 0.8))

        invoiceData = {
            "invoiceNumber": "-",
            "invoiceDate": "-",
            "supplierName": "-",
            "supplierAddress": "-",
            "supplierAddress2": "-",
            "supplierAddress3": "-",
            "buyerName": "-",
            "buyerAddress": "-",
            "storeLocation": "-",
            "poNumber": "-",
            "paymentTerms": "-",
            "currency": "-",
            "totalAmount": 0,
            "taxType": "-",
            "taxRate": "-",
            "taxAmount": 0,
            "dueDate": "-",
            "customerReference": "-",
            "expectedReceipt": "-",
            "generalComment": "-",
            "confidenceScore": confidenceScore,
            "lineItem": [],
            "boundingBoxes": {}
        }

        # Invoice-level fields
        for invoiceField, possibleKeys in fieldMapping.items():
            value, boxes = extractFieldValueWithBoundingBoxes(parsedData, possibleKeys, invoiceField)
            invoiceData[invoiceField] = value
            invoiceData['boundingBoxes'][invoiceField] = boxes

        # Line items
        serviceTableData = parsedData.get("LineItems") or parsedData.get("service_table") or []
        for service in serviceTableData:
            lineItem = {"boundingBoxes": {}, "supplierName": invoiceData.get('supplierName', '')}
            for column, possibleKeys in tableFieldMapping.items():
                value, boxes = extractFieldValueWithBoundingBoxes(service, possibleKeys, column)

                # Special handling for UOM default
                if "UOM" in possibleKeys and not value:
                    value = "EA"

                # Numeric field defaults
                isNumeric = column in ["quantity", "unitPrice", "totalPrice", "lineDiscountAmount",
                                       "amountExclTax", "taxAmount", "subTotal", "discountAmount"]
                value = normalizeFieldValue(value, isNumeric)

                lineItem[column] = value
                lineItem['boundingBoxes'][column] = boxes

            lineItem = setBackUpLineItemTotalPrice(lineItem)
            invoiceData["lineItem"].append(lineItem)

        invoiceData["documentType"] = "invoice"

        if invoiceData.get('boundingBoxes'):
            invoiceData['boundingBoxes'] = json.dumps(invoiceData['boundingBoxes'], cls=DecimalEncoder)

        mappedJsonData.append(invoiceData)

    return mappedJsonData

@tracer.capture_method
def extractFieldValueWithBoundingBoxes(parsedData, possibleKeys, invoiceField):
    """Extract field value and bounding boxes from parsed data."""
    boundingBoxes = []
    value = "-"
    for key in possibleKeys:
        if key in parsedData:
            fieldData = parsedData[key]
            if isinstance(fieldData, dict) and 'value' in fieldData:
                value = fieldData.get('value', '')
                geometryData = fieldData.get('geometry', [])
                for geo in geometryData:
                    page = normalizePageNumber(geo.get('page', 1))
                    box = geo.get('boundingBox', {})
                    boundingBoxes.append({
                        'width': Decimal(str(box.get('width', 0))),
                        'height': Decimal(str(box.get('height', 0))),
                        'left': Decimal(str(box.get('left', 0))),
                        'top': Decimal(str(box.get('top', 0))),
                        'page': page
                    })
            else:
                value = str(fieldData)
            break
    return value, boundingBoxes

@tracer.capture_method
def normalizeFieldValue(value, isNumeric=False):
    """Ensure correct default for numeric/text fields."""
    if isNumeric:
        return 0 if not value or value == "-" else value
    return "-" if not value else value

# Helper class for JSON serialization of Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


@tracer.capture_method
def performDuplicateChecking(invoice, merchantId):
    
    invoiceResp = EXTRACTED_DOCUMENT_DDB_TABLE.query(
        IndexName='gsi-merchantId-invoiceNumber',
        KeyConditionExpression=Key('merchantId').eq(merchantId)&Key('invoiceNumber').eq(invoice.get('invoiceNumber')),
        FilterExpression=Attr('documentType').eq("invoice")
    ).get('Items', [])

    if invoiceResp:
        invoice["isDuplicate"] = True
    else:
        invoice["isDuplicate"] = False
    
    return invoice

@tracer.capture_method
def performStandardization(invoice, merchantConfig):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)

    # Get default prompt
    default_prompt = defaultPrompts.STANDARDIZATION_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchantConfig.get('promptPaths', {})
    standardization_prompt_path = prompt_paths.get('standardizationPrompt')
    prompt_template = fetch_prompt(standardization_prompt_path, default_prompt)

    prompt = prompt_template.format(invoiceData=json.dumps(invoiceData))
    response, input_tokens, output_tokens = promptBedrock(prompt)
    invoice_json = json.loads(response)
        
    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def generatePoExpiryDate(invoice):
    if invoice.get('invoiceDate'):
        invoice_date = datetime.strptime(invoice['invoiceDate'], '%Y-%m-%d')
        po_expiry_date = invoice_date + timedelta(days=365)
        invoice['poExpiry'] = po_expiry_date.strftime('%Y-%m-%d')

    return invoice

@tracer.capture_method
def performMissingFieldChecking(invoice, merchantConfig):
    invoiceData = copy.deepcopy(invoice)
    missing_fields = []
    required_fields = getRequiredFields(merchantConfig)
    
    for field in required_fields['invoice']:
        if field not in invoiceData or invoiceData.get(field) is None or invoiceData.get(field) == "" or invoiceData.get(field) == "-":
            missing_fields.append(field)
    
    # Update invoice status based on missing fields
    if missing_fields:
        invoiceData['status'] = "Exceptions"
        invoiceData['missingFieldException'] = f"Missing required fields: {', '.join(missing_fields)}"
    else:
        invoiceData['missingFieldException'] = "N/A"
    
    # Check line item fields
    for item in invoiceData.get('lineItem', []):
        item_missing_fields = []
        
        for field in required_fields['lineItem']:
            if field not in item or item.get(field) is None or item.get(field) == "" or item.get(field) == "-":
                item_missing_fields.append(field)
        
        if item_missing_fields:
            item['status'] = "Exceptions"
            item['missingFieldException'] = f"Missing required fields: {', '.join(item_missing_fields)}"
        else:
            item['missingFieldException'] = "N/A"
    
    return invoiceData

@tracer.capture_method
def performAmountChecking(invoice, merchantConfig):
    invoiceData = copy.deepcopy(invoice)

    #Skip amount checking if invoiceToPO logic is enabled
    if merchantConfig:
        custom_logics = merchantConfig.get('customLogics', {})
        if custom_logics.get('invoiceToPO', False):
            invoiceData['amountException'] = "N/A"
            return invoiceData
    
    # Calculate sum of line item total prices
    lineItemTotalPrice = sum([safe_float(item.get('totalPrice', 0)) for item in invoiceData.get('lineItem', [])])
    totalInvoiceAmount = safe_float(invoiceData.get('totalAmount', 0))
    
    # Handle cases where tax is marked as not applicable
    if invoiceData.get('taxType') == "-" and invoiceData.get('taxRate') == "-":
        # Calculate expected total without tax
        lineItemSubtotal = sum([safe_float(item.get('amountExclTax', 0)) for item in invoiceData.get('lineItem', [])])
        
        # Compare totals (with tolerance for rounding)
        tolerance = 0.01
        if abs(lineItemTotalPrice - totalInvoiceAmount) <= tolerance:
            invoiceData['amountException'] = "N/A"
        else:
            invoiceData['amountException'] = f"Amount mismatch: Line items total {lineItemTotalPrice}, Invoice total {totalInvoiceAmount}"
            invoiceData['status'] = "Exceptions"
    else:
        # Standard amount checking with tax
        tolerance = 0.01
        if abs(lineItemTotalPrice - totalInvoiceAmount) <= tolerance:
            invoiceData['amountException'] = "N/A"
        else:
            invoiceData['amountException'] = f"Amount mismatch: Line items total {lineItemTotalPrice}, Invoice total {totalInvoiceAmount}"
            invoiceData['status'] = "Exceptions"
    
    return invoiceData

@tracer.capture_method
def performExceptionChecking(invoice, merchantConfig):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)
    
    # Get default prompt
    default_prompt = defaultPrompts.EXCEPTION_STATUS_CHECKING_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchantConfig.get('promptPaths', {})
    exception_checking_prompt_path = prompt_paths.get('exceptionCheckingPrompt')
    prompt_template = fetch_prompt(exception_checking_prompt_path, default_prompt)
    
    prompt = prompt_template.format(invoiceData=json.dumps(invoiceData))
    response, input_tokens, output_tokens = promptBedrock(prompt)
    
    invoice_json = json.loads(response)
    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def applyInvoiceToPOExceptionLogic(invoice, merchantConfig):
    """
    Apply exception logic for Invoice to PO conversion
    Mark line items as exceptions if locationCode is missing
    """
    custom_logics = merchantConfig.get('customLogics', {})
    use_invoice_to_po = custom_logics.get('invoiceToPO', False)
    
    if not use_invoice_to_po:
        return invoice
    
    # Check if invoice status is already exceptions
    if invoice.get('status', '').lower() != 'success':
        return invoice
    
    location_code = invoice.get('locationCode', '')
    
    if not location_code or location_code == '-':
        items_changed = 0
        for item in invoice.get('lineItem', []):
            if item.get('status') == 'Success':
                item['status'] = 'Exceptions'
                
                current_exception_status = item.get('exceptionStatus', '')
                if 'missing location code' not in current_exception_status.lower():
                    if current_exception_status and current_exception_status != 'N/A':
                        item['exceptionStatus'] = f"{current_exception_status} and missing location code required for PO conversion"
                        invoice['status'] = 'Exceptions'
                    else:
                        item['exceptionStatus'] = "Missing location code required for PO conversion"
                        invoice['status'] = 'Exceptions'
                
                items_changed += 1
        
        # Update invoice level exception status
        current_exception_status = invoice.get('exceptionStatus', '')
        if 'missing location code' not in current_exception_status.lower():
            if current_exception_status and current_exception_status != 'N/A':
                invoice['exceptionStatus'] = f"{current_exception_status} and missing location code required for PO conversion"
                invoice['status'] = 'Exceptions'
            else:
                invoice['exceptionStatus'] = "Missing location code required for PO conversion"
                invoice['status'] = 'Exceptions'
    
    return invoice

@tracer.capture_method
def performInvoiceToPOConversion(invoice, merchantConfig, documentUploadId, eventSource, now, line_item=None):
    """
    Convert successful invoice records to PO records for merchants with invoiceToPO enabled
    Only processes line items with "Success" status
    """
    custom_logics = merchantConfig.get('customLogics', {})
    use_invoice_to_po = custom_logics.get('invoiceToPO', False)
    merchantId = merchantConfig.get('merchantId')
    line_items_created = 0
    extractedPoId = invoice.get('referenceExtractedPoId')

    if not use_invoice_to_po:
        return
    
    all_line_items = invoice.get("lineItem", [])
    if not all_line_items:
        return
        return None
    
    successful_line_items = []
    for item in all_line_items:
        item_status = item.get("status", "").lower()
        if item_status == "success":
            successful_line_items.append(item)
    
    # Check if we have any successful line items to process
    if not successful_line_items:
        return None
    
    # if not invoice.get('supplierCode') or not invoice.get('locationCode'):
    #     return None
    locationCode = invoice.get('locationCode')
    if isEmptyValue(locationCode):
        return None

    totalAmount = Decimal('0')
    for item in successful_line_items:
        item_total = clean_to_decimal(item.get('totalPrice', '0'))
        totalAmount += item_total

    # Generate new PO details
    poNumberGenerated = generatePoNumber(merchantId, now)

    # Create PO record
    extracted_po_payload = {
        "extractedPoId": extractedPoId,
        "merchantId": merchantId,
        "poNumber": poNumberGenerated,
        "poDate": invoice.get('invoiceDate'),
        "supplierName": invoice.get('supplierName'),
        "supplierCode": invoice.get('supplierCode'),
        "buyerName": invoice.get('buyerName'),
        "storeLocation": invoice.get('storeLocation'),
        "locationCode": invoice.get('locationCode'),
        "dim": invoice.get('dim'),
        "invoiceDate": invoice.get("invoiceDate", ""),
        "invoiceNumber": invoice.get("invoiceNumber", ""),
        "poExpiry": invoice.get("poExpiry"),
        "currency": invoice.get('currency'),
        "totalAmount": totalAmount,
        "status": "Success",
        "exceptionStatus": "N/A",
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "sourceFile": invoice.get('sourceFile'),
        "documentUploadId": documentUploadId,
        "lineItem": []
    }
    
    line_items_count = len(successful_line_items)
    
    # Convert line items to PO format
    for item in successful_line_items:
        extractedPoLineItemsId = str(uuid.uuid4())
        extractedDocumentsLineItemsId = item.get("extractedDocumentsLineItemsId", "")
        
        extracted_po_line_item_payload = {
            "extractedPoLineItemsId": extractedPoLineItemsId,
            "documentUploadId": documentUploadId,
            "extractedDocumentsLineItemsId": extractedDocumentsLineItemsId,
            "extractedDocumentsId": invoice.get("extractedDocumentsId", ""),
            "description": item.get('description'),
            "itemCode": item.get('itemCode'),
            "invoiceNumber": invoice.get("invoiceNumber", ""),
            "quantity": clean_to_decimal(item.get('quantity')),
            "unitPrice": clean_to_decimal(item.get('unitPrice')),
            "poExpiry": invoice.get("poExpiry"),
            "uom": item.get('uom'),
            "itemType": item.get("itemType"),
            "totalPrice": clean_to_decimal(item.get('totalPrice')),
            "purchaserCode": item.get('purchaserCode'),
            "buyerGroup": item.get('buyerGroup'),
            "invoiceDate": invoice.get('invoiceDate'),
            "locationCode": invoice.get('locationCode'),
            "expectedReceipt": invoice.get('expectedReceipt'),
            "status": "Success",
            "exceptionStatus": "N/A",
            "createdAt": now,
            "updatedAt": now
        }
        
        extracted_po_payload["lineItem"].append(extracted_po_line_item_payload)
    
    # Save PO record to database
    createExtractedPORecord(extracted_po_payload, merchantId, documentUploadId, now)

    return poNumberGenerated

@tracer.capture_method
def cleanInvoiceData(invoice):
    """Clean and validate invoice data before saving"""
    # Clean numeric fields
    numeric_fields = ['totalAmount', 'taxAmount', 'confidenceScore']
    for field in numeric_fields:
        if field in invoice:
            invoice[field] = float(invoice[field]) if invoice[field] not in [None, "", "-"] else 0
    
    # Clean line items
    for item in invoice.get('lineItem', []):
        item_numeric_fields = ['quantity', 'unitPrice', 'totalPrice', 'lineDiscountAmount', 'amountExclTax', 'taxAmount', 'subTotal', 'discountAmount']
        for field in item_numeric_fields:
            if field in item:
                item[field] = float(item[field]) if item[field] not in [None, "", "-"] else 0
    
    return invoice

@tracer.capture_method
def processLineItemQuantity(lineItems):
    """
    Filter and process line items based on quantity rules:
    - Skip if quantity is empty/0
    - Convert "LS" to 1
    - Keep if quantity >= 1
    Returns: (processedLineItems, allItemsFiltered)
    """
    processedLineItems = []
    
    for item in lineItems:
        quantity = item.get('quantity', '')
        
        # Skip if quantity is empty or 0
        if isEmptyValue(quantity) or str(quantity).strip() == '0':
            continue
            
        # Convert "LS" to 1
        if str(quantity).strip().upper() == 'LS':
            item['quantity'] = '1'
        
        # Add item to processed list (quantity >= 1 or converted LS)
        processedLineItems.append(item)
    
    # Check if all items were filtered out
    allItemsFiltered = len(lineItems) > 0 and len(processedLineItems) == 0
    
    return processedLineItems, allItemsFiltered

@tracer.capture_method
def createExtractedResultRecord(invoiceData, merchantId, documentUploadId, sourceFileName, filePath, now, merchantConfig=None, input_tokens=0, output_tokens=0):
    extractedDocumentsId = str(uuid.uuid4())
    extractedPoId = str(uuid.uuid4())
        
    for idx, item in enumerate(invoiceData.get("lineItem", [])):
        # Generate unique ID for each line item
        extractedDocumentsLineItemsId = str(uuid.uuid4())
        # IMPORTANT: Assign the ID back to the line item so it's available later
        item["extractedDocumentsLineItemsId"] = extractedDocumentsLineItemsId
        
        # Calculate exception fields for THIS specific item
        exception_fields = parseExceptionFields(
            item.get('exceptionStatus', ''), 
            item, 
            merchantConfig,
            invoiceData
        )

        extractedDocumentLineItemPayload = {
            "extractedDocumentsLineItemsId": extractedDocumentsLineItemsId,
            'invoiceNumber': invoiceData.get("invoiceNumber"),
            "itemCode": item.get("itemCode"),
            "description": item.get("description"),
            "unitPrice": item.get("unitPrice"),
            "itemUom": item.get("uom"),
            "quantity": item.get("quantity"),
            "totalPrice": item.get("totalPrice"),
            "lineDiscountAmount": item.get("lineDiscountAmount"),
            "comment": item.get("comment"),
            "merchantId": merchantId,
            "purchaserCode": item.get("purchaserCode"),
            "buyerGroup": item.get("buyerGroup"),
            "supplierCode": invoiceData.get("supplierCode"),
            "supplierName": invoiceData.get("supplierName"),
            "extractedDocumentsId": extractedDocumentsId,
            "itemType": item.get("itemType"),
            "storeLocation": invoiceData.get("premiseAddress"),
            "storeName": invoiceData.get("premiseAddress"),
            "locationCode": invoiceData.get("locationCode"),
            "dim": invoiceData.get("dim"),
            "expectedReceipt": item.get("expectedReceipt"),
            "generalComment": item.get("generalComment"),
            "poExpiry": invoiceData.get("poExpiry"),
            'boundingBoxes': item.get('boundingBoxes', {}),
            "amountExclTax": item.get("amountExclTax"),
            "taxAmount": item.get("taxAmount"),
            "subTotal": item.get("subTotal"),
            "discountAmount": item.get("discountAmount"),
            "exceptionFields": exception_fields,
            "status": item.get("status"),
            "exceptionStatus": item.get("exceptionStatus"),
            "missingFieldException": item.get("missingFieldException"),
            "filePath": filePath,
            "createdAt": now,
            "createdBy": "System",
            "updatedAt": now,
            "updatedBy": "System",
            "approvedAt": "",
            "approvedBy": "",
            "remarks": "",
            "sourceFile": sourceFileName,
            "confidenceScore": round(invoiceData.get("confidenceScore", 0)*100),
            "documentUploadId": documentUploadId,
        }

        extractedDocumentLineItemPayload = convert_floats_to_decimals(extractedDocumentLineItemPayload)
        EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE.put_item(Item=extractedDocumentLineItemPayload)  

    # Calculate overall exception fields for the invoice
    overall_exception_fields = parseExceptionFields(
        invoiceData.get('exceptionStatus', ''), 
        {},  # No specific item data for overall
        merchantConfig,
        invoiceData
    )
        
    extractedDocumentPayload = {
        "extractedDocumentsId": extractedDocumentsId,
        'referenceExtractedPoId': extractedPoId,
        "merchantId": merchantId,
        "invoiceNumber": invoiceData.get("invoiceNumber"),
        "invoiceDate": invoiceData.get("invoiceDate"),
        "documentType": invoiceData.get("documentType"),
        "supplierName": invoiceData.get("supplierName"),
        "supplierAddress": invoiceData.get("supplierAddress"),
        "supplierCode": invoiceData.get("supplierCode"),
        "buyerName": invoiceData.get("buyerName"),
        "buyerAddress": invoiceData.get("buyerAddress"),
        "buyerCode": invoiceData.get("buyerCode"),
        "poNumber": invoiceData.get("poNumber"),
        "paymentTerms": invoiceData.get("paymentTerms"),
        "currency": invoiceData.get("currency"),
        "totalAmount": invoiceData.get("totalAmount"),
        "taxType": invoiceData.get("taxType"),
        "taxRate": invoiceData.get("taxRate"),
        "taxAmount": invoiceData.get("taxAmount"),
        "dueDate": invoiceData.get("dueDate"),
        "customerReference": invoiceData.get("customerReference"),
        "documentStatus": invoiceData.get("status"),
        "storeName": invoiceData.get("storeName"),
        "locationCode": invoiceData.get("locationCode"),
        "dim": invoiceData.get("dim"),
        "expectedReceipt": invoiceData.get("expectedReceipt"),
        "generalComment": invoiceData.get("generalComment"),
        "poExpiry": invoiceData.get("poExpiry"),
        'boundingBoxes': invoiceData.get('boundingBoxes', {}),
        "exceptionStatus": invoiceData.get('exceptionStatus'),
        "exceptionFields": overall_exception_fields,
        "missingFieldException": invoiceData.get("missingFieldException"),
        "amountException": invoiceData.get("amountException"),
        "filePath": filePath,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "remarks": "",
        "sourceFile": sourceFileName,
        "confidenceScore": round(invoiceData.get("confidenceScore", 0)*100),
        "documentUploadId": documentUploadId,
        "totalInputTokens": input_tokens,
        "totalOutputTokens": output_tokens        
    }

    extractedDocumentPayload = convert_floats_to_decimals(extractedDocumentPayload)
    EXTRACTED_DOCUMENT_DDB_TABLE.put_item(Item=extractedDocumentPayload)
    invoiceData["extractedDocumentsId"] = extractedDocumentsId
    invoiceData["referenceExtractedPoId"] = extractedPoId

    return invoiceData

@tracer.capture_method
def createExtractedPORecord(po_record, merchantId, documentUploadId, now):
    """Create extracted PO record in DynamoDB"""
    po_record_copy = copy.deepcopy(po_record)
    po_record_copy['merchantId'] = merchantId
    
    # Extract line items from the COPY
    line_items = po_record_copy.pop('lineItem', [])
    
    # Convert main PO record to Decimals
    po_record_for_db = convert_floats_to_decimals(po_record_copy)
    
    # Save main PO record
    EXTRACTED_PO_DDB_TABLE.put_item(Item=po_record_for_db)
    
    # Save PO line items separately
    for line_item in line_items:
        line_item['extractedPoId'] = po_record_for_db['extractedPoId']
        line_item['merchantId'] = merchantId
        
        # Convert line item to Decimals
        line_item_for_db = convert_floats_to_decimals(line_item)
        EXTRACTED_PO_LINE_ITEM_DDB_TABLE.put_item(Item=line_item_for_db)

@tracer.capture_method
def createTimelineRecord(merchantId, invoiceData, now):
    if 'approvalStatus' in invoiceData:
        if invoiceData['approvalStatus'] == "APPROVED":
            title = "approved"
            description = "Invoice approved"
        else:
            title = "rejected"
            description = invoiceData.get('rejectionReason', "Invoice rejected")
    elif invoiceData['status'] == "Success":
        title = "Document Processed"
        description = "Document extracted successfully"
    else:
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
def documentUploadStatusCheck(document_upload_id):
    all_extracted_documents = EXTRACTED_DOCUMENT_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id)
    ).get('Items', [])

    all_statuses = [extracted_document.get('exceptionStatus') for extracted_document in all_extracted_documents]

    prompt = defaultPrompts.DOCUMENT_UPLOAD_STATUS_CHECK_PROMPT.format(all_statuses=all_statuses)

    exception_status, input_tokens, output_tokens = promptBedrock(prompt)
    exception_status = json.loads(exception_status)
    return exception_status, input_tokens, output_tokens

@tracer.capture_method
def updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData, now):
    exception_details, input_tokens, output_tokens = documentUploadStatusCheck(documentUploadId)
    exception_status = str(exception_details.get("exceptionStatus"))
    status = str(exception_details.get("status"))

    # Collect valid confidence scores (ensure they're not 0 or None)
    confidence_scores = []
    for mappedJson in updatedMappedJsonData:
        score = mappedJson.get("confidenceScore", 0)
        confidence_scores.append(float(score))

    # Calculate average (avoid division by zero)
    if confidence_scores and any(confidence_scores):
        avg_confidence_score = round(sum(confidence_scores) / len(confidence_scores)) * 100 
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
            ':status': status,
            ':exceptionStatus': exception_status,
            ':updatedAt': now,
            ':updatedBy': "System",
            ':avgConfidenceScore': avg_confidence_score_decimal,
            ':confidenceScoreList': confidence_scores_decimal 
        }
    )

    return input_tokens, output_tokens

@tracer.capture_method
def updateFailedDocumentUploadStatus(documentUploadId, exceptionStatus):
    if exceptionStatus == "Document Type Unrecognized":
        status = "Pending Review"
    else:
        status = "Failed"

    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={
            'documentUploadId': documentUploadId,
        },
        UpdateExpression="set #status_attr = :status, exceptionStatus = :exceptionStatus",
        ExpressionAttributeNames={
            '#status_attr': 'status'
        },
        ExpressionAttributeValues={
            ':status': status,
            ':exceptionStatus': exceptionStatus
        }
    )

@tracer.capture_method
def get_agent_configuration_content_checking(merchant_id):
    response = AGENT_CONFIGURATION_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchant_id),
        FilterExpression=Attr('service.actions').eq('Invoice Extraction') & 
                          Attr('activeStatus').eq(True)
    )
    items = response.get('Items', [])
    
    if items:
        sorted_items = sorted(items, key=lambda x: x.get('updatedAt', ''), reverse=True)
        return sorted_items[0].get('configuration', {}).get('contentChecking', False)
    return False

@tracer.capture_method
def send_to_erp_sqs(invoice, merchant_id):
    payload = {
        'documentId': invoice.get('extractedDocumentsId'),
        'documentType': 'invoice',
        'merchantId': merchant_id,
    }
    
    content_checking = get_agent_configuration_content_checking(merchant_id)
    if not content_checking:
        response = SQS_CLIENT.send_message(
            QueueUrl=N8N_SQS_QUEUE,
            MessageBody=json.dumps(payload),
        )

@tracer.capture_method
def generatePoNumber(merchantId, extraction_date):

    # Convert extraction_date to proper format and get today's date
    if isinstance(extraction_date, str):
        try:
            date_obj = datetime.strptime(extraction_date, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            # Try alternative format if the first one fails
            date_obj = datetime.strptime(extraction_date, '%Y-%m-%d')
    else:
        date_obj = extraction_date
    
    # Format as DDMMYYYY for consistency with the requirement
    today = date_obj.strftime('%Y-%m-%d')
    todayDate = date_obj.strftime('%d%m%Y')
    postingNoPrefix = f'ROBO-{todayDate}'
    
    # Check if sequence number generator exists for this prefix
    sequenceNumGenResp = getSequenceNumberGenerator(postingNoPrefix)
    
    if not sequenceNumGenResp:
        # Create new sequence generator
        createSequenceNumberGenerator(postingNoPrefix, extraction_date)
        postingNo = f'{postingNoPrefix}-0001'
    else:
        # Get latest date from existing sequence generator
        latestDate = datetime.strptime(sequenceNumGenResp.get('updatedAt'), '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d')
        
        if latestDate == today:
            # Same date - increment sequence
            latestValue = str(int(sequenceNumGenResp.get('latestValue')) + 1).zfill(4)
        else:
            # New date - reset sequence
            latestValue = '0001'
        
        # Update sequence generator
        updateSequenceNumberGenerator(postingNoPrefix, latestValue, extraction_date)
        postingNo = f'{postingNoPrefix}-{latestValue}'
    
    return postingNo


@tracer.capture_method
def getMerchantConfiguration(merchantId):
    """
    Get merchant configuration once and return structured data
    """
    response = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchantId})
    merchant = response.get('Item', {})
    
    # Extract all necessary fields
    custom_logics = merchant.get('customLogics', {})
    mappingPrompts = merchant.get('mappingPrompts', {})
    
    merchantConfig = {
        'merchantId': merchantId,
        'customLogics': custom_logics,
        'mappingPaths': {
            'supplierMapping': merchant.get('supplierMapping'),
            'itemMapping': merchant.get('itemMapping'),
            'storeMapping': merchant.get('storeMapping'),
        },
        'promptPaths': mappingPrompts,
        'requiredFields': merchant.get('requiredFields'),
    }
    
    return merchantConfig

@tracer.capture_method
def parseExceptionFields(exceptionStatus, itemData, merchantConfig, invoiceData=None):
    """
    Parse exception status and item data to extract missing/problematic fields
    Returns comma-separated human-readable field names
    Only processes if enableExceptionFields is True for the merchant
    """
    # Check if merchant has exception fields enabled
    custom_logics = merchantConfig.get('customLogics', {})
    if not custom_logics.get('enableExceptionFields', False):
        return ""
    
    exception_fields = []
    
    if not exceptionStatus or exceptionStatus == "N/A":
        return ""
    
    # Check for master mapping error (itemCode issue)
    if "master mapping error" in exceptionStatus.lower():
        human_readable = EXCEPTION_FIELD_MAPPING.get('itemcode', 'Item Code')
        if human_readable not in exception_fields:
            exception_fields.append(human_readable)
    
    # Check for missing field values in exception status
    if "missing field values" in exceptionStatus.lower():
        # Extract field names from parentheses if present
        matches = re.findall(r'\((.*?)\)', exceptionStatus)
        for match in matches:
            field_names = [name.strip() for name in match.split(',')]
            for field_name in field_names:
                # Convert to human readable
                field_key = field_name.lower().replace(' ', '').replace('_', '')
                human_readable = EXCEPTION_FIELD_MAPPING.get(field_key, field_name)
                if human_readable not in exception_fields:
                    exception_fields.append(human_readable)
    
    return ", ".join(exception_fields)

@tracer.capture_method
def getSequenceNumberGenerator(sequenceNumberGeneratorId):
    """Get sequence number generator record"""
    response = SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.get_item(
        Key={'sequenceNumberGeneratorId': sequenceNumberGeneratorId}
    )
    return response.get('Item')

@tracer.capture_method
def createSequenceNumberGenerator(sequenceNumberGeneratorId, now):
    """Create new sequence number generator record"""
    SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.put_item(Item={
        'sequenceNumberGeneratorId': sequenceNumberGeneratorId,
        'latestValue': '0001',
        'updatedAt': now
    })

@tracer.capture_method
def updateSequenceNumberGenerator(sequenceNumberGeneratorId, latestValue, now):
    """Update sequence number generator with new value"""
    SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.update_item(
        Key={'sequenceNumberGeneratorId': sequenceNumberGeneratorId},
        UpdateExpression='SET latestValue=:latestValue, updatedAt=:updatedAt',
        ExpressionAttributeValues={
            ':latestValue': latestValue,
            ':updatedAt': now
        }
    )

@tracer.capture_method
def strip_bounding_boxes(invoice_data):
    invoice_copy = copy.deepcopy(invoice_data)
    
    # Remove and store top-level bounding boxes
    bounding_boxes = invoice_copy.pop("boundingBoxes", {})
    
    # Process line items
    line_item_bounding_boxes = {}
    formatted_items = []
    
    for idx, item in enumerate(invoice_copy.get("lineItem", [])):
        item_id = f"item_{idx}"
        if "boundingBoxes" in item:
            line_item_bounding_boxes[item_id] = item.pop("boundingBoxes")
        item["item_list_id"] = item_id
        formatted_items.append(item)
    
    invoice_copy['lineItem'] = formatted_items
    
    return invoice_copy, bounding_boxes, line_item_bounding_boxes

@tracer.capture_method
def restore_bounding_boxes(invoice_data, bounding_boxes, line_item_bounding_boxes):
    invoice_copy = copy.deepcopy(invoice_data)
    
    # Restore top-level bounding boxes
    invoice_copy["boundingBoxes"] = bounding_boxes
    
    # Restore line item bounding boxes
    for idx, item in enumerate(invoice_copy.get("lineItem", [])):
        item_id = f"item_{idx}"
        if item_id in line_item_bounding_boxes:
            item["boundingBoxes"] = line_item_bounding_boxes[item_id]
        # Remove item_list_id if it was added during processing
        if "item_list_id" in item:
            item.pop("item_list_id")
    
    return invoice_copy

@tracer.capture_method
def safe_float(value):
    """Convert value to float safely"""
    if value is None or value == "" or value == "-":
        return 0.0
    
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert to float: {value}, defaulting to 0")
        return 0.0

@tracer.capture_method
def getRequiredFields(merchantConfig):
    """Get required fields configuration for merchant"""
    required_fields_config = merchantConfig.get('requiredFields', {})
    
    try:
        if isinstance(required_fields_config, dict) and required_fields_config.get('path'):
            # Fetch from S3
            response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=required_fields_config['path'])
            content = response['Body'].read().decode('utf-8')
            required_fields = json.loads(content)
            return required_fields
        elif isinstance(required_fields_config, dict) and ('invoice' in required_fields_config or 'lineItem' in required_fields_config):
            # Direct configuration
            return required_fields_config
    except Exception as e:
        logger.warning(f"Failed to get required fields: {str(e)}. Using default required fields.")
    
    # Default required fields
    return {
        'invoice': ['invoiceNumber', 'invoiceDate', 'supplierName'],
        'lineItem': ['description', 'quantity', 'unitPrice']
    }

@tracer.capture_method
def normalizePageNumber(pageNumber):
    """Normalize page number to ensure it's an integer"""
    try:
        max_depth = 10  # Prevent infinite recursion
        depth = 0
        
        while isinstance(pageNumber, list) and depth < max_depth:
            if len(pageNumber) > 0:
                pageNumber = pageNumber[0]
                depth += 1
            else:
                return 1
        
        # Convert to integer
        if isinstance(pageNumber, (int, float)):
            return int(pageNumber)
        elif isinstance(pageNumber, str) and pageNumber.isdigit():
            return int(pageNumber)
        else:
            return 1
            
    except Exception:
        return 1

@tracer.capture_method
def setBackUpLineItemTotalPrice(lineItem):
    """Calculate totalPrice from amountExclTax and taxAmount if totalPrice is empty, with subTotal-discount as backup"""
    if lineItem.get('totalPrice'):
        return lineItem
    
    amount_excl_tax = lineItem.get('amountExclTax', 0)
    tax_amount = lineItem.get('taxAmount', 0)
    sub_total_amount = lineItem.get('subTotal', 0)
    discount_amount = lineItem.get('discountAmount', 0)

    
    # First try: amountExclTax + taxAmount (if both have values > 0)
    excl_tax_float = float(str(amount_excl_tax).replace(',', '')) if amount_excl_tax else 0
    tax_amount_float = float(str(tax_amount).replace(',', '')) if tax_amount else 0
    
    if excl_tax_float > 0 or tax_amount_float > 0:
        calculated_total = excl_tax_float + tax_amount_float
        lineItem['totalPrice'] = str(calculated_total)
        lineItem['boundingBoxes']['totalPrice'] = (
            lineItem['boundingBoxes'].get('amountExclTax', []) + 
            lineItem['boundingBoxes'].get('taxAmount', [])
        )
    else:
        # Backup: subTotal - discountAmount (if subTotal has value > 0)
        sub_total_float = float(str(sub_total_amount).replace(',', '')) if sub_total_amount else 0
        discount_amount_float = float(str(discount_amount).replace(',', '')) if discount_amount else 0
        
        if sub_total_float > 0:
            calculated_total = sub_total_float - discount_amount_float
            lineItem['totalPrice'] = str(calculated_total)
            lineItem['boundingBoxes']['totalPrice'] = (
                lineItem['boundingBoxes'].get('subTotal', []) + 
                lineItem['boundingBoxes'].get('discountAmount', [])
            )
    
    return lineItem

@tracer.capture_method
def queryDDBTable(table, key, value, field=None, index_name=None):
    """
    General query function that can handle both primary key and GSI queries
    """
    if not key or not value:
        return None
    
    try:
        # Build query parameters
        query_params = {
            'KeyConditionExpression': Key(key).eq(value)
        }
        
        # Add index name if provided (for GSI queries)
        if index_name:
            query_params['IndexName'] = index_name
        
        # Execute query
        response = table.query(**query_params)
        items = response.get('Items', [])
        
        if not items:
            return None
        
        # Get the first item and extract the requested field
        item = items[0]
        if field is not None:
            return item.get(field)
        else:
            return item
            
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB error in queryDDBTable: {error_message}")
        return {"success": False, "error": f"Database error: {error_message}", "data": None}
        
    except Exception as e:
        logger.error(f"Unexpected error in queryDDBTable: {str(e)}")
        return {"success": False, "error": f"Unexpected error: {str(e)}", "data": None}

@tracer.capture_method
def convert_floats_to_decimals(obj):
    """Recursively convert all float values in a nested structure to Decimal"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimals(item) for item in obj]
    else:
        return obj

@tracer.capture_method
def performMasterDataChecking(invoice, merchantConfig):
    total_input_tokens = 0
    total_output_tokens = 0

    # Get merchant settings from config
    custom_logic = merchantConfig.get('customLogics')
    useStoreMapping = custom_logic.get('useStoreMapping') 
    
    original_confidence_score = invoice.get('confidenceScore')
    
    # 1. Vendor mapping using OpenSearch (first in sequence)
    invoice = performVendorOpenSearchMapping(invoice)
    
    # 2. Store mapping using OpenSearch (second in sequence, if enabled)
    if useStoreMapping:
        invoice, store_input_tokens, store_output_tokens = performStoreOpenSearchMapping(invoice, merchantConfig)
        total_input_tokens += store_input_tokens
        total_output_tokens += store_output_tokens

    # 3. Item mapping using OpenSearch + existing prompt (third in sequence)
    invoice, line_item_input_tokens, line_item_output_tokens = performLineItemMasterMapping(invoice, merchantConfig)

    if original_confidence_score is not None:
        invoice['confidenceScore'] = original_confidence_score
        
    total_input_tokens = line_item_input_tokens
    total_output_tokens = line_item_output_tokens

    return invoice, total_input_tokens, total_output_tokens

@tracer.capture_method
def performVendorOpenSearchMapping(invoice):
    """Direct vendor mapping using OpenSearch - returns vendor code directly without LLM"""
    supplier_name = invoice.get("supplierName", "").strip()
    
    if not supplier_name:
        invoice['supplierCode'] = "-"
        invoice['status'] = "Exceptions"
        invoice['exceptionStatus'] = "Vendor name not provided"
        return invoice
    
    # Try exact match first
    exact_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"vendor name.keyword": supplier_name}}
                ]
            }
        }
    }
    
    vendor_results = searchOpenSearchForMapping(SUPPLIER_INDEX, exact_query, max_results=5)
    
    # If no exact match, try semantic search
    if not vendor_results:
        semantic_query = {
            "query": {
                "match": {
                    "vendor name": supplier_name
                }
            }
        }
        vendor_results = searchOpenSearchForMapping(SUPPLIER_INDEX, semantic_query, max_results=5)
    
    if vendor_results:
        # Take the first (best) match
        matched_vendor = vendor_results[0]['_source']
        invoice['supplierCode'] = matched_vendor.get("vendor code", "-")
        invoice['status'] = "Success"
        invoice['exceptionStatus'] = "N/A"
    else:
        invoice['supplierCode'] = "-"
        invoice['status'] = "Exceptions"
        invoice['exceptionStatus'] = "Vendor with name not found in the database"
    
    return invoice

@tracer.capture_method
def searchOpenSearchForMapping(index_name, query_body, max_results=10):
    """Search OpenSearch for mapping data using hybrid approach (exact then semantic)"""
    try:
        url = f'https://{ES_DOMAIN_ENDPOINT}/{index_name}/_doc/_search'
        headers = {"Content-Type": "application/json"}
        
        # Set size for pagination
        query_body["size"] = min(max_results, 10000)
        
        # Make initial request
        response = requests.post(url, auth=AWSAUTH, headers=headers, data=json.dumps(query_body))
        # response.raise_for_status()
        results = response.json()

        hits = results.get('hits', {}).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)
        
        # Handle pagination if needed and if we need more results
        if total > 10000 and max_results > 10000:
            from_value = 10000
            while from_value < total and len(hits) < max_results:
                query_body["from"] = from_value
                response = requests.post(url, auth=AWSAUTH, headers=headers, data=json.dumps(query_body))
                response.raise_for_status()
                more_results = response.json()
                new_hits = more_results.get('hits', {}).get('hits', [])
                hits.extend(new_hits)
                from_value += 10000
                
                # Stop if we've reached our desired max_results
                if len(hits) >= max_results:
                    hits = hits[:max_results]
                    break
        
        return hits

    except Exception as e:
        logger.error(f"OpenSearch search failed for index {index_name}: {str(e)}")
        return []

@tracer.capture_method
def fetch_prompt(prompt_path, default_prompt):
    """Fetch custom prompt from S3 or return default prompt"""
    if not prompt_path:
        return default_prompt
    
    try:
        response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=prompt_path)
        custom_prompt = response['Body'].read().decode('utf-8')
        return custom_prompt
    except Exception as e:
        logger.warning(f"Failed to fetch custom prompt from {prompt_path}: {str(e)}. Using default prompt.")
        return default_prompt

@tracer.capture_method
def create_item_mapping_prompt(mapping_batch_str, invoice_items, merchantConfig):
    formatted_items = json.dumps(invoice_items)
    default_prompt = defaultPrompts.LINE_ITEM_MASTER_MAPPING_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchantConfig.get('promptPaths', {})
    item_mapping_prompt_path = prompt_paths.get('itemMappingPrompt')
    prompt_template = fetch_prompt(item_mapping_prompt_path, default_prompt)
    
    prompt = prompt_template.format(
        database=mapping_batch_str,
        formatted_items=formatted_items
    )

    return prompt

@tracer.capture_method
def createStoreMappingPrompt(mapping_batch_str, premise_address, merchantConfig):
    input_item = {
        "premiseAddress": premise_address
    }

    default_prompt = defaultPrompts.STORE_MASTER_MAPPING_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchantConfig.get('promptPaths', {})
    store_mapping_prompt_path = prompt_paths.get('storeMappingPrompt')
    prompt_template = fetch_prompt(store_mapping_prompt_path, default_prompt)

    prompt = prompt_template.format(
        database=mapping_batch_str, 
        input_item=json.dumps(input_item)
    )

    return prompt

def clean_to_decimal(value):
    if not value or value == "-":
        return Decimal('0')
    # Remove currency symbols before conversion
    cleaned = str(value).replace('RM', '').replace('USD', '').replace('SGD', '').replace('$', '').replace(',', '').strip()
    try:
        return Decimal(cleaned)
    except (ValueError, TypeError, decimal.InvalidOperation):
        logger.warning(f"Could not convert '{value}' (cleaned: '{cleaned}') to Decimal, using 0")
        return Decimal('0')
    
@tracer.capture_method
def performLineItemMasterMapping(invoice, merchantConfig):
    mapped_line_items, input_tokens, output_tokens = performItemOpenSearchMapping(
        invoice, 
        merchantConfig
    )
    
    invoice["lineItem"] = mapped_line_items
    
    return invoice, input_tokens, output_tokens

@tracer.capture_method
def performItemOpenSearchMapping(invoice, merchantConfig):
    """Item mapping using OpenSearch to get top 5 matches, then use existing ItemMappingFM prompt"""
    invoice = copy.deepcopy(invoice)
    invoice_items = invoice.get("lineItem", [])
    total_input_tokens = 0
    total_output_tokens = 0
    formatted_items = []
    preserved_fields_map = {}

    # Prepare items same as original code
    for index, item in enumerate(invoice_items):
        item_id = f"item_{index}"
        preserved_fields_map[item_id] = {
            "quantity": item.get("quantity"),
            "totalPrice": item.get("totalPrice"),
            "subTotal": item.get("subTotal"),
            "discountAmount": item.get("discountAmount"),
            "boundingBoxes": item.get("boundingBoxes")
        }
        
        item_payload = {
            "item_list_id": item_id,
            "supplierName": item.get("supplierName"),
            "supplierCode": invoice.get("supplierCode"),
            "description": item.get("description"),
            "unitPrice": item.get("unitPrice"),
            "uom": item.get("uom"),
            "storeLocation": invoice.get("storeLocation"),
            "purchaserCode": item.get("purchaserCode"),
            "buyerGroup": item.get("buyerGroup"),
        }
        formatted_items.append(item_payload)
        print ("Input data", item_payload)
    # Get vendor code for filtering
    supplier_code = invoice.get("supplierCode", "")
    all_items = []
    item_results = []

    if supplier_code and supplier_code != "-":
        # Get individual descriptions for each item
        valid_items = [
            item for item in formatted_items 
            if item.get("description") and item.get("description") != "-" and item.get("description").strip()
        ]
        
        # Add safety check for empty descriptions
        if not valid_items:
            # Create unmapped items for all
            for item_payload in formatted_items:
                unmapped_item = createUnmappedItem(item_payload)
                all_items.append(unmapped_item)
        else:
            # Query each item individually and combine results

            for item_payload in valid_items:
                description = item_payload.get("description", "").strip()
                logger.info(f"Exact query for: {description}")
                
                ## EXACT MATCH QUERY
                exact_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"vendor.keyword": supplier_code}},
                                {"term": {"item description.keyword": description}}
                            ]
                        }
                    }
                }
                
                individual_results = searchOpenSearchForMapping(LINE_ITEM_INDEX, exact_query, max_results=25)
                logger.info(f"Exact results for '{description}': {len(individual_results)}")

                ## Skip semantic search
                if len(individual_results) > 0:
                    item_results.extend(individual_results)
                    continue
            
               
                ## SEMANTIC MATCH QUERY
                semantic_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"vendor.keyword": supplier_code}},
                                {"match": {"item description": description}}
                            ]
                        }
                    }
                }
                
                individual_results = searchOpenSearchForMapping(LINE_ITEM_INDEX, semantic_query, max_results=25)
                logger.info(f"Semantic results for '{description}': {len(individual_results)}")
                item_results.extend(individual_results)

            if item_results:
                # Format OpenSearch results to match expected prompt format
                formatted_batch = formatOpenSearchItemResults(item_results)
                print(formatted_batch)
                # Use existing ItemMappingFM prompt
                prompt = create_item_mapping_prompt(formatted_batch, formatted_items, merchantConfig)
                
                try:
                    batch_result, batch_input_tokens, batch_output_tokens = promptBedrock(prompt)
                    total_input_tokens += batch_input_tokens
                    total_output_tokens += batch_output_tokens
                    
                    # Parse the JSON response
                    item_json_results = json.loads(batch_result)
                    
                    if isinstance(item_json_results, list):
                        all_items = item_json_results
                    else:
                        logger.error(f"Batch result is not a list: {type(item_json_results)}")
                        all_items = []
                        
                except Exception as e:
                    logger.error(f"Error processing batch: {str(e)}")
                    all_items = []
            else:
                all_items = []
    else:
        all_items = []
    
    # If we don't have results for all items, fill in the missing ones
    if len(all_items) < len(formatted_items):
        # Create unmapped items for missing ones
        for i in range(len(all_items), len(formatted_items)):
            unmapped_item = createUnmappedItem(formatted_items[i])
            all_items.append(unmapped_item)
    
    # Final validation
    validated_items = []
    for item in all_items:
        if isinstance(item, dict):
            validated_items.append(item)
        else:
            # Create fallback dict
            fallback_item = {
                "description": "Error in processing",
                "itemCode": "-",
                "purchaserCode": "-", 
                "buyerGroup": "-",
                "status": "Exceptions",
                "exceptionStatus": "Processing error",
                "completeMapping": False
            }
            validated_items.append(fallback_item)

    # Restore preserved fields
    for item in validated_items:
        item_id = item.pop("item_list_id", None)        
        if item_id and item_id in preserved_fields_map:
            preserved_fields = preserved_fields_map[item_id]
            for field_name, field_value in preserved_fields.items():
                if field_value is not None:
                    item[field_name] = field_value

    return validated_items, total_input_tokens, total_output_tokens

@tracer.capture_method
def performStoreOpenSearchMapping(invoice, merchantConfig):
    """Store mapping using OpenSearch to get top 5 matches, then use StoreMappingFM prompt"""
    total_input_tokens = 0
    total_output_tokens = 0
    storeCode = formatStoreCode(invoice)
    if isEmptyValue(storeCode):
        premise_address = invoice.get("storeLocation", "").strip()
        premise_address = normalize_text(premise_address)
        keywords = extract_keywords(premise_address)

        words = re.findall(r'\b\w{2,}\b', premise_address)  # Only words with 2+ characters
        keywords = [word for word in words if not re.match(r'^\d+\.?\d*$', word)]
        
        if not premise_address or premise_address == "-":
            invoice['locationCode'] = "-"
            invoice['dim'] = "-"
            return invoice, 0, 0  # Return input/output tokens
        
        # Try exact match first
        exact_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"store name.keyword": premise_address}}
                    ]
                }
            }
        }

        store_results = searchOpenSearchForMapping(STORE_INDEX, exact_query, max_results=10)
        
        # # If no exact match, try keyword search
        # if not store_results:
        #     keyword_query = {
        #         'query': {
        #             "bool": {
        #                 "must": [
        #                     {"term": {"active status": "active"}}
        #                 ],
        #                 "should": [
        #                     {
        #                         "match": {
        #                             "store name": {
        #                                 "query": " ".join(keywords),
        #                                 "operator": "and",
        #                                 "boost": 2.0
        #                             }
        #                         }
        #                     },
        #                     {
        #                         "match": {
        #                             "store name": {
        #                                 "query": " ".join(keywords),
        #                                 "operator": "or",
        #                                 "boost": 1.5
        #                             }
        #                         }
        #                     }
        #                 ] + [
        #                     {
        #                         "match": {
        #                             "store name": {
        #                                 "query": keyword,
        #                                 "boost": 1.0
        #                             }
        #                         }
        #                     } for keyword in keywords
        #                 ],
        #                 "minimum_should_match": 2                  
        #             }
        #         }
        #     }
        #     store_results = searchOpenSearchForMapping(STORE_INDEX, keyword_query, max_results=15)
        #     print("KeyWord Search", store_results)
            
        if not store_results:
            # Prepare different wildcard patterns
            concatenated_keywords = "".join(keywords)  # "TmnCahayaMasai"
            
            semantic_query = {
                'query': {
                    "bool": {      
                        "must": [
                            {"term": {"active status": "active"}}
                        ],
                        "should": [
                            # Original fuzzy matching
                            {
                                "match": {
                                    "store name": {
                                        "query": premise_address,
                                        "fuzziness": "AUTO",
                                        "boost": 3.0
                                    }
                                }
                            },
                            # Wildcard search for concatenated keywords
                            {
                                "wildcard": {
                                    "store name": {
                                        "value": f"*{concatenated_keywords}*",
                                        "boost": 2.5
                                    }
                                }
                            },
                            # Wildcard search with each keyword separated
                            {
                                "wildcard": {
                                    "store name": {
                                        "value": f"*{'*'.join(keywords)}*",
                                        "boost": 2.0
                                    }
                                }
                            },
                            # Individual keyword wildcards
                            *[
                                {
                                    "wildcard": {
                                        "store name": {
                                            "value": f"*{keyword}*",
                                            "boost": 1.0
                                        }
                                    }
                                } for keyword in keywords if len(keyword) > 2
                            ],
                            # Case-insensitive wildcard for the full premise address
                            {
                                "wildcard": {
                                    "store name": {
                                        "value": f"*{premise_address.replace(' ', '*')}*",
                                        "boost": 1.5,
                                        "case_insensitive": True
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                }
            }
            store_results = searchOpenSearchForMapping(STORE_INDEX, semantic_query, max_results=100)

        
        if store_results:
            # Format OpenSearch results for prompt
            formatted_batch = formatOpenSearchStoreResults(store_results)
            
            # Use StoreMappingFM prompt
            prompt = createStoreMappingPrompt(formatted_batch, premise_address, merchantConfig)
            
            store_result, input_tokens, output_tokens = promptBedrock(prompt)
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            
            # Parse the JSON response
            store_mapping = json.loads(store_result)
            
            # Update invoice with mapped store information
            invoice['buyerName'] = store_mapping.get("storeName", invoice.get('buyerName', ""))
            invoice['locationCode'] = store_mapping.get("locationCode", "-")
            invoice['dim'] = store_mapping.get("locationCode", "-")
            
            # Handle status and exceptions
            if store_mapping.get('status') == "Exceptions":
                if invoice.get('status') != "Exceptions":
                    invoice['status'] = store_mapping.get('status')
                    invoice['exceptionStatus'] = store_mapping.get("exceptionStatus")
                else:
                    # Append to existing exception status
                    current_exception = invoice.get('exceptionStatus', '')
                    if current_exception and current_exception != 'N/A':
                        invoice['exceptionStatus'] = f"{current_exception} and {store_mapping.get('exceptionStatus')}"
                    else:
                        invoice['exceptionStatus'] = store_mapping.get("exceptionStatus")
    elif not isEmptyValue(storeCode):
        # If storeCode is provided, use it directly
        invoice['locationCode'] = storeCode
        invoice['dim'] = storeCode

    else:
        invoice['locationCode'] = "-"
        invoice['dim'] = "-"
        
        # Set exception status if store mapping was expected but failed
        if invoice.get('status') != "Exceptions":
            invoice['status'] = "Exceptions"
            invoice['exceptionStatus'] = "Store mapping failed - address not found in database"
        else:
            # Append to existing exception status
            current_exception = invoice.get('exceptionStatus', '')
            if current_exception and current_exception != 'N/A':
                invoice['exceptionStatus'] = f"{current_exception} and store mapping failed"
            else:
                invoice['exceptionStatus'] = "Store mapping failed - address not found in database"
    
    return invoice, total_input_tokens, total_output_tokens

@tracer.capture_method
def createUnmappedItem(item_payload):
    """Helper function to create unmapped item structure"""
    return {
        "supplierCode": item_payload.get("supplierCode", ""),
        "description": item_payload.get("description", ""),
        "unitPrice": item_payload.get("unitPrice", ""),
        "uom": item_payload.get("uom", "EA"),
        "storeLocation": item_payload.get("storeLocation", ""),
        "storeName": item_payload.get("storeName", ""),
        "purchaserCode": "-",
        "buyerGroup": "-",
        "itemCode": "-",
        "itemType": "Item",
        "status": "Exceptions",
        "exceptionStatus": "Master Mapping Error on line item",
        "completeMapping": False
    }

@tracer.capture_method
def formatOpenSearchItemResults(opensearch_results):
    """Convert OpenSearch item results to the exact format expected by ItemMappingFM prompt"""
    columns = "vendor code|item code|item description|item description2|uom|unit price|item status"
    rows = []
    
    for result in opensearch_results:
        source = result.get('_source', {})
        
        # Map OpenSearch fields to expected format
        vendor_code = source.get("vendor", "")
        item_code = source.get("item code", "")
        item_description = source.get("item description", "")
        item_description2 = source.get("item description2", "")
        uom = source.get("uom", "")
        unit_price = source.get("unit price", "")
        item_status = source.get("item status", "")
        
        formatted_row = f"{vendor_code}|{item_code}|{item_description}|{item_description2}|{uom}|{unit_price}|{item_status}"
        rows.append(formatted_row)
    
    final_result = f"Columns: {columns}\n" + "\n".join(rows)
    
    return final_result

def normalize_text(premise_address: str) -> str:
    """Basic text normalization"""
    # Replace FM/Fm variations with FamilyMart
    premise_address = re.sub(r'\bFM\b', 'FamilyMart', premise_address, flags=re.IGNORECASE)
    premise_address = re.sub(r'\bFm\b', 'FamilyMart', premise_address)
    premise_address = re.sub(r'Family\s+Mart', 'FamilyMart', premise_address, flags=re.IGNORECASE)
    premise_address = re.sub(r'\s*-\s*', ' ', premise_address)
    premise_address = re.sub(r'\s+', ' ', premise_address)
    return premise_address.strip()

def extract_keywords(premise_address: str) -> List[str]:
    """Extract keywords by splitting the text"""
    # Remove brand names and clean up
    premise_address = re.sub(r'\b(FM|Fm|Family\s*Mart|FamilyMart)\b', '', premise_address, flags=re.IGNORECASE)
    premise_address = re.sub(r'^[-\s]+|[-\s]+$', '', premise_address)  # Remove leading/trailing dashes and spaces

    # Split into words and filter out numbers/short words
    words = re.findall(r'\b\w{2,}\b', premise_address)  # Only words with 2+ characters
    keywords = [word for word in words if not re.match(r'^\d+\.?\d*$', word)]
    return keywords

@tracer.capture_method
def formatOpenSearchStoreResults(opensearch_results):
    """Convert OpenSearch store results to the exact format expected by StoreMappingFM prompt"""
    columns = "store name|store code|active status"
    rows = []
    
    for result in opensearch_results:
        source = result.get('_source', {})
        
        # Map OpenSearch fields to expected format
        store_name = source.get("store name", "")
        store_code = source.get("store code", "")
        active_status = source.get("active status", "")
        
        formatted_row = f"{store_name}|{store_code}|{active_status}"
        rows.append(formatted_row)
    
    final_result = f"Columns: {columns}\n" + "\n".join(rows)
    
    return final_result

@tracer.capture_method
def formatStoreCode(invoice):
    storeCode = invoice.get("storeCode")
    if isEmptyValue(storeCode):
        return "-"


    storeCodeStr = str(storeCode).strip()
    if isEmptyValue(storeCodeStr):
        return "-"

    if not storeCodeStr.isdigit():
        return "-"
    
    currentLength = len(storeCodeStr)

    if currentLength == 4:
        return storeCodeStr
    
    if currentLength > 4:
        return "-"

    formattedStoreCode = storeCodeStr.zfill(4)
    return formattedStoreCode
    
