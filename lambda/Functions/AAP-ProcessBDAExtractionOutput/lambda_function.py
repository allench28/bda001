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
import urllib.parse

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
# SUPPLIER_TABLE = os.environ.get('SUPPLIER_TABLE')
# SUPPLIER_ITEM_TABLE = os.environ.get('SUPPLIER_ITEM_TABLE')
# STORE_TABLE = os.environ.get('STORE_TABLE')


S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs', region_name='us-east-1')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_TABLE)
EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_LINE_ITEM_TABLE)
EXTRACTED_PO_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_PO_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_PO_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
SEQUENCE_NUMBER_GENERATOR_DDB_TABLE = DDB_RESOURCE.Table(SEQUENCE_NUMBER_GENERATOR_TABLE)

# SUPPLIER_DDB_TABLE = DDB_RESOURCE.Table(SUPPLIER_TABLE)
# SUPPLIER_ITEM_DDB_TABLE = DDB_RESOURCE.Table(SUPPLIER_ITEM_TABLE)
# STORE_DDB_TABLE = DDB_RESOURCE.Table(STORE_TABLE)

logger = Logger()
tracer = Tracer()

IGNORED_EXCEPTION_FIELDS = {
    'linediscountamount', 'generalcomment', 'comment', 'expectedreceipt'
}
EXCEPTION_FIELD_MAPPING = {
    'itemcode': 'No.',
    # 'description': 'Description',
    # 'unitprice': 'Unit Price',
    # 'uom': 'Unit of Measure',
    'invoicedate': 'Order Date',
    'quantity': 'Quantity',
    'invoicenumber': 'Vendor Order No / Vendor Shipment No',
    # 'totalprice': 'Total Price',
    'purchasercode': 'Purchaser Code',
    'locationcode': 'Location Code / Dim',
    'buyergroup': 'Buyer Group',
    'suppliercode': 'Buy From Vendor Number',
    'itemtype': 'Item Type',
}

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
            file_path = urllib.parse.unquote(body.get('file_path', ''))

            # NEW: Single merchant table query at the start
            merchant_config = getMerchantConfiguration(merchantId)
            
            updatedMappedJsonData = []
            
            if not result_json_list:
                continue

            # if source_file_name.split('_')[0] != 'invoice':
            if source_file_name.split('_')[0] == 'po' and 'grn' in source_file_name:
                unsupportExtractedDocumentTypePayload = {
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
                    'boundingBoxes': "{}",
                    "status": "Exceptions",
                    "exceptionStatus": "Document Format Unrecognized",
                    "createdAt": now,
                    "createdBy": "System",
                    "updatedAt": now,
                    "updatedBy": "System",
                    "sourceFile": source_file_name,
                    "confidenceScore": 0,
                    "documentUploadId": documentUploadId
                }
                createExtractedResultRecord(unsupportExtractedDocumentTypePayload, merchantId, documentUploadId, source_file_name, file_path, now, merchant_config)
                updatedMappedJsonData.append(unsupportExtractedDocumentTypePayload)
                updateFailedDocumentUploadStatus(documentUploadId, "Document Type Unrecognized")
                createTimelineRecord(merchantId, unsupportExtractedDocumentTypePayload, now)
                continue

            mappedJsonData = processJsonResult(result_json_list)

            for invoice in mappedJsonData:
                logger.info(f'INVOICE: {invoice}')

                # Pass merchant_config to functions instead of merchantId
                invoice = applyMerchantSpecificLogic(invoice, merchant_config)
                logger.info(f'INVOICE AFTER MERCHANT-SPECIFIC LOGIC: {invoice}')

                invoice, input_tokens, output_tokens= performMasterDataChecking(invoice, merchant_config)
                logger.info(f'INVOICE AFTER MASTER DATA CHECKING: {invoice}')
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                invoice = performDuplicateChecking(invoice, merchantId)
                logger.info(f'INVOICE AFTER DUPLICATE CHECKING: {invoice}')
                
                invoice, input_tokens, output_tokens = performStandardization(invoice, merchant_config)
                logger.info(f'INVOICE AFTER STANDARDIZATION: {invoice}')
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                invoice = performMissingFieldChecking(invoice, merchant_config)
                logger.info(f'INVOICE AFTER MISSING FIELD CHECKING: {invoice}')

                invoice = performAmountChecking(invoice, merchant_config)
                logger.info(f'INVOICE AFTER AMOUNT CHECKING: {invoice}')

                invoice, input_tokens, output_tokens = performExceptionChecking(invoice, merchant_config)
                logger.info(f'INVOICE AFTER EXCEPTION CHECKING: {invoice}')
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                invoice = applyInvoiceToPOExceptionLogic(invoice, merchant_config)

                invoice = createExtractedResultRecord(invoice, merchantId, documentUploadId, source_file_name, file_path, now, merchant_config)

                # Pass merchant_config instead of merchantId
                performInvoiceToPOConversion(invoice, merchant_config, documentUploadId, now)

                if invoice.get("status") == "Success":
                    send_to_erp_sqs(invoice, merchantId)
                
                createTimelineRecord(merchantId, invoice, now)
                updatedMappedJsonData.append(invoice)
                
            logger.info(f'UPDATED MAPPED JSON: {updatedMappedJsonData}')
            input_tokens, output_tokens = updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData, now)
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

            logger.info(f'TOTAL INPUT TOKENS: {total_input_tokens}')
            logger.info(f'TOTAL OUTPUT TOKENS: {total_output_tokens}')
            
            generate_combined_csv(mappedJsonData, merchantId, documentUploadId, source_file_name, now, day)
            
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
        return {
            "status": True,
            'body': "The server encountered an unexpected condition that prevented it from fulfilling your request."
        }

@tracer.capture_method
def performAmountChecking(invoice, merchant_config):
    invoiceData = copy.deepcopy(invoice)

    #Skip amount checking if invoiceToPO logic is enabled
    if merchant_config:
        custom_logics = merchant_config.get('customLogics', {})
        if custom_logics.get('invoiceToPO', False):
            invoiceData['amountException'] = "N/A"
            return invoiceData
    
    # Calculate sum of line item total prices
    lineItemTotalPrice = sum([safe_float(item.get('totalPrice', 0)) for item in invoiceData.get('lineItem', [])])
    totalInvoiceAmount = safe_float(invoiceData.get('totalAmount', 0))
    
    # Handle cases where tax is marked as not applicable
    if invoiceData.get('taxType') == "-" and invoiceData.get('taxRate') == "-":
        invoiceData['taxAmount'] = "0"
    
    taxAmount = safe_float(invoiceData.get('taxAmount', 0))
    
    # Check if total invoice amount matches sum of line items (with tolerance for floating-point comparison)
    if abs(totalInvoiceAmount - lineItemTotalPrice) > 0.02:
        # Calculate subtotal by removing tax from total
        subTotal = totalInvoiceAmount - taxAmount
        invoiceData['subTotal'] = str(subTotal)
        
        # Check if subtotal matches line item total
        if abs(subTotal - lineItemTotalPrice) > 0.02:
            logger.info(f"Line item total price {lineItemTotalPrice} does not match invoice total price {subTotal}")
            invoiceData['amountException'] = "Sum of line items price does not match invoice total price"

        else:
            invoiceData['amountException'] = "N/A"
    else:
        invoiceData['amountException'] = "N/A"

    return invoiceData


@tracer.capture_method
def generate_combined_csv(mappedJsonData, merchantId, documentUploadId, source_file_name, now, day):
    file_name = source_file_name.replace(".pdf", "")
    success_file_name = f"invoice_table_{file_name}_{now}.csv"
    success_csv_key = f"extracted_data/{merchantId}/{documentUploadId}/{day}/{success_file_name}"
    failed_file_name = f"invoice_flagged_{file_name}_{now}.csv"
    failed_csv_key = f"extracted_data/{merchantId}/{documentUploadId}/{day}/{failed_file_name}"
    
    csv_data = []
    error_csv_data = []
    error_headers = []
    headers = [
        "Vendor_Name", 
        "Invoice_Date", 
        "Invoice_Number", 
        "Store_Location",
        "Category",
        "Description",
        "Amount_Excl_Tax",
        "Tax_Amount",
        "Total_Amount",
        "Due_Date",
        "Source_File"
    ]
    
    # Define a mapping of CSV columns to possible JSON keys
    field_mapping = {
        "Vendor_Name": ["supplierName"],
        "Invoice_Date": ["invoiceDate"],
        "Invoice_Number": ["invoiceNumber"],
        "Store_Location": ["buyerAddress"],
        "Category": ["category"],
        "Description": ["description"],
        "Amount_Excl_Tax": ["amountWithoutTax"],
        "Tax_Amount": ["taxAmount"],
        "Total_Amount": ["totalAmountInclTax"],
        "Due_Date": ["dueDate"],
    }

    for jsonData in mappedJsonData:
        row = []
        for lineItem in jsonData.get("lineItem", []):
            for column, possible_keys in field_mapping.items():
                value = ""
                for key in possible_keys:
                    if key in lineItem:
                        value = lineItem[key]
                        break
                row.append(value)
            row.append(source_file_name)
            csv_data.append(row)

        ## Generate failed report
        if len(csv_data) == 0:
            error_headers.extend(headers)
            error_headers.append("Error_Message")
            error_row = ["", "", "", "", "", "", "", "", "", "", source_file_name, "No table data extracted from the file"]
            error_csv_data.append(error_row)
    
    if len(csv_data) > 0:
        
        if os.path.exists('/tmp/' + success_file_name):
            os.remove('/tmp/' + success_file_name)
        
        with open('/tmp/' + success_file_name, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(headers)
            writer.writerows(csv_data)
        
        S3_CLIENT.upload_file('/tmp/' + success_file_name, BDA_PROCESSING_BUCKET, success_csv_key)
        os.remove('/tmp/' + success_file_name)
            
    else:
        
        if os.path.exists('/tmp/' + failed_file_name):
            os.remove('/tmp/' + failed_file_name)

        with open('/tmp/' + failed_file_name, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(error_headers)
            writer.writerows(error_csv_data)

        
        S3_CLIENT.upload_file('/tmp/' + failed_file_name, BDA_PROCESSING_BUCKET, failed_csv_key)

        
        os.remove('/tmp/' + failed_file_name)

@tracer.capture_method
def performExceptionChecking(invoice, merchant_config):
    # Replace the manual stripping code with:
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)
    
    # Get default prompt
    default_prompt = defaultPrompts.EXCEPTION_STATUS_CHECKING_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchant_config.get('promptPaths', {})
    exception_checking_prompt_path = prompt_paths.get('exceptionCheckingPrompt')
    prompt_template = fetch_prompt(exception_checking_prompt_path, default_prompt)
    
    prompt = prompt_template.format(invoiceData=invoiceData)
    response, input_tokens, output_tokens = promptBedrock(prompt)
    
    invoice_json = json.loads(response)
    
    # Replace the manual restoration code with:
    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def processJsonResult(result_json_list):
    mappedJsonData = []

    field_mapping = {
        "invoiceNumber": ["InvoiceNumber"],
        "invoiceDate": ["InvoiceDate"],
        "supplierName": ["Vendor", "VendorName"],
        "supplierAddress": ["VendorAddress"],
        "supplierAddress2": ["VendorAddress2"],
        "supplierAddress3": ["VendorAddress3"],
        "buyerName": ["Recipient"],
        "buyerAddress": ["RecipientAddress"],
        "storeLocation": ["PremiseAddress"],
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

    table_field_mapping = {
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

    for file_key in result_json_list:
        try:
            # logger.info(f'FILE KEY: {file_key}')
            response = S3_CLIENT.get_object(Bucket=BDA_PROCESSING_BUCKET, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            # logger.info(f'DATA: {data}')

            explainability_info_list = data.get('explainability_info', [])
            
            # Initialize the row and extracted_data dictionary
            row = {}
            extracted_data = {}  # This will hold the raw data before mapping
            row['boundingBoxes'] = {}  # Initialize boundingBoxes as an empty dict
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
            logger.info(f'SERVICE TABLE DATA: {service_table_data}')
            
            # Now process the extracted data according to field_mapping
            for column, possible_keys in field_mapping.items():
                value = ""
                row['boundingBoxes'][column] = []
                
                for key in possible_keys:
                    if key in extracted_data:
                        field_info = extracted_data[key]
                        # Extract just the value
                        value = field_info.get('value', '')
                        geometry_data = field_info.get('geometry', [])
                        
                        # Process bounding boxes
                        for geo_data in geometry_data:
                            page = geo_data.get('page', '')
                            bounding_box = geo_data.get('boundingBox', {})
                            
                            # Convert to Decimal for DynamoDB
                            decimal_box = {
                                'width': Decimal(str(bounding_box.get('width', 0))),
                                'height': Decimal(str(bounding_box.get('height', 0))),
                                'left': Decimal(str(bounding_box.get('left', 0))),
                                'top': Decimal(str(bounding_box.get('top', 0))),
                                'page': page  # Add page to each bounding box
                            }
                            
                            # Add bounding box to the appropriate field array
                            row['boundingBoxes'][column].append(decimal_box)
                        
                        break  # Break after finding the first matching key
                
                # Set the actual value for this field
                row[column] = value

            # Process line items
            row["lineItem"] = []
            if service_table_data:
                for service in service_table_data:
                    lineItem = {}
                    lineItem['boundingBoxes'] = {}

                    # To copy supplier name from invoice level to line item
                    lineItem['supplierName'] = row.get('supplierName', '')
                    
                    # Process each line item field based on table_field_mapping
                    for column, possible_keys in table_field_mapping.items():
                        value = ""
                        lineItem['boundingBoxes'][column] = []
                        
                        for key in possible_keys:
                            if key in service:
                                field_info = service[key]
                                # Extract just the value
                                if key == "UOM":
                                    value = field_info.get('value') if field_info.get('value') != "" else "EA"
                                else:
                                    value = field_info.get('value', '')
                                
                                geometry_data = field_info.get('geometry', [])
                                
                                # Process bounding boxes for line items
                                for geo_data in geometry_data:
                                    page = geo_data.get('page', '')
                                    bounding_box = geo_data.get('boundingBox', {})
                                    
                                    decimal_box = {
                                        'width': Decimal(str(bounding_box.get('width', 0))),
                                        'height': Decimal(str(bounding_box.get('height', 0))),
                                        'left': Decimal(str(bounding_box.get('left', 0))),
                                        'top': Decimal(str(bounding_box.get('top', 0))),
                                        'page': page
                                    }
                                    
                                    lineItem['boundingBoxes'][column].append(decimal_box)
                                
                                break
                        
                        # Set the actual value for this line item field
                        lineItem[column] = value

                    lineItem = setBackUpLineItemTotalPrice(lineItem)
                    row["lineItem"].append(lineItem)
                    
            # Extract confidence score from the matched_blueprint section
            confidence_score = 0
            if 'matched_blueprint' in data:
                confidence_score = data.get('matched_blueprint', {}).get('confidence', 0)
            elif 'blueprint_match' in data:
                confidence_score = data.get('blueprint_match', {}).get('confidence', 0)
            elif 'explainability_info' in data:
                # Try to find it in explainability_info if not at top level
                for info in data.get('explainability_info', []):
                    if isinstance(info, dict) and 'matched_blueprint' in info:
                        confidence_score = info.get('matched_blueprint', {}).get('confidence', 0)
                        break

            # Convert to float for consistency
            confidence_score = float(confidence_score)

            # Add confidence score to row data
            row["confidenceScore"] = confidence_score
            
            mappedJsonData.append(row)
        except Exception as e:
            logger.error(f"Error processing file {file_key}: {str(e)}")
            # Continue with next file if there's an error

    logger.info(f'MAPPED JSON DATA: {mappedJsonData}')
    return mappedJsonData

@tracer.capture_method
def setBackUpLineItemTotalPrice(lineItem):
    """Calculate totalPrice from amountExclTax and taxAmount if totalPrice is empty, with subTotal-discount as backup"""
    amount_excl_tax = lineItem.get('amountExclTax', 0)
    tax_amount = lineItem.get('taxAmount', 0)
    sub_total_amount = lineItem.get('subTotal', 0)
    discount_amount = lineItem.get('discountAmount', 0)
    
    if not lineItem.get('totalPrice') or lineItem.get('totalPrice') == '':
        # First try: amountExclTax + taxAmount (if both have values > 0)
        excl_tax_float = float(amount_excl_tax) if amount_excl_tax else 0
        tax_amount_float = float(tax_amount) if tax_amount else 0
        
        if excl_tax_float > 0 or tax_amount_float > 0:
            logger.warning('Calculating totalPrice from amountExclTax and taxAmount')
            logger.info(f'Amount Excl Tax: {amount_excl_tax}, Tax Amount: {tax_amount}')
            
            calculated_total = excl_tax_float + tax_amount_float
            lineItem['totalPrice'] = str(calculated_total)
            
            lineItem['boundingBoxes']['totalPrice'] = (
                lineItem['boundingBoxes'].get('amountExclTax', []) + 
                lineItem['boundingBoxes'].get('taxAmount', [])
            )
        else:
            # Backup: subTotal - discountAmount (if subTotal has value > 0)
            sub_total_float = float(sub_total_amount) if sub_total_amount else 0
            discount_amount_float = float(discount_amount) if discount_amount else 0
            
            if sub_total_float > 0:
                logger.warning('Calculating totalPrice from subTotal and discountAmount (backup method)')
                logger.info(f'Sub total: {sub_total_amount}, discount amount: {discount_amount}')
                
                calculated_total = sub_total_float - discount_amount_float
                lineItem['totalPrice'] = str(calculated_total)
                
                lineItem['boundingBoxes']['totalPrice'] = (
                    lineItem['boundingBoxes'].get('subTotal', []) + 
                    lineItem['boundingBoxes'].get('discountAmount', [])
                )
    
    return lineItem

@tracer.capture_method
def safe_float(value, default=0):
    """Convert value to float with validation, returning default if conversion fails"""
    if not isinstance(value, (int, float, str)):
        return default
    if not value:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

@tracer.capture_method
def applyMerchantSpecificLogic(invoice, merchant_config):
    """Apply merchant-specific validation and conversion rules"""
    
    # Get flags from merchant config
    custom_logics = merchant_config.get('customLogics', {})
    override_quantity_from_uom = custom_logics.get('overrideQuantityFromUom', False)
    use_customer_ref_as_po = custom_logics.get('useCustomerRefAsPO', False)

    # LOGIC 1: LUCKY FROZEN handling
    if override_quantity_from_uom:
        supplier_name = invoice.get('supplierName', '')
        
        if supplier_name and 'LUCKY FROZEN' in supplier_name.upper():
            invoice['supplierName'] = supplier_name.upper()
            logger.info(f"Applied special rule for supplier: {supplier_name}")
            
            for item in invoice.get('lineItem', []):
                uom = item.get('uom', '')
                
                if uom and (any(char.isdigit() for char in uom)):
                    numeric_part = ''.join(char for char in uom if char.isdigit() or char == '.')
                    
                    if numeric_part:
                        try:
                            numeric_value = float(numeric_part)
                            logger.info(f"Overriding quantity from UOM: {item.get('quantity')} -> {numeric_value}")
                            
                            item['quantity'] = str(numeric_value)
                            
                            alpha_part = ''.join(char for char in uom if char.isalpha())
                            item['uom'] = alpha_part if alpha_part else "EA"
                        except ValueError:
                            logger.warning(f"Failed to extract numeric value from UOM: {uom}")
    
    # LOGIC 2: DKSH Malaysia handling - Use Customer Reference as PO Number
    if use_customer_ref_as_po:
        supplier_name = invoice.get('supplierName', '')
        
        if supplier_name and ('DKSH MALAYSIA' in supplier_name.upper() or 'LUCKY FROZEN' in supplier_name.upper()):
            invoice['supplierName'] = supplier_name.upper()
            customer_reference = None
            
            if 'CustomerReference' in invoice:
                customer_reference = invoice.get('CustomerReference')
            elif 'customerReference' in invoice:
                customer_reference = invoice.get('customerReference')
            
            if customer_reference:
                logger.info(f"Overriding PO number with Customer Reference for DKSH Malaysia: {invoice.get('poNumber')} -> {customer_reference}")
                invoice['poNumber'] = customer_reference
                
    return invoice

@tracer.capture_method
def get_merchant_mapping(merchantId):
    response = MERCHANT_DDB_TABLE.get_item(
        Key={'merchantId': merchantId},
    )
    
    merchant = response['Item']
    supplierMappingPath = merchant.get('supplierMapping')
    itemMappingPath = merchant.get('itemMapping')
    storeMappingPath = merchant.get('storeMapping')

    return supplierMappingPath, itemMappingPath, storeMappingPath

@tracer.capture_method
def performDuplicateChecking(invoice, merchantId):
    # for mappedJson in mappedJsonData:
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
def performMissingFieldChecking(invoice, merchant_config):
    invoiceData = copy.deepcopy(invoice)

    missing_fields = []
    required_fields = getRequiredFields(merchant_config)
    
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
def getRequiredFields(merchant_config):
    # merchant_config contains requiredFields which holds the s3 key to the required fields JSON file
    
    # Default required fields as fallback
    default_required_fields = {
        'invoice': ['invoiceNumber', 'invoiceDate', 'supplierName', 'supplierAddress', 'buyerName', 'buyerAddress', 'poNumber', 'paymentTerms', 'currency', 'totalAmount', 'taxType', 'taxRate', 'taxAmount', 'dueDate'],
        'lineItem': ['description', 'unitPrice', 'uom', 'quantity']
    }
    
    # Get the S3 key for required fields file
    required_fields_path = merchant_config.get('requiredFields')
    
    if not required_fields_path:
        logger.info("No required fields path specified, using default required fields")
        return default_required_fields
    
    try:
        # Fetch custom required fields from S3
        response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=required_fields_path)
        content = response['Body'].read().decode('utf-8')
        custom_required_fields = json.loads(content)
        
        logger.info(f"Using custom required fields from: {required_fields_path}")
        return custom_required_fields
        
    except Exception as e:
        logger.warning(f"Failed to fetch required fields from {required_fields_path}: {str(e)}. Using default required fields.")
        return default_required_fields

@tracer.capture_method
def performStandardization(invoice, merchant_config):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)

    # Get default prompt
    default_prompt = defaultPrompts.STANDARDIZATION_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchant_config.get('promptPaths', {})
    standardization_prompt_path = prompt_paths.get('standardizationPrompt')
    prompt_template = fetch_prompt(standardization_prompt_path, default_prompt)

    prompt = prompt_template.format(invoiceData=json.dumps(invoiceData))
    response, input_tokens, output_tokens = promptBedrock(prompt)
    invoice_json = json.loads(response)
        
    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def createExtractedResultRecord(invoiceData, merchantId, documentUploadId, source_file_name, file_path, now, merchant_config=None):
    extractedDocumentsId = str(uuid.uuid4())
    
    for item in invoiceData.get("lineItem", []):
        # Calculate exception fields for THIS specific item
        exception_fields = parseExceptionFields(
            item.get('exceptionStatus', ''), 
            item, 
            merchant_config,
            invoiceData
        )

        extractedDocumentLineItemPayload = {
            "extractedDocumentsLineItemsId": str(uuid.uuid4()),
            'invoiceNumber': invoiceData.get("invoiceNumber"),
            "itemCode": item.get("itemCode"),
            "description": item.get("description"),
            "unitPrice": item.get("unitPrice"),
            "itemUom": item.get("uom"),
            "quantity": item.get("quantity"),
            "totalPrice": item.get("totalPrice"), # totalPrice is the final field name
            "lineDiscountAmount": item.get("lineDiscountAmount"),
            "comment": item.get("comment"),
            "merchantId": merchantId,
            "extractedDocumentsId": extractedDocumentsId,
            "documentUploadId": documentUploadId,
            'boundingBoxes': item.get('boundingBoxes'),
            "exceptionStatus": item.get('exceptionStatus'),
            "exceptionFields": exception_fields,
            'status': item.get('status'),
            "supplierCode": invoiceData.get("supplierCode"),
            "supplierName": invoiceData.get("supplierName"),
            "currency": invoiceData.get("currency"),
            "purchaserCode": item.get("purchaserCode"),
            "buyerGroup": item.get("buyerGroup"),
            "itemType": item.get("itemType"),
            "createdAt": now,
            "createdBy": "System",
            "updatedAt": now,
            "updatedBy": "System"
        }

        extractedDocumentLineItemPayload = convert_floats_to_decimals(extractedDocumentLineItemPayload)
        EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE.put_item(Item=extractedDocumentLineItemPayload)  

    extractedDocumentPayload = {
        "extractedDocumentsId": extractedDocumentsId,
        "merchantId": merchantId,
        "invoiceNumber": invoiceData.get("invoiceNumber"),
        "invoiceDate": invoiceData.get("invoiceDate"),
        "documentType": "invoice",
        "supplierName": invoiceData.get("supplierName"),
        "supplierAddress": invoiceData.get("supplierAddress"),
        "supplierCode": invoiceData.get("supplierCode"),
        "buyerName": invoiceData.get("buyerName"),
        "buyerAddress": invoiceData.get("buyerAddress"),
        "storeLocation": invoiceData.get("PremiseAddress"),
        "buyerCode": invoiceData.get("buyerCode"),
        "purchaseOrderNo": invoiceData.get("poNumber"),
        "paymentTerms": invoiceData.get("paymentTerms"),
        "currency": invoiceData.get("currency"),
        "totalInvoiceAmount": invoiceData.get("totalAmount"),
        "taxType": invoiceData.get("taxType"),
        "taxRate": invoiceData.get("taxRate", 0),
        "taxAmount": invoiceData.get("taxAmount", 0),
        "dueDate": invoiceData.get("dueDate"),
        "documentStatus": invoiceData.get("status"),
        "storeName": invoiceData.get("storeName"),
        "locationCode": invoiceData.get("locationCode"),
        "dim": invoiceData.get("dim"),
        "expectedReceipt": invoiceData.get("expectedReceipt"),
        "generalComment": invoiceData.get("generalComment"),
        "poExpiry": invoiceData.get("poExpiry"),
        'boundingBoxes': invoiceData.get('boundingBoxes'),
        "exceptionStatus": invoiceData.get('exceptionStatus'),
        "filePath": file_path,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "remarks": "",
        "sourceFile": source_file_name,
        "confidenceScore": round(invoiceData.get("confidenceScore", 0)*100),
        "documentUploadId": documentUploadId
    }

    extractedDocumentPayload = convert_floats_to_decimals(extractedDocumentPayload)
    EXTRACTED_DOCUMENT_DDB_TABLE.put_item(Item=extractedDocumentPayload)
    invoiceData["extractedDocumentsId"] = extractedDocumentsId

    return invoiceData

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
    logger.info(f'DOCUMENT EXCEPTION STATUS CHECK RESULT: {exception_status}')
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
    
    sorted_items = sorted(items, key=lambda x: x.get('updatedAt', ''), reverse=True)
    return sorted_items[0].get('configuration').get('contentChecking')

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
        logger.info(f'SQS MESSAGE SENT: {response}')

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

@tracer.capture_method
def parse_mappings(object_key):
    response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=object_key)
    csv_content = response['Body'].read().decode('utf-8')
    
    df = pd.read_csv(io.StringIO(csv_content))

    return df

@tracer.capture_method
def create_batches(data: pd.DataFrame, batch_size):
    total_rows = len(data)
    num_batches = (total_rows + batch_size - 1) // batch_size  # Ceiling division
    
    batches = []
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        batch = data.iloc[start_idx:end_idx].copy()
        batches.append(batch)
    
    logger.info(f"Created {len(batches)} batches from {total_rows} items")
    return batches

@tracer.capture_method
def format_mapping_database(mapping_batch):
    if 'item code' in mapping_batch.columns:
        # This is an item mapping database
        columns = "vendor code|item code|item description|uom|unit price|item status"
        # columns = "vendor name|vendor code|item name|item code"
    elif 'store code' in mapping_batch.columns:
        # This is a store mapping database
        # columns = "store code|store name|store address|active status"
        columns = "store name|store code|active status"
    else:
        # This is a vendor mapping database
        columns = "vendor code|vendor name|vendor address|active status"
        # columns = "vendor name|vendor code"
        
    rows = []

    for _, row in mapping_batch.iterrows():
        formatted_row = "|".join(str(row.get(col, "")) for col in columns.split("|"))
        rows.append(formatted_row)
    
    return f"Columns: {columns}\n" + "\n".join(rows)

@tracer.capture_method
def fetch_prompt(prompt_path, default_prompt):
    """
    Fetch custom prompt from S3 or return default prompt
    """
    if not prompt_path:
        logger.info("No custom prompt path provided, using default prompt.")
        return default_prompt
    
    try:
        logger.info(f"Fetching custom prompt from: {prompt_path}")
        response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=prompt_path)
        custom_prompt = response['Body'].read().decode('utf-8')
        logger.info(f"Using custom prompt from: {prompt_path}")
        return custom_prompt
    except Exception as e:
        logger.warning(f"Failed to fetch custom prompt from {prompt_path}: {str(e)}. Using default prompt.")
        return default_prompt

@tracer.capture_method
def create_item_mapping_prompt(mapping_batch_str, invoice_items, merchant_config):
    formatted_items = json.dumps(invoice_items)
    default_prompt = defaultPrompts.LINE_ITEM_MASTER_MAPPING_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchant_config.get('promptPaths', {})
    item_mapping_prompt_path = prompt_paths.get('itemMappingPrompt')
    prompt_template = fetch_prompt(item_mapping_prompt_path, default_prompt)
    
    prompt = prompt_template.format(
        database=mapping_batch_str,
        formatted_items=formatted_items
    )

    return prompt

@tracer.capture_method
def create_vendor_mapping_prompt(mapping_batch_str, invoice, merchant_config):
    input_item = {
        "supplierName": invoice.get("supplierName"),
        "supplierAddress": invoice.get("supplierAddress"),
        "supplierAddress2": invoice.get("supplierAddress2", ""),
        "supplierAddress3": invoice.get("supplierAddress3", ""),
    }

    default_prompt = defaultPrompts.VENDOR_MASTER_MAPPING_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchant_config.get('promptPaths', {})
    vendor_mapping_prompt_path = prompt_paths.get('vendorMappingPrompt')
    prompt_template = fetch_prompt(vendor_mapping_prompt_path, default_prompt)

    prompt = prompt_template.format(
        database=mapping_batch_str, 
        input_item=json.dumps(input_item)
    )

    return prompt

@tracer.capture_method
def create_store_mapping_prompt(mapping_batch_str, invoice, merchant_config):
    input_item = {
        "premiseAddress": invoice.get("storeLocation", "")
    }

    default_prompt = defaultPrompts.STORE_MASTER_MAPPING_PROMPT
    
    # Get prompt path from merchant config
    prompt_paths = merchant_config.get('promptPaths', {})
    store_mapping_prompt_path = prompt_paths.get('storeMappingPrompt')
    prompt_template = fetch_prompt(store_mapping_prompt_path, default_prompt)

    prompt = prompt_template.format(
        database=mapping_batch_str, 
        input_item=json.dumps(input_item)
    )

    return prompt

@tracer.capture_method
def performMasterDataChecking(invoice, merchant_config):
    total_input_tokens = 0
    total_output_tokens = 0


    # Get merchant settings from config
    custom_logic = merchant_config.get('customLogics')
    useStoreMapping  = custom_logic.get('useStoreMapping') 
    
    original_confidence_score = invoice.get('confidenceScore')

    # Get mapping paths from config
    mappingPaths  = merchant_config.get('mappingPaths')
    supplierMapping  = mappingPaths.get('supplierMapping')
    itemMapping = mappingPaths.get('itemMapping')
    storeMapping = mappingPaths.get('storeMapping')

    
    supplier_database = parse_mappings(supplierMapping)
    item_database = parse_mappings(itemMapping)

    
    invoice, vendor_input_tokens, vendor_output_tokens = performVendorMasterMapping(invoice, supplier_database, merchant_config)
    logger.info(f'INVOICE AFTER VENDOR MASTER DATA CHECKING: {invoice}')
    
    if useStoreMapping:
        store_database = parse_mappings(storeMapping)
        invoice, store_input_tokens, store_output_tokens = performStoreMasterMapping(invoice, store_database, merchant_config)
        logger.info(f'INVOICE AFTER STORE MASTER DATA CHECKING: {invoice}')
        total_input_tokens += store_input_tokens
        total_output_tokens += store_output_tokens
    else:
        logger.info('Store mapping skipped based on merchant configuration')

    invoice, line_item_input_tokens, line_item_output_tokens = performLineItemMasterMapping(invoice, item_database, merchant_config)
    logger.info(f'INVOICE AFTER LINE ITEM MASTER DATA CHECKING: {invoice}')

    if original_confidence_score is not None:
        invoice['confidenceScore'] = original_confidence_score
        

    total_input_tokens = line_item_input_tokens + vendor_input_tokens
    total_output_tokens = line_item_output_tokens + vendor_output_tokens

    return invoice, total_input_tokens, total_output_tokens

@tracer.capture_method
def performLineItemMasterMapping(invoice, item_database, merchant_config):
    mapped_line_items, input_tokens, output_tokens = process_item_mapping_with_batches(
        invoice, 
        item_database,
        merchant_config
    )
    
    invoice["lineItem"] = mapped_line_items
    
    return invoice, input_tokens, output_tokens

@tracer.capture_method
def process_item_mapping_with_batches(invoice, item_database, merchant_config, max_attempts=1):
    invoice = copy.deepcopy(invoice)
    invoice_items = invoice.get("lineItem", [])
    total_input_tokens = 0
    total_output_tokens = 0
    formatted_items = []
    preserved_fields_map = {}

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
            "supplierName": invoice.get("supplierName"),
            "supplierCode": invoice.get("supplierCode"),
            "description": item.get("description"),
            "unitPrice": item.get("unitPrice"),
            "uom": item.get("uom"),
            "storeLocation": invoice.get("storeLocation"),
            "storeName": invoice.get("storeName"),
            # "quantity": item.get("quantity"),
            # "totalPrice": item.get("totalPrice"),
            "purchaserCode": item.get("purchaserCode"),
            "buyerGroup": item.get("buyerGroup"),
        }
        formatted_items.append(item_payload)

    logger.info(f'FORMATTED ITEMS: {formatted_items}')
    
    mapping_batch_size = min(150, len(item_database))
    mapping_batches = create_batches(item_database, mapping_batch_size)

    all_mapped_items = []
    remaining_items = formatted_items.copy()

    for batch_idx, batch in enumerate(mapping_batches):
        if not remaining_items:
            logger.info("No remaining items")
            break

        logger.info(f"Processing item mapping batch {batch_idx+1}/{len(mapping_batches)}")
        formatted_batch = format_mapping_database(batch)
        
        # Pass merchant_config to get merchant-specific prompt
        prompt = create_item_mapping_prompt(formatted_batch, remaining_items, merchant_config)
        
        batch_result, batch_input_tokens, batch_output_tokens = promptBedrock(prompt)
        item_json_results = json.loads(batch_result)
        logger.info(f'BATCH RESULT: {item_json_results}')
        total_input_tokens += batch_input_tokens
        total_output_tokens += batch_output_tokens

        newly_mapped_ids = set()
        new_remaining_items = []

        for item_result in item_json_results:
            item_id = item_result.get('item_list_id')
            
            if item_result.get('completeMapping') == True:
                all_mapped_items.append(item_result)
                newly_mapped_ids.add(item_id)
            else:
                new_remaining_items.append(item_result)

        remaining_items = new_remaining_items
        
        logger.info(f"After batch {batch_idx+1}: {len(all_mapped_items)} mapped, {len(new_remaining_items)} remaining")
        time.sleep(1)
        
    all_items = all_mapped_items + remaining_items

    for item in all_items:
        item_id = item.pop("item_list_id", None)        
        # Restore preserved fields
        if item_id and item_id in preserved_fields_map:
            preserved_fields = preserved_fields_map[item_id]
            for field_name, field_value in preserved_fields.items():
                if field_value is not None:  # Only restore if the original value was not None
                    item[field_name] = field_value
    
    return all_items, total_input_tokens, total_output_tokens

@tracer.capture_method
def performVendorMasterMapping(invoice, database, merchant_config):
    total_input_tokens = 0
    total_output_tokens = 0

    mapping_batch_size = min(150, len(database))
    mapping_batches = create_batches(database, mapping_batch_size)
    
    mapped_vendor = None
    
    for batch_idx, batch in enumerate(mapping_batches):
        logger.info(f"Processing vendor mapping batch {batch_idx+1}/{len(mapping_batches)}")
        
        formatted_batch = format_mapping_database(batch)
        prompt = create_vendor_mapping_prompt(formatted_batch, invoice, merchant_config)
        
        batch_result, batch_input_tokens, batch_output_tokens = promptBedrock(prompt)
        total_input_tokens += batch_input_tokens
        total_output_tokens += batch_output_tokens
        
        vendor_mapping = json.loads(batch_result)
        mapped_vendor = vendor_mapping

        if vendor_mapping.get('completeMapping') == True:
            logger.info(f"Found vendor match: {mapped_vendor.get('supplierName')}")
            break
    
    # Update invoice with mapped vendor information
    invoice['supplierName'] = mapped_vendor.get("supplierName")
    invoice['supplierAddress'] = mapped_vendor.get("supplierAddress")
    invoice['supplierCode'] = mapped_vendor.get("supplierCode")
    invoice['status'] = mapped_vendor.get("status")
    invoice['exceptionStatus'] = mapped_vendor.get("exceptionStatus")

    return invoice, total_input_tokens, total_output_tokens

@tracer.capture_method
def performStoreMasterMapping(invoice, store_database, merchant_config):
    total_input_tokens = 0
    total_output_tokens = 0
    
    if store_database.empty:
        invoice['locationCode'] = "-"
        invoice['dim'] = "-"
        return invoice, total_input_tokens, total_output_tokens
    
    mapping_batch_size = min(150, len(store_database))
    mapping_batches = create_batches(store_database, mapping_batch_size)
    
    invoice_copy, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)
    
    matched_store = None
    
    for batch_idx, batch in enumerate(mapping_batches):
        logger.info(f"Processing store mapping batch {batch_idx+1}/{len(mapping_batches)}")
        
        formatted_batch = format_mapping_database(batch)
        prompt = create_store_mapping_prompt(formatted_batch, invoice_copy, merchant_config)
        
        batch_result, batch_input_tokens, batch_output_tokens = promptBedrock(prompt)
        total_input_tokens += batch_input_tokens
        total_output_tokens += batch_output_tokens
        
        store_mapping = json.loads(batch_result)
        matched_store = store_mapping
        
        # Break early if we found a complete match
        if store_mapping.get('completeMapping') == True:
            logger.info(f"Found complete store match: {matched_store.get('storeName')}")
            break
    
    # Restore bounding boxes
    invoice = restore_bounding_boxes(invoice, bounding_boxes, line_item_bounding_boxes)
    
    # Update invoice with matched store information
    if matched_store:
        invoice['buyerName'] = matched_store.get("storeName", invoice.get('buyerName', ""))
        invoice['locationCode'] = matched_store.get("locationCode", "-")
        invoice['dim'] = matched_store.get("locationCode", "-")
        
        if matched_store.get('status') == "Exceptions" and invoice.get('status') != "Exceptions":
            invoice['status'] = matched_store.get('status')
            invoice['exceptionStatus'] = matched_store.get("exceptionStatus")
    
    return invoice, total_input_tokens, total_output_tokens

@tracer.capture_method
def performInvoiceToPOConversion(invoice, merchant_config, documentUploadId, now):
    """
    Convert successful invoice records to PO records for merchants with invoiceToPO enabled
    Only processes line items with "Success" status
    """
    try:
        custom_logics = merchant_config.get('customLogics', {})
        use_invoice_to_po = custom_logics.get('invoiceToPO', False)
        merchantId = merchant_config.get('merchantId')
        
        if not use_invoice_to_po:
            logger.info("InvoiceToPO conversion skipped - not enabled for merchant")
            return
        
        # documentStatus = invoice.get("status", "").lower()
        # if documentStatus != "success":
        #     return None

        
        all_line_items = invoice.get("lineItem", [])
        if not all_line_items:
            return None
        
        successful_line_items = []
        for item in all_line_items:
            item_status = item.get("status", "").lower()
            if item_status == "success":
                successful_line_items.append(item)
        
        # Check if we have any successful line items to process
        if not successful_line_items:
            return None
  
        
        # Generate PO number with running sequence
        poNumberGenerated = generate_poNumber(merchantId, now)
        extracted_po_id = str(uuid.uuid4())
        
        # Create EXTRACTED_PO record
        extracted_po_payload = {
            "extractedPoId": extracted_po_id,
            "documentUploadId": documentUploadId,
            "merchantId": merchantId,
            "poNumber": poNumberGenerated,
            "buyerName": invoice.get("buyerName", ""),
            "currency": invoice.get("currency", ""),
            "dim": invoice.get("dim", ""),
            "invoiceDate": invoice.get("invoiceDate", ""),
            "invoiceNumber": invoice.get("invoiceNumber", ""),
            "locationCode": invoice.get("locationCode", ""),
            "remarks": invoice.get("remarks", ""),
            "documentType": "po",
            "supplierName": invoice.get("supplierName", ""),
            "supplierCode": invoice.get("supplierCode", ""),
            "dueDate": invoice.get("dueDate", ""),
            "taxAmount": invoice.get("taxAmount", ""),
            "taxRate": invoice.get("taxRate", ""),
            "taxType": invoice.get("taxType", ""),
            "totalInvoiceAmount": invoice.get("totalAmount", ""),
            "expectedReceipt": invoice.get("expectedReceipt", ""),
            "generalComment": invoice.get("generalComment", ""),
            "poExpiry": invoice.get('poExpiry', ""),
            "updatedAt": now,
            "updatedBy": "System",
            "createdAt": now,
            "createdBy": "System"
        }
        
        extracted_po_payload = convert_floats_to_decimals(extracted_po_payload)
        EXTRACTED_PO_DDB_TABLE.put_item(Item=extracted_po_payload)
        
        line_items_created = 0
        for item in successful_line_items:  
            extracted_po_line_item_payload = {
                "extractedPoLineItemsId": str(uuid.uuid4()),
                "documentUploadId": documentUploadId,
                "extractedDocumentsId": invoice.get("extractedDocumentsId", ""),
                "extractedPoId": extracted_po_id,
                "merchantId": merchantId,
                "poNumber": poNumberGenerated,
                "buyerGroup": item.get("buyerGroup", ""),
                "currency": invoice.get("currency", ""),
                "description": item.get("description", ""),
                "invoiceNumber": invoice.get("invoiceNumber", ""), 
                "itemCode": item.get("itemCode", ""),
                "itemType": item.get("itemType", ""),
                "itemUom": item.get("uom", ""),
                "purchaserCode": item.get("purchaserCode", ""),
                "quantity": item.get("quantity", ""),
                "supplierCode": invoice.get("supplierCode", ""),
                "supplierName": invoice.get("supplierName", ""),
                "totalPrice": item.get("totalPrice", ""),
                "unitPrice": item.get("unitPrice", ""),
                "lineDiscountAmount": item.get("lineDiscountAmount", ""),
                "comment": item.get("comment", ""),
                "updatedAt": now,
                "updatedBy": "System",
                "createdAt": now,
                "createdBy": "System"
            }
            
            extracted_po_line_item_payload = convert_floats_to_decimals(extracted_po_line_item_payload)
            EXTRACTED_PO_LINE_ITEM_DDB_TABLE.put_item(Item=extracted_po_line_item_payload)
            line_items_created += 1
            
        return poNumberGenerated
        
    except Exception as e:
        logger.error(f"Error in invoiceToPO conversion: {str(e)}")
        raise

@tracer.capture_method
def generate_poNumber(merchantId, extraction_date):
    """
    Generate PO number with format: ROBO-DDMMYYYY-0001
    Uses extraction date (createdAt), not invoice date
    Following the same logic as generateRunningNumber from runningNumber.py
    """
    try:
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
        
    except Exception as e:
        logger.error(f"Error generating PO number: {str(e)}")
        # Fallback to timestamp-based number
        timestamp = int(time.time() * 1000)
        return f"ROBO-{datetime.now().strftime('%d%m%Y')}-{timestamp % 10000:04d}"

@tracer.capture_method
def getMerchantConfiguration(merchantId):
    """
    Get merchant configuration once and return structured data
    """
    try:
        response = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchantId})
        merchant = response.get('Item', {})
        
        # Extract all necessary fields
        custom_logics = merchant.get('customLogics', {})
        mappingPrompts = merchant.get('mappingPrompts', {})
        
        merchant_config = {
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
        
        return merchant_config
        
    except Exception as e:
        logger.error(f"Error fetching merchant configuration: {str(e)}")
        # Return default configuration
        return {
            'merchantId': merchantId,

            'customLogics': {
                'overrideQuantityFromUom': False,
                'useCustomerRefAsPO': False,
                'invoiceToPO': False,
                'useStoreMapping': False,
                'enableExceptionFields': False
            },
            'mappingPaths': {
                'supplierMapping': None,
                'itemMapping': None,
                'storeMapping': None
            },
            'promptPaths': {
                'vendorMappingPrompt': None,
                'itemMappingPrompt': None,
                'storeMappingPrompt': None,
                'exceptionCheckingPrompt': None,
                'standardizationPrompt': None
            },
            'requiredFields': {}
        }

@tracer.capture_method
def parseExceptionFields(exceptionStatus, itemData, merchant_config, invoiceData=None):
    """
    Parse exception status and item data to extract missing/problematic fields
    Returns comma-separated human-readable field names
    Only processes if enableExceptionFields is True for the merchant
    """
    # Check if merchant has exception fields enabled
    custom_logics = merchant_config.get('customLogics', {})
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
        if matches:
            # Get the last match which should contain field names
            field_list = matches[-1]
            # Split by comma and clean up
            fields = [field.strip().lower() for field in field_list.split(',')]
            for field in fields:
                # Skip ignored fields
                if field in IGNORED_EXCEPTION_FIELDS:
                    continue
                human_readable = EXCEPTION_FIELD_MAPPING.get(field, field.title())
                if human_readable not in exception_fields:
                    exception_fields.append(human_readable)
    
    # Check individual fields for empty/missing values
    required_fields = ['locationCode', 'invoiceDate', 'invoiceNumber', 'supplierCode']
    for field in required_fields:
        # Skip ignored fields
        field_lower = field.lower()
        if field_lower in IGNORED_EXCEPTION_FIELDS:
            continue
            
        value = invoiceData.get(field, "")
        if not value or str(value).strip() == "" or str(value).strip() == "-":
            human_readable = EXCEPTION_FIELD_MAPPING.get(field_lower, field.title())
            if human_readable not in exception_fields:
                exception_fields.append(human_readable)
    
    # Check line item fields
    required_line_item_fields = ['itemCode', 'quantity', 'itemType', 'purchaserCode', 'buyerGroup']
    for field in required_line_item_fields:
        value = itemData.get(field, "")
        if not value or str(value).strip() == "" or str(value).strip() == "-":
            human_readable = EXCEPTION_FIELD_MAPPING.get(field, field.title())
            if human_readable not in exception_fields:
                exception_fields.append(human_readable)
    
    return ", ".join(exception_fields)

@tracer.capture_method
def getSequenceNumberGenerator(sequenceNumberGeneratorId):
    """Get sequence number generator record"""
    sequenceNumResp = SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.get_item(
        Key={'sequenceNumberGeneratorId': sequenceNumberGeneratorId}
    ).get('Item')
    
    if sequenceNumResp:
        return sequenceNumResp
    else:
        return None

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
def createSequenceNumberGenerator(sequenceNumberGeneratorId, now):
    """Create new sequence number generator record"""
    SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.put_item(Item={
        'sequenceNumberGeneratorId': sequenceNumberGeneratorId,
        'latestValue': '0001',
        'updatedAt': now
    })

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
def restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes):
    invoice_json["boundingBoxes"] = bounding_boxes
    
    # Restore line item bounding boxes
    if "lineItem" in invoice_json:
        for item in invoice_json.get("lineItem", []):
            item_id = item.get('item_list_id')
            if item_id and item_id in line_item_bounding_boxes:
                item.pop('item_list_id', None)
                item["boundingBoxes"] = line_item_bounding_boxes[item_id]
    
    return invoice_json

@tracer.capture_method
def applyInvoiceToPOExceptionLogic(invoice, merchant_config):
    """
    Apply invoice-to-PO specific exception logic:
    - For invoices with exception status and missing locationCode
    - Change all line items from success to exception status
    - Only applies when invoiceToPO is enabled for the merchant
    """
    try:
        # Check if merchant has invoiceToPO enabled
        custom_logics = merchant_config.get('customLogics', {})
        invoice_to_po_enabled = custom_logics.get('invoiceToPO', False)
        
        if not invoice_to_po_enabled:
            logger.info("Invoice-to-PO exception logic skipped - not enabled for merchant")
            return invoice
        
        # Check if invoice has exception status
        invoice_status = invoice.get('status', '').lower()
        if invoice_status != 'exceptions':
            logger.info(f"Invoice-to-PO exception logic skipped - invoice status is '{invoice_status}', not 'exceptions'")
            return invoice
        
        # Check if locationCode is missing or empty
        location_code = invoice.get('locationCode', '')
        if location_code and str(location_code).strip() != '' and str(location_code).strip() != '-':
            logger.info(f"Invoice-to-PO exception logic skipped - locationCode is present: '{location_code}'")
            return invoice
        
        # LocationCode is missing - update all line items from success to exception
        logger.info(f"Applying invoice-to-PO exception logic for missing locationCode in invoice: {invoice.get('invoiceNumber')}")
        
        updated_line_items = []
        items_changed = 0
        
        for item in invoice.get('lineItem', []):
            item_status = item.get('status', '').lower()
            
            if item_status == 'success':
                # Change from success to exception
                item['status'] = 'Exceptions'
                item['exceptionStatus'] = 'Missing location code required for PO conversion'
                items_changed += 1
                logger.info(f"Changed line item '{item.get('description', 'Unknown')}' from Success to Exceptions due to missing locationCode")
            
            updated_line_items.append(item)
        
        invoice['lineItem'] = updated_line_items
        
        if items_changed > 0:
            # Update invoice exception status to reflect the line item changes
            current_exception_status = invoice.get('exceptionStatus', '')
            if 'missing location code' not in current_exception_status.lower():
                if current_exception_status and current_exception_status != 'N/A':
                    invoice['exceptionStatus'] = f"{current_exception_status} and missing location code required for PO conversion"
                else:
                    invoice['exceptionStatus'] = "Missing location code required for PO conversion"
            
            logger.info(f"Updated {items_changed} line items from Success to Exceptions due to missing locationCode")
        
        return invoice
        
    except Exception as e:
        logger.error(f"Error in applyInvoiceToPOExceptionLogic: {str(e)}")
        return invoice