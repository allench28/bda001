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
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
BDA_PROCESSING_BUCKET = os.environ.get('BDA_PROCESSING_BUCKET')
AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
SUPPLIER_TABLE = os.environ.get('SUPPLIER_TABLE')
SUPPLIER_ITEM_TABLE = os.environ.get('SUPPLIER_ITEM_TABLE')

S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs', region_name='us-east-1')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_TABLE)
EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
SUPPLIER_DDB_TABLE = DDB_RESOURCE.Table(SUPPLIER_TABLE)
SUPPLIER_ITEM_DDB_TABLE = DDB_RESOURCE.Table(SUPPLIER_ITEM_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        total_input_tokens = 0
        total_output_tokens = 0

        for record in event.get('Records', []):
            body = json.loads(record.get('body', '{}'))
            
            # Check if this is converse API or BDA format
            if 'extractionResult' in body:
                # Converse API format
                invocation_id = body.get('invocationId')
                extraction_result = body.get('extractionResult')
                source_file_name = body.get('sourceFileName')
                merchantId = body.get('merchantId')
                documentUploadId = body.get('documentUploadId')
                file_path = urllib.parse.unquote(body.get('filePath', ''))
                
                # Process as direct data
                result_data_list = [extraction_result]
                
            else:
                # Traditional BDA format
                invocation_id = body.get('invocation_id')
                result_json_list = body.get('result_json_list', [])
                source_file_name = body.get('source_file_name')
                merchantId = body.get('merchant_id')
                documentUploadId = body.get('document_upload_id')
                file_path = urllib.parse.unquote(body.get('filePath', ''))
                
                # Process as S3 keys
                result_data_list = result_json_list

            if not result_data_list:
                logger.warning(" No data to process, skipping")
                continue

            # Get merchant configuration
            merchant_config = getMerchantConfiguration(merchantId)
            
            updatedMappedJsonData = []
            
            # Handle unsupported document types
            if source_file_name.split('_')[0] == 'po' and 'grn' in source_file_name:
                now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
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
                createExtractedResultRecord(unsupportExtractedDocumentTypePayload, merchantId, documentUploadId, source_file_name, file_path)
                updatedMappedJsonData.append(unsupportExtractedDocumentTypePayload)
                updateFailedDocumentUploadStatus(documentUploadId, "Document Type Unrecognized")
                createTimelineRecord(merchantId, unsupportExtractedDocumentTypePayload)
                continue

            # Process the data
            mappedJsonData = processJsonResult(result_data_list)

            for invoice in mappedJsonData:
                # Perform master data checking
                invoice, input_tokens, output_tokens = performMasterDataChecking(invoice, merchantId)
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                # Perform duplicate checking
                invoice = performDuplicateChecking(invoice, merchantId)

                # Perform standardization
                invoice, input_tokens, output_tokens = performStandardization(invoice, merchant_config)
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                # Perform missing field checking
                invoice = performMissingFieldChecking(invoice, merchant_config)

                # Perform amount checking
                invoice = performAmountChecking(invoice)

                # Perform exception checking
                invoice, input_tokens, output_tokens = performExceptionChecking(invoice)
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                # Create extracted result record
                invoice = createExtractedResultRecord(invoice, merchantId, documentUploadId, source_file_name, file_path)

                # Send to ERP if successful
                if invoice.get("status") == "Success":
                    logger.info(f' Sent successful invoice {invoice.get("invoiceNumber", "Unknown")} to ERP')
                
                # Create timeline record
                createTimelineRecord(merchantId, invoice)
                updatedMappedJsonData.append(invoice)
                
            
            # Update document upload status
            input_tokens, output_tokens = updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData)
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

            logger.info(f' Total token usage - Input: {total_input_tokens}, Output: {total_output_tokens}')
                        
        return {
            "status": True,
            "body": " Data extraction process completed."
        }
    
    except NoCredentialsError:
        logger.error(" AWS credentials not available")
        return {"status": False, "body": "AWS credentials not available"}
    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception(f" Unexpected error: {str(ex)}")
        updateFailedDocumentUploadStatus(documentUploadId, "System Error")
        return {
            "status": True,
            'body': " The server encountered an unexpected condition that prevented it from fulfilling your request."
        }

@tracer.capture_method
def processJsonResult(result_data_list):
    mappedJsonData = []

    field_mapping = {
        "invoiceNumber": ["InvoiceNumber"],
        "invoiceDate": ["InvoiceDate"],
        "supplierName": ["Vendor"],
        "supplierAddress": ["VendorAddress"],
        "buyerName": ["Recipient"],
        "buyerAddress": ["RecipientAddress"],
        "poNumber": ["POnumber"],
        "paymentTerms": ["PaymentTerms"],
        "currency": ["Currency"],
        "totalInvoiceAmount": ["TotalCharge"],
        "taxType": ["TaxType"],
        "taxRate": ["TaxRate"],
        "taxAmount": ["TaxAmount"],
        "dueDate": ["DueDate"],
        "accountNo": ["AccountNumber"],
        "contractNo": ["ContractNo"],
        "leaseId": ["LeaseId"],
        "branchName": ["MallName"],
        "branchLocation": ["LotNo"],
        "billingPeriod": ["BillingPeriod"],
        "invoiceCategory": ["InvoiceCategory"],
    }

    table_field_mapping = {
        "description": ["Description"],
        "unitPrice": ["UnitPrice"],
        "uom": ["UOM"],
        "quantity": ["Quantity"],
        "totalPrice": ["AmountWithoutTax"],
    }

    for data_item_index, data_item in enumerate(result_data_list):
        try:            
            # Check if this is direct converse data or BDA S3 key
            if isinstance(data_item, dict) and 'inference_result' in data_item:
                data = data_item
            else:
                try:
                    response = S3_CLIENT.get_object(Bucket=BDA_PROCESSING_BUCKET, Key=data_item)
                    content = response['Body'].read().decode('utf-8')
                    data = json.loads(content)
                except Exception as s3_error:
                    logger.error(f" Failed to load S3 data: {str(s3_error)}")
                    continue

            explainabilityInfoList = data.get('explainability_info', [])
            
            # Initialize the row and extracted_data dictionary
            row = {}
            extractedData = {}
            row['boundingBoxes'] = {}
            serviceTableData = data.get('inference_result', {}).get('service_table', [])
            
            # Extract data from explainability info or inference_result
            if explainabilityInfoList:
                for explanation_obj in explainabilityInfoList:
                    for key, value in explanation_obj.items():
                        if key != "service_table":
                            extractedData[key] = value
            else:
                # Process inference_result directly (simplified format for converse API)
                inference_result = data.get('inference_result', {})
                for key, value in inference_result.items():
                    if key != 'service_table':
                        # Convert to explainability format for consistency
                        extractedData[key] = {
                            'value': value,
                            'geometry': []
                        }
            
            # Process the extracted data according to field_mapping
            for column, possible_keys in field_mapping.items():
                value = ""
                row['boundingBoxes'][column] = []
                
                for key in possible_keys:
                    if key in extractedData:
                        field_info = extractedData[key]
                        # Extract just the value
                        value = field_info.get('value', '') if isinstance(field_info, dict) else str(field_info)
                        geometry_data = field_info.get('geometry', []) if isinstance(field_info, dict) else []
                        
                        # Process bounding boxes
                        for geo_data in geometry_data:
                            page = normalizePageNumber(geo_data.get('page', 1))
                            boundingBox = geo_data.get('boundingBox', {})
                            
                            # Convert to Decimal for DynamoDB
                            decimal_box = {
                                'width': Decimal(str(boundingBox.get('width', 0))),
                                'height': Decimal(str(boundingBox.get('height', 0))),
                                'left': Decimal(str(boundingBox.get('left', 0))),
                                'top': Decimal(str(boundingBox.get('top', 0))),
                                'page': page
                            }
                            
                            row['boundingBoxes'][column].append(decimal_box)
                        
                        break
                
                row[column] = value

            # Process line items
            row["lineItem"] = []
            if serviceTableData:
                for service_index, service in enumerate(serviceTableData):
                    lineItem = {}
                    lineItem['boundingBoxes'] = {}

                    # Copy supplier name from invoice level to line item
                    lineItem['supplierName'] = row.get('supplierName', '')
                    
                    # Process each line item field based on table_field_mapping
                    for column, possible_keys in table_field_mapping.items():
                        value = ""
                        lineItem['boundingBoxes'][column] = []
                        
                        for key in possible_keys:
                            if key in service:
                                if isinstance(service[key], dict):
                                    # BDA format with field_info structure
                                    field_info = service[key]
                                    value = field_info.get('value', '')
                                    geometry_data = field_info.get('geometry', [])
                                    
                                    # Process bounding boxes for line items
                                    for geo_data in geometry_data:
                                        page = normalizePageNumber(geo_data.get('page', 1))
                                        boundingBox = geo_data.get('boundingBox', {})
                                        
                                        decimal_box = {
                                            'width': Decimal(str(boundingBox.get('width', 0))),
                                            'height': Decimal(str(boundingBox.get('height', 0))),
                                            'left': Decimal(str(boundingBox.get('left', 0))),
                                            'top': Decimal(str(boundingBox.get('top', 0))),
                                            'page': page
                                        }
                                        
                                        lineItem['boundingBoxes'][column].append(decimal_box)
                                else:
                                    # Direct value format (converse API)
                                    value = str(service[key])
                                
                                if key == "UOM" and not value:
                                    value = "EA"
                                
                                break
                        
                        lineItem[column] = value

                    lineItem = setBackUpLineItemTotalPrice(lineItem)
                    row["lineItem"].append(lineItem)
                    
            # Extract confidence score
            confidence_score = 0
            if 'matched_blueprint' in data:
                confidence_score = data.get('matched_blueprint', {}).get('confidence', 0)
            elif 'document_class' in data:
                confidence_score = 0.8  # Default confidence for converse API
            
            row["confidenceScore"] = float(confidence_score)
            
            mappedJsonData.append(row)
            
        except Exception as e:
            logger.error(f" Error processing data item {data_item_index + 1}: {str(e)}")
            continue

    return mappedJsonData

@tracer.capture_method
def performInvoiceCategoryClassification(invoice):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)
    
    prompt = defaultPrompts.INVOICE_CATEGORY_CLASSIFICATION_PROMPT.format(invoiceData=json.dumps(invoiceData))
    response, input_tokens, output_tokens = promptBedrock(prompt)
    invoice_json = json.loads(response)
    
    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def normalizePageNumber(pageNumber):
    """Normalize page number to ensure it's an integer - enhanced version"""
    try:        
        # Handle nested lists and various data types recursively
        max_depth = 10  # Prevent infinite recursion
        depth = 0
        
        while isinstance(pageNumber, list) and depth < max_depth:
            if len(pageNumber) > 0:
                pageNumber = pageNumber[0]
                depth += 1
            else:
                logger.warning(" Empty list found in page number, defaulting to 1")
                return 1
        
        # Convert to integer
        if isinstance(pageNumber, (int, float)):
            result = int(pageNumber)
            return result
        elif isinstance(pageNumber, str):
            if pageNumber.isdigit():
                result = int(pageNumber)
                return result
            else:
                logger.warning(f" Non-numeric string page number: '{pageNumber}', defaulting to 1")
                return 1
        else:
            logger.warning(f" Invalid page number type: {type(pageNumber)}, value: {pageNumber}, defaulting to 1")
            return 1
            
    except Exception as ex:
        logger.warning(f" Error normalizing page number {pageNumber}: {str(ex)}, defaulting to 1")
        return 1

@tracer.capture_method
def performExceptionChecking(invoice):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)

    formatPromptInput = {
        "status": invoiceData.get('status'),
        "mappingException": invoiceData.get('mappingException', "N/A"),
        "missingFieldException": invoiceData.get('missingFieldException', "N/A"),
        "isDuplicate": invoiceData.get('isDuplicate', False),
        "amountException": invoiceData.get('amountException', "N/A"),
        "lineItem": [],
    }

    for item in invoiceData.get('lineItem', []):
        item_data = {
            "itemCode": item.get('itemCode', "-"),
            "status": item.get('status', ""),
            "mappingException": item.get('mappingException', "N/A"),
            "missingFieldException": item.get('missingFieldException', "N/A"),
        }
        formatPromptInput['lineItem'].append(item_data)

    prompt = defaultPrompts.EXCEPTION_STATUS_CHECKING_PROMPT.format(invoiceData=json.dumps(formatPromptInput))
    response, input_tokens, output_tokens = promptBedrock(prompt)

    ## reformat invoice data
    response_json = json.loads(response)
    invoice_json = copy.deepcopy(invoiceData)
    for line_item in invoice_json.get('lineItem', []):
        item_code = line_item.get('itemCode', "-")
        for item in response_json.get('lineItem', []):
            if item.get('itemCode') == item_code:
                line_item['status'] = item.get('status', "")
                line_item['exceptionStatus'] = item.get('exceptionStatus', "N/A")
                break
    
    invoice_json['status'] = response_json.get('status', invoice_json.get('status', "Exceptions"))
    invoice_json['exceptionStatus'] = response_json.get('exceptionStatus', "N/A")

    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def performAmountChecking(invoice):
    
    invoiceData = copy.deepcopy(invoice)
    
    # Calculate sum of line item total prices
    lineItemTotalPrice = sum([safe_float(item.get('totalPrice', 0)) for item in invoiceData.get('lineItem', [])])
    totalInvoiceAmount = safe_float(invoiceData.get('totalInvoiceAmount', 0))
    # totalInvoiceAmount = float(invoiceData.get('totalInvoiceAmount', 0))
    
    # Handle cases where tax is marked as not applicable
    if invoiceData.get('taxType') == "-" and invoiceData.get('taxRate') == "-":
        invoiceData['taxAmount'] = "0"
    
    taxAmount = safe_float(invoiceData.get('taxAmount', 0))
    
    # Check if total invoice amount matches sum of line items (with tolerance for floating-point comparison)
    if abs(totalInvoiceAmount - lineItemTotalPrice) > 0.02:
        # Calculate subtotal by removing tax from total
        subTotal = totalInvoiceAmount - taxAmount
        # Check if subtotal matches line item total
        if abs(subTotal - lineItemTotalPrice) > 0.02:
            invoiceData['amountException'] = "Sum of line items price does not match invoice total price"

        else:
            invoiceData['amountException'] = "N/A"
    else:
        invoiceData['amountException'] = "N/A"

    return invoiceData

@tracer.capture_method
def safe_float(value, default=0):
    """Convert value to float with validation, returning default if conversion fails"""
    if not isinstance(value, (int, float, str)):
        return default
    if not value:
        return default
    try:
        if isinstance(value, str):
            value = value.replace(',', '').strip()
        return float(value)
    except (ValueError, TypeError):
        return default

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
                
                calculated_total = sub_total_float - discount_amount_float
                lineItem['totalPrice'] = str(calculated_total)
                
                lineItem['boundingBoxes']['totalPrice'] = (
                    lineItem['boundingBoxes'].get('subTotal', []) + 
                    lineItem['boundingBoxes'].get('discountAmount', [])
                )
    
    return lineItem

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
        'invoice': ['invoiceNumber', 'invoiceDate', 'supplierName', 'supplierCode', 'totalInvoiceAmount'],
        'lineItem': ['itemCode' ,'description', 'unitPrice', 'quantity', 'totalPrice']
    }
    
    # Get the S3 key for required fields file
    required_fields_path = merchant_config.get('requiredFields')
    
    if not required_fields_path:
        return default_required_fields
    
    try:
        # Fetch custom required fields from S3
        response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=required_fields_path)
        content = response['Body'].read().decode('utf-8')
        custom_required_fields = json.loads(content)
        
        return custom_required_fields
        
    except Exception as e:
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
def createExtractedResultRecord(invoiceData, merchantId, documentUploadId, source_file_name, file_path):
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    mappingPoint = invoiceData.get("mappingPoint")
    if type(mappingPoint) is not dict:
        mappingPoint = None
    extractedDocumentsId = str(uuid.uuid4())
    for item in invoiceData.get("lineItem", []):
        extractedDocumentLineItemPayload = {
            "extractedDocumentsLineItemsId": str(uuid.uuid4()),
            'invoiceNumber': invoiceData.get("invoiceNumber"),
            "itemCode": item.get("itemCode"),
            "description": item.get("description"),
            "unitPrice": item.get("unitPrice"),
            "itemUom": item.get("uom"),
            "quantity": item.get("quantity"),
            "totalPrice": item.get("totalPrice"), # totalPrice is the final field name
            "merchantId": merchantId,
            "extractedDocumentsId": extractedDocumentsId,
            "documentUploadId": documentUploadId,
            'boundingBoxes': item.get('boundingBoxes'),
            "exceptionStatus": item.get('exceptionStatus'),
            'status': item.get('status'),
            "supplierCode": invoiceData.get("supplierCode"),
            "supplierName": invoiceData.get("supplierName"),
            "currency": invoiceData.get("currency"),
            "createdAt": now,
            "createdBy": "System",
            "updatedAt": now,
            "updatedBy": "System",
            "accountName": item.get("accountName", ''),
            "supplierAnalyticAccountCode": invoiceData.get("analyticAccountCode", ''),
        }

        extractedDocumentLineItemPayload = convert_floats_to_decimals(extractedDocumentLineItemPayload)
        EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE.put_item(Item=extractedDocumentLineItemPayload)  

    sstLineItemPayload = {
        "extractedDocumentsLineItemsId": str(uuid.uuid4()),
        'invoiceNumber': invoiceData.get("invoiceNumber"),
        "itemCode": "SST",
        "description": "SST",
        "unitPrice": invoiceData.get("taxAmount"),
        "itemUom": "",
        "quantity": 1,
        "totalPrice": invoiceData.get("taxAmount"),
        "merchantId": merchantId,
        "extractedDocumentsId": extractedDocumentsId,
        "documentUploadId": documentUploadId,
        'boundingBoxes': {},
        "exceptionStatus": "N/A",
        'status': "Success",
        "supplierCode": invoiceData.get("supplierCode"),
        "supplierName": invoiceData.get("supplierName"),
        "currency": invoiceData.get("currency"),
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "accountName": "SST",
        "supplierAnalyticAccountCode": invoiceData.get("analyticAccountCode", ''),
    }
    
    sstLineItemPayload = convert_floats_to_decimals(sstLineItemPayload)
    EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE.put_item(Item=sstLineItemPayload)  

    extractedDocumentPayload = {
        "extractedDocumentsId": extractedDocumentsId,
        "merchantId": merchantId,
        "invoiceNumber": invoiceData.get("invoiceNumber"),
        "invoiceDate": invoiceData.get("invoiceDate"),
        "documentType": "invoice",
        "supplierName": invoiceData.get("supplierName"),
        "erpSupplierName": mappingPoint.get("erpBranchName") if mappingPoint is not None else "",
        "supplierAddress": invoiceData.get("supplierAddress"),
        "supplierCode": invoiceData.get("supplierCode"),
        "buyerName": "Golden Scoop Sdn. Bhd.",
        "buyerAddress": invoiceData.get("buyerAddress"),
        "buyerCode": "-",
        "purchaseOrderNo": invoiceData.get("poNumber"),
        "paymentTerms": invoiceData.get("paymentTerms"),
        "currency": invoiceData.get("currency"),
        "totalInvoiceAmount": invoiceData.get("totalInvoiceAmount"),
        "taxType": invoiceData.get("taxType"),
        "taxRate": invoiceData.get("taxRate"),
        "taxAmount": invoiceData.get("taxAmount"),
        "dueDate": invoiceData.get("dueDate"),
        "supplierAnalyticAccountCode": invoiceData.get("analyticAccountCode"),
        "remarks": invoiceData.get("remarks"),
        "accountNo": invoiceData.get("accountNo"),
        "contractNo": invoiceData.get("contractNo"),
        "leaseId": invoiceData.get("leaseId"),
        "branchName": invoiceData.get("branchName"),
        "branchLocation": invoiceData.get("branchLocation"),
        "documentStatus": invoiceData.get("status"),
        'boundingBoxes': invoiceData.get('boundingBoxes'),
        "exceptionStatus": invoiceData.get('exceptionStatus'),
        "filePath": file_path,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "sourceFile": source_file_name,
        "confidenceScore": round(invoiceData.get("confidenceScore", 0)*100),
        "documentUploadId": documentUploadId
    }

    extractedDocumentPayload = convert_floats_to_decimals(extractedDocumentPayload)
    EXTRACTED_DOCUMENT_DDB_TABLE.put_item(Item=extractedDocumentPayload)
    invoiceData["extractedDocumentsId"] = extractedDocumentsId

    return invoiceData


@tracer.capture_method
def createTimelineRecord(merchantId, invoiceData):
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
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
def updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData):
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
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
def fetch_prompt(prompt_path, default_prompt):
    """
    Fetch custom prompt from S3 or return default prompt
    """
    if not prompt_path:
        return default_prompt
    
    try:
        response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=prompt_path)
        custom_prompt = response['Body'].read().decode('utf-8')
        return custom_prompt
    except Exception as e:
        logger.warning(f" Failed to fetch custom prompt from {prompt_path}: {str(e)}. Using default prompt.")
        return default_prompt


@tracer.capture_method
def performMasterDataChecking(invoice, merchantId):
    total_input_tokens = 0
    total_output_tokens = 0

    ## Save the confidence score before processing
    original_confidence_score = invoice.get('confidenceScore')
    
    invoice, vendor_input_tokens, vendor_output_tokens = performMasterVendorMapping(invoice, merchantId)

    invoice, line_item_input_tokens, line_item_output_tokens = performMasterItemMapping(invoice, merchantId)

    ## Restore the confidence score
    if original_confidence_score is not None:
        invoice['confidenceScore'] = original_confidence_score

    total_input_tokens = line_item_input_tokens + vendor_input_tokens
    total_output_tokens = line_item_output_tokens + vendor_output_tokens

    return invoice, total_input_tokens, total_output_tokens

@tracer.capture_method
def performMasterVendorMapping(invoice, merchantId):
    """
    Main entry point for vendor mapping using three-stage approach:
    1. Shortlist candidates
    2. Refine selection
    3. Choose final mapping
    """
    total_input_tokens = 0
    total_output_tokens = 0
    batch_size = 50
    
    # Stage 1: Initial candidate selection - get database records
    vendor_candidates = stage1_shortlist_vendors(invoice, merchantId, batch_size)

    # if len(vendor_candidates) == 1 then that means we have a direct match
    if len(vendor_candidates) == 1:
        final_result, stage3_tokens = stage3_final_vendor_selection(vendor_candidates, invoice)
        total_input_tokens += stage3_tokens['input']
        total_output_tokens += stage3_tokens['output']

        return final_result, total_input_tokens, total_output_tokens
    
    # Stage 2: Refine candidates - process the shortlisted vendors
    shortlisted_vendors, stage2_tokens = stage2_refine_vendors(vendor_candidates, invoice)
    total_input_tokens += stage2_tokens['input']
    total_output_tokens += stage2_tokens['output']

    # Stage 3: Final selection from remaining candidates
    if shortlisted_vendors:
        final_result, stage3_tokens = stage3_final_vendor_selection(shortlisted_vendors, invoice)
        total_input_tokens += stage3_tokens['input']
        total_output_tokens += stage3_tokens['output']
    else:
        final_result = return_default_vendor(invoice)
    
    return final_result, total_input_tokens, total_output_tokens

@tracer.capture_method
def stage1_shortlist_vendors(invoice, merchantId, batch_size=100):
    targeted_candidates = get_targeted_vendor_candidates(invoice, merchantId)
    
    if targeted_candidates:
        return targeted_candidates
    
    else:
        all_vendors = []
        pagination_key = None
        
        while True:
            query_params = build_ddb_query_params(merchantId, batch_size, pagination_key)
            response = SUPPLIER_DDB_TABLE.query(**query_params)
            batch_items = response.get('Items', [])
            
            if not batch_items:
                break
            
            all_vendors.extend(batch_items)
            
            pagination_key = response.get('LastEvaluatedKey')
            if not pagination_key:
                break
        
        return all_vendors

@tracer.capture_method
def stage2_refine_vendors(vendor_candidates, invoice):
    """
    Stage 2: Process the shortlisted vendor candidates from Stage 1
    Returns the matched vendor or refined candidates
    """
    total_input_tokens = 0
    total_output_tokens = 0
    batch_size = 50
    shortlisted_vendors = []

    if not vendor_candidates:
        return [], {'input': 0, 'output': 0}
    
    # Process vendor candidates in batches
    for i in range(0, len(vendor_candidates), batch_size):

        batch = vendor_candidates[i:i + batch_size]
        batch_result, input_tokens, output_tokens = process_batch(batch, invoice, 'supplier')

        batch_result = [
            vendor for vendor in batch_result
            if (vendor.get('index') and 
                (isinstance(vendor.get('index'), int) and vendor.get('index') < len(batch)) or
                (isinstance(vendor.get('index'), str) and vendor.get('index').isdigit() and int(vendor.get('index')) < len(batch)))
        ] # hallucination check
        
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        
        # Update invoice with partial results for next batch
        shortlisted_vendors.extend(batch_result)
        time.sleep(1)
    
    return shortlisted_vendors, {'input': total_input_tokens, 'output': total_output_tokens}

@tracer.capture_method
def stage3_final_vendor_selection(shortlisted_vendors, invoice):
    """
    Stage 3: Make final vendor selection from refined candidates
    Returns the final matched vendor
    """
    final_mapping_result, input_tokens, output_tokens = process_final_selection_batch(shortlisted_vendors, invoice, 'supplier')

    return final_mapping_result, {'input': input_tokens, 'output': output_tokens}

def return_default_vendor(invoice):
    invoice['completeMapping'] = False
    invoice['status'] = "Exceptions"
    invoice['exceptionStatus'] = "Could not find vendor match"
    invoice['supplierCode'] = "-"
    invoice['analyticAccountCode'] = "-"

    return invoice

@tracer.capture_method
def performMasterItemMapping(invoice, merchantId):
    """
    Main entry point for vendor mapping using three-stage approach:
    1. Shortlist candidates
    2. Refine selection
    3. Choose final mapping
    """
    total_input_tokens = 0
    total_output_tokens = 0
    batch_size = 50
    
    # Stage 1: Initial candidate selection - get database records
    item_candidates = stage1_shortlist_items(invoice, merchantId, batch_size)

    # Stage 2: Refine candidates - process the shortlisted vendors
    shortlisted_items, stage2_tokens = stage2_refine_items(item_candidates, invoice)
    total_input_tokens += stage2_tokens['input']
    total_output_tokens += stage2_tokens['output']

    # Stage 3: Final selection from remaining candidates
    if shortlisted_items:
        final_result, stage3_tokens = stage3_final_item_mapping(shortlisted_items, invoice)
        total_input_tokens += stage3_tokens['input']
        total_output_tokens += stage3_tokens['output']
    else:
        for item in invoice.get("lineItem", []):
            item['completeMapping'] = False
            item['status'] = "Exceptions"
            item['exceptionStatus'] = "Could not find item match"
            item['itemCode'] = "-"
            item['accountName'] = "-"

        final_result = invoice
        
    return final_result, total_input_tokens, total_output_tokens

@tracer.capture_method
def stage1_shortlist_items(invoice, merchantId, batch_size=100):
    all_items = []
    pagination_key = None
    
    while True:
        query_params = build_ddb_query_params(merchantId, batch_size, pagination_key)
        response = SUPPLIER_ITEM_DDB_TABLE.query(**query_params)
        batch_items = response.get('Items', [])
        
        if not batch_items:
            break
        
        all_items.extend(batch_items)
        
        pagination_key = response.get('LastEvaluatedKey')
        if not pagination_key:
            break
    
    return all_items

@tracer.capture_method
def stage2_refine_items(item_candidates, invoice):
    """
    Stage 2: Process the shortlisted vendor candidates from Stage 1
    Returns the matched vendor or refined candidates
    """
    total_input_tokens = 0
    total_output_tokens = 0
    batch_size = 50
    shortlisted_items = []

    if not item_candidates:
        return [], {'input': 0, 'output': 0}
    
    # Process vendor candidates in batches
    for i in range(0, len(item_candidates), batch_size):
        batch = item_candidates[i: i + batch_size]
        batch_result, input_tokens, output_tokens = process_batch(batch, invoice, 'item')

        batch_result = [
            vendor for vendor in batch_result
            if (vendor.get('index') and 
                (isinstance(vendor.get('index'), int) and vendor.get('index') < len(batch)) or
                (isinstance(vendor.get('index'), str) and vendor.get('index').isdigit() and int(vendor.get('index')) < len(batch)))
        ] # hallucination check
        
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        
        # Update invoice with partial results for next batch
        shortlisted_items.extend(batch_result)
        time.sleep(1)
 
    return shortlisted_items, {'input': total_input_tokens, 'output': total_output_tokens}

@tracer.capture_method
def stage3_final_item_mapping(shortlisted_items, invoice):
    """
    Stage 3: Make final vendor selection from refined candidates
    Returns the final matched vendor
    """
    final_mapping_result, input_tokens, output_tokens = process_final_selection_batch(shortlisted_items, invoice, 'item')

    return final_mapping_result, {'input': input_tokens, 'output': output_tokens}

@tracer.capture_method
def process_final_selection_batch(candidates, invoice, mapping_type):
    """
    Process final selection from candidates using FINAL_MAPPING_PROMPT
    """
    formatted_candidates = format_database_from_dynamo(candidates, mapping_type)
    formatted_invoice = copy.deepcopy(invoice)
    line_items = formatted_invoice.pop("lineItem", [])
    
    if mapping_type == 'supplier':
        invoice_bounding_boxes = formatted_invoice.pop("boundingBoxes", {})
        
        prompt = defaultPrompts.FINAL_VENDOR_MAPPING_PROMPT.format(
            database=formatted_candidates,
            invoice=json.dumps(formatted_invoice)
        )

        result, input_tokens, output_tokens = promptBedrock(prompt)
        result_json = json.loads(result)

        result_json['boundingBoxes'] = invoice_bounding_boxes

        if result_json.get('replaceLineItems'):
            new_price = safe_float(invoice.get('totalInvoiceAmount')) - safe_float(invoice.get('taxAmount'))
            line_item = {
                "description": result_json.get('invoiceCategory'),
                "unitPrice": new_price,
                "uom": "",
                "quantity": 1,
                "totalPrice": new_price,
                "boundingBoxes": {}
            }
            result_json['lineItem'] = [line_item]
        else:
            result_json['lineItem'] = line_items

    else: # mapping_type == 'item'        
        formatted_items = []
        bounding_box_map = {}
        
        for index, item in enumerate(line_items):
            item_id = f"item_{index}"
            bounding_box_map[item_id] = item.get("boundingBoxes", {})
            
            item_payload = {
                "item_list_id": item_id,
                "description": item.get("description"),
                "unitPrice": item.get("unitPrice"),
                "uom": item.get("uom"),
                "quantity": item.get("quantity"),
                "totalPrice": item.get("totalPrice")
            }
            formatted_items.append(item_payload)
        
        # Create and execute prompt
        prompt = defaultPrompts.FINAL_ITEM_MAPPING_PROMPT.format(
            database=formatted_candidates,
            formatted_items=json.dumps(formatted_items)
        )

        result, input_tokens, output_tokens = promptBedrock(prompt)
        result_json = json.loads(result)

        for item in result_json:
            item_id = item.pop("item_list_id", None)
            if item_id and item_id in bounding_box_map:
                item["boundingBoxes"] = bounding_box_map[item_id]

        formatted_invoice['lineItem'] = result_json
        result_json = formatted_invoice

    return result_json, input_tokens, output_tokens

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
        logger.error(f" Error fetching merchant configuration for {merchantId}: {str(e)}")
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

@tracer.capture_method(capture_response=False)
def get_vendor_by_id_field(merchantId, field_name, field_value, limit=100):
    if not field_value:
        return [], None
        
    all_items = []
    last_key = None
    excluded_attributes = ['createdAt', 'createdBy', 'udpdatedAt', 'updatedBy', 'merchantId', 'isActive']
    
    # Select the appropriate GSI based on field name
    index_name = f"gsi-merchantId-{field_name}"
    
    while True:
        params = {
            'IndexName': index_name,
            'KeyConditionExpression': Key('merchantId').eq(merchantId) & Key(field_name).eq(field_value),
            'Select': 'ALL_ATTRIBUTES',
            'Limit': limit
        }
        
        if last_key:
            params['ExclusiveStartKey'] = last_key
        
        response = SUPPLIER_DDB_TABLE.query(**params)
        items = response.get('Items', [])
        
        # Clean up items by removing excluded attributes
        for item in items:
            for attr in excluded_attributes:
                if attr in item:
                    del item[attr]
                    
        all_items.extend(items)
        
        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break
    
    return all_items, None

@tracer.capture_method
def get_targeted_vendor_candidates(invoice, merchantId):
    # test = {
    #     'contractNo': invoice.get('contractNo'),
    #     'accountNo': invoice.get('accountNo'),
    #     'leaseId': invoice.get('leaseId'),
    #     'branchName': invoice.get('branchName'),
    #     'branchLocation': invoice.get('branchLocation')
    # }

    
    
    company_database = []
    seen_supplier_ids = set()

    contract_no = invoice.get('contractNo')
    account_no = invoice.get('accountNo')
    lease_id = invoice.get('leaseId')
        
    # If we have any specific criteria, use targeted queries
    has_criteria = any([contract_no, account_no, lease_id])
    
    if has_criteria:
        # Create a list of field names and values to check
        search_combinations = [
            ('contractId', contract_no), 
            ('accountId', contract_no),
            ('contractId', account_no),
            ('accountId', account_no),
            ('contractId', lease_id),
            ('accountId', lease_id)
        ]
        
        # Remove combinations with None values
        search_combinations = [(field, value) for field, value in search_combinations if value]
        
        # Query each combination and collect results
        for field_name, field_value in search_combinations:
            vendor_items, _ = get_vendor_by_id_field(merchantId, field_name, field_value)
            
            # Add unique vendors to the results
            for item in vendor_items:
                supplier_id = item.pop('supplierId', None)
                if supplier_id and supplier_id not in seen_supplier_ids:
                    company_database.append(item)
                    seen_supplier_ids.add(supplier_id)
        
        # Query by branch fields
        # if has_branch_name or has_branch_location:
        #     if has_branch_name and has_branch_location:
        #         vendor_items, _ = get_vendor_by_both_branch_fields(merchantId)
        #     else:
        #         vendor_items, _ = get_vendor_by_branch_fields(merchantId)
            
        #     for item in vendor_items:
                
        #         supplier_id = item.pop('supplierId')
        #         if supplier_id not in seen_supplier_ids:
        #             company_database.append(item)
        #             seen_supplier_ids.add(supplier_id)
    
    return company_database

@tracer.capture_method
def process_batch(batch_items, invoice, table_type):
    formatted_batch = format_database_from_dynamo(batch_items, table_type)
    formatted_invoice = copy.deepcopy(invoice)
    line_items = formatted_invoice.pop("lineItem", [])
    invoice_bounding_boxes = formatted_invoice.pop("boundingBoxes", {})
    
    if table_type == 'supplier':
        formatted_invoice.pop("buyerName") # remove to prevent mapping on this field
        formatted_invoice.pop("buyerAddress") # remove to prevent mapping on this field
        
        prompt = defaultPrompts.VENDOR_MASTER_MAPPING_PROMPT.format(
            database=formatted_batch,
            input_item=json.dumps(formatted_invoice)
        )

        result, input_tokens, output_tokens = promptBedrock(prompt)
        result_json = json.loads(result)
        
    elif table_type == 'item':
        for item in line_items:
            item.pop("boundingBoxes")

        prompt = defaultPrompts.LINE_ITEM_MASTER_MAPPING_PROMPT.format(
            database=formatted_batch,
            formatted_items=json.dumps(line_items)
        )
        
        result, input_tokens, output_tokens = promptBedrock(prompt)
        result_json = json.loads(result)
    
    else:
        raise ValueError(f"Unsupported table_type: {table_type}")
    
    return result_json, input_tokens, output_tokens

def build_ddb_query_params(merchant_id, limit, last_key=None):
    params = {
        'IndexName': 'gsi-merchantId',
        'KeyConditionExpression': Key('merchantId').eq(merchant_id),
        'Limit': limit
    }
    
    if last_key:
        params['ExclusiveStartKey'] = last_key
        
    return params

def format_database_from_dynamo(vendor_items, table_type):
    table_exclude_columns = {
        'supplier': {'merchantId', 'supplierId', 'createdAt', 'createdBy', 'updatedAt', 
                     'updatedBy', 'isActive'},
        'item': {'merchantId', 'supplierItemId', 'createdAt', 'createdBy', 'updatedAt', 
                 'updatedBy'}
    }
    
    exclude_columns = table_exclude_columns.get(table_type)
    
    if not vendor_items:
        return "Columns: No vendor data available"
    
    rows = []
    all_columns = set()
    for item in vendor_items:
        all_columns.update(key for key in item.keys() if key not in exclude_columns)
    
    column_list = sorted(all_columns)
    columns_str = "index|" + "|".join(column_list)   

    for idx, item in enumerate(vendor_items, start=1):
        row_values = [str(idx)]
        for column in column_list:
            value = item.get(column, "")
            
            if isinstance(value, Decimal):
                value = float(value)
            
            value_str = str(value).strip()
            value_str = value_str.replace("|", "/")
            
            row_values.append(value_str)
        
        rows.append("|".join(row_values))
    
    return f"Columns: {columns_str}\n" + "\n".join(rows)

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

