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

AGENT_MAPPING_BUCKET = os.environ.get('AGENT_MAPPING_BUCKET')
DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
EXTRACTED_DOCUMENT_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
EXTRACTED_DOCUMENT_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_DOCUMENT_LINE_ITEM_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
BDA_PROCESSING_BUCKET = os.environ.get('BDA_PROCESSING_BUCKET')
AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
N8N_SQS_QUEUE = os.environ.get('N8N_SQS_QUEUE')
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

            if source_file_name.split('_')[0] != 'invoice':
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
                    "totalInvoiceAmount": 0,
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
                createExtractedResultRecord(unsupportExtractedDocumentTypePayload, merchantId, documentUploadId, source_file_name, file_path, now)
                # updateDocumentUploadStatus(documentUploadId, [unsupportDocumentTypePayload], now)
                updatedMappedJsonData.append(unsupportExtractedDocumentTypePayload)
                updateFailedDocumentUploadStatus(documentUploadId, "Document Format Unrecognized")
                createTimelineRecord(merchantId, unsupportExtractedDocumentTypePayload, now)
                continue

            
            ## Perform initial mapping on the JSON file according to the required fields
            mappedJsonData = processJsonResult(result_json_list)

            for invoice in mappedJsonData:
                # logger.info(f'INVOICE: {invoice}')
                originalInvoiceData = copy.deepcopy(invoice)

                ## Perform invoice category classification
                invoice, input_tokens, output_tokens = performInvoiceCategoryClassification(invoice)
                logger.info(f'INVOICE AFTER INVOICE CATEGORY CLASSIFICATION: {invoice}')
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                ## Perform master data checking
                invoice, input_tokens, output_tokens = performMasterDataChecking(invoice, merchantId)
                logger.info(f'INVOICE AFTER MASTER DATA CHECKING: {invoice}')
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                invoice = performHallucinationCheck(originalInvoiceData, invoice, merchantId)

                # ## Perform standardization
                invoice, input_tokens, output_tokens = performStandardization(invoice, merchantId)
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                logger.info(f'INVOICE AFTER STANDARDIZATION: {invoice}')

                invoice, input_tokens, output_tokens = performMissingFieldChecking(invoice, merchantId)
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                logger.info(f'INVOICE AFTER MISSING FIELD CHECKING: {invoice}')

                # ## Perform duplication checking
                invoice = performDuplicateChecking(invoice, merchantId)
                logger.info(f'INVOICE AFTER DUPLICATE CHECKING: {invoice}')

                invoice, input_tokens, output_tokens = performExceptionChecking(invoice)
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                logger.info(f'INVOICE AFTER EXCEPTION CHECKING: {invoice}')

                ## Create the ExtractedDocument & ExtractedDocumentLineItem table records
                invoice = createExtractedResultRecord(invoice, merchantId, documentUploadId, source_file_name, file_path, now)

                if invoice.get("status") == "Success":
                    send_to_erp_sqs(invoice, merchantId)
                
                ## Create the Timeline table records
                createTimelineRecord(merchantId, invoice, now)

                updatedMappedJsonData.append(invoice)
                
            ## Update the DocumentUpload table status
            # logger.info(f'UPDATED MAPPED JSON: {updatedMappedJsonData}')
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
def performInvoiceCategoryClassification(invoice):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)
    
    prompt = INVOICE_CATEGORY_CLASSIFICATION_PROMPT.format(invoiceData=json.dumps(invoiceData))
    response, input_tokens, output_tokens = promptBedrock(prompt)
    invoice_json = json.loads(response)
    
    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def performHallucinationCheck(originalInvoice, masterMappedInvoice, merchantId):
    supplier_code = masterMappedInvoice.get('supplierCode')

    response = SUPPLIER_DDB_TABLE.query(
        IndexName='gsi-merchantId-supplierCode',
        KeyConditionExpression=Key('merchantId').eq(merchantId) & Key('supplierCode').eq(supplier_code)
    )

    if len(response.get('Items'))==0 and supplier_code != "-":
        logger.info(f"Supplier code {supplier_code} not found in database")
        masterMappedInvoice['supplierCode'] = "-"
        masterMappedInvoice['supplierName'] = originalInvoice.get('supplierName')
        masterMappedInvoice['completeMapping'] = False
        masterMappedInvoice['analyticAccountCode'] = "-"
        masterMappedInvoice['status'] = "Exceptions"
        masterMappedInvoice['exceptionStatus'] = "Vendor not found in database"

    return masterMappedInvoice

@tracer.capture_method
def performExceptionChecking(invoice):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)
    
    for item in invoiceData.get('lineItem', []):
        if item.get('totalPrice') is None or item.get('totalPrice') == "":
            if (item.get('subTotal') is not None and item.get('subTotal') != "") and (item.get('taxAmount') is None or item.get('taxAmount') == ""):
                item['totalPrice'] = safe_float(item.get('subTotal', 0))

    lineItemTotalPrice = sum([safe_float(item.get('totalPrice', 0)) for item in invoiceData.get('lineItem', [])])
    totalInvoiceAmount = safe_float(invoiceData.get('totalInvoiceAmount', 0))
    if invoiceData.get('taxType') == "-" and invoiceData.get('taxRate') == "-":
        invoiceData['taxAmount'] = 0
    
    taxAmount = safe_float(invoiceData.get('taxAmount', 0))
    if abs(totalInvoiceAmount - lineItemTotalPrice) > 0.02:  # Use small tolerance for floating-point comparison
        subTotal = totalInvoiceAmount - taxAmount
        invoiceData['subTotal'] = subTotal
        if abs(subTotal - lineItemTotalPrice) > 0.02:  # Use small tolerance for floating-point comparison
            logger.info(f"Line item total price {lineItemTotalPrice} does not match invoice total price {subTotal}")
            invoiceData['amountException'] = "Sum of line items price does not match invoice total price"
        else:
            invoiceData['amountException'] = "N/A"
    else:
        invoiceData['amountException'] = "N/A"

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

    prompt = EXCEPTION_STATUS_CHECKING_PROMPT.format(invoiceData=json.dumps(formatPromptInput))
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
def performMasterDataChecking(invoice, merchantId):
    total_input_tokens = 0
    total_output_tokens = 0

    ## Save the confidence score before processing
    original_confidence_score = invoice.get('confidenceScore')
    
    invoice, vendor_input_tokens, vendor_output_tokens = performMasterVendorMapping(invoice, merchantId)
    # logger.info(f'INVOICE AFTER MASTER DATA CHECKING: {invoice}')
    # logger.info(f'INVOICE VENDOR MAPPING INPUT TOKENS: {vendor_input_tokens}')
    # logger.info(f'INVOICE VENDOR MAPPING OUTPUT TOKENS: {vendor_output_tokens}')

    invoice, line_item_input_tokens, line_item_output_tokens = performMasterItemMapping(invoice, merchantId)
    # invoice, line_item_input_tokens, line_item_output_tokens = performLineItemMasterMapping(invoice, merchantId)
    # logger.info(f'INVOICE AFTER LINE ITEM MASTER DATA CHECKING: {invoice}')
    # logger.info(f'INVOICE LINE ITEM MAPPING INPUT TOKENS: {line_item_input_tokens}')
    # logger.info(f'INVOICE LINE ITEM MAPPING OUTPUT TOKENS: {line_item_output_tokens}')    

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
    logger.info(f"Stage 1 returned {len(vendor_candidates)} vendor candidates")

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
    logger.info(f"Stage 2 returned {len(shortlisted_vendors)} vendor candidates")
    logger.info(f'Shortlisted Vendors: {shortlisted_vendors}')

    # Stage 3: Final selection from remaining candidates
    if shortlisted_vendors:
        final_result, stage3_tokens = stage3_final_vendor_selection(shortlisted_vendors, invoice)
        total_input_tokens += stage3_tokens['input']
        total_output_tokens += stage3_tokens['output']
    else:
        logger.info("No vendor candidates found in Stage 2")
        final_result = return_default_vendor(invoice)
    
    return final_result, total_input_tokens, total_output_tokens

@tracer.capture_method
def stage1_shortlist_vendors(invoice, merchantId, batch_size=100):
    logger.info("Stage 1: Starting vendor shortlisting")    
    targeted_candidates = get_targeted_vendor_candidates(invoice, merchantId)
    
    if targeted_candidates:
        logger.info(f"Found {len(targeted_candidates)} targeted vendor candidates")
        return targeted_candidates
    
    else:
        logger.info("No targeted candidates found, returning full vendor database")
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
    logger.info("Stage 2: Starting vendor refinement")
    total_input_tokens = 0
    total_output_tokens = 0
    batch_size = 50
    shortlisted_vendors = []

    if not vendor_candidates:
        return [], {'input': 0, 'output': 0}
    
    # Process vendor candidates in batches
    for i in range(0, len(vendor_candidates), batch_size):
        logger.info(f'Processing batch {i}')

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
    
    logger.info("Stage 2 complete")
    return shortlisted_vendors, {'input': total_input_tokens, 'output': total_output_tokens}

@tracer.capture_method
def stage3_final_vendor_selection(shortlisted_vendors, invoice):
    """
    Stage 3: Make final vendor selection from refined candidates
    Returns the final matched vendor
    """
    logger.info("Stage 3: Starting final vendor selection")

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
    logger.info(f"Stage 1 returned {len(item_candidates)} item candidates")

    # Stage 2: Refine candidates - process the shortlisted vendors
    shortlisted_items, stage2_tokens = stage2_refine_items(item_candidates, invoice)
    total_input_tokens += stage2_tokens['input']
    total_output_tokens += stage2_tokens['output']
    logger.info(f"Stage 2 returned {len(shortlisted_items)} item candidates")

    # Stage 3: Final selection from remaining candidates
    if shortlisted_items:
        final_result, stage3_tokens = stage3_final_item_mapping(shortlisted_items, invoice)
        total_input_tokens += stage3_tokens['input']
        total_output_tokens += stage3_tokens['output']
        # logger.info(f'final result: {final_result}')
    else:
        logger.info("No items candidates found in Stage 2")
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
    logger.info("Stage 1: Starting item shortlisting")    

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
    logger.info("Stage 2: Starting item refinement")
    total_input_tokens = 0
    total_output_tokens = 0
    batch_size = 50
    shortlisted_items = []

    if not item_candidates:
        return [], {'input': 0, 'output': 0}
    
    # Process vendor candidates in batches
    for i in range(0, len(item_candidates), batch_size):
        logger.info(f'Processing batch {i}')

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
    
    logger.info("Stage 2 complete")
    return shortlisted_items, {'input': total_input_tokens, 'output': total_output_tokens}

@tracer.capture_method
def stage3_final_item_mapping(shortlisted_items, invoice):
    """
    Stage 3: Make final vendor selection from refined candidates
    Returns the final matched vendor
    """
    logger.info("Stage 3: Starting final item selection")

    final_mapping_result, input_tokens, output_tokens = process_final_selection_batch(shortlisted_items, invoice, 'item')

    return final_mapping_result, {'input': input_tokens, 'output': output_tokens}

@tracer.capture_method
def process_final_selection_batch(candidates, invoice, mapping_type):
    """
    Process final selection from candidates using FINAL_MAPPING_PROMPT
    """
    # logger.info(f'Candidates: {candidates}')
    formatted_candidates = format_database_from_dynamo(candidates, mapping_type)
    formatted_invoice = copy.deepcopy(invoice)
    line_items = formatted_invoice.pop("lineItem", [])
    
    if mapping_type == 'supplier':
        invoice_bounding_boxes = formatted_invoice.pop("boundingBoxes", {})
        
        prompt = FINAL_VENDOR_MAPPING_PROMPT.format(
            database=formatted_candidates,
            invoice=json.dumps(formatted_invoice)
        )

        result, input_tokens, output_tokens = promptBedrock(prompt)
        result_json = json.loads(result)

        result_json['boundingBoxes'] = invoice_bounding_boxes

        if result_json.get('replaceLineItems'):
            line_item = {
                "description": result_json.get('invoiceCategory'),
                "unitPrice": invoice.get('totalInvoiceAmount'),
                "uom": "",
                "quantity": 1,
                "totalPrice": invoice.get('totalInvoiceAmount'),
                "boundingBoxes": {}
            }
            result_json['lineItem'] = [line_item]
        else:
            result_json['lineItem'] = line_items

    else: # mapping_type == 'item'        
        formatted_items = []
        bounding_box_map = {}
        
        # logger.info(f'LINE ITEMS: {line_items}')
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
        prompt = FINAL_ITEM_MAPPING_PROMPT.format(
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

LINE_ITEM_MASTER_MAPPING_PROMPT = """
TASK: Find and return the best matching database records for the given each JSON object item in the input list.

RULES:
- NEVER make up itemCode and accountName. Use "-" if no match found
- Replace " with ' in descriptions

STEP 1: ATTEMPT MAPPING
- attempt to find matching records in database:

  a. DIRECT TEXT MATCHING (for products with models/specs):
     - Requires exact match on:
       * Model numbers (iPad Pro 2016 ≠ iPad Pro 2018)
       * Specifications (256GB ≠ 512GB)
       * Product identifiers
     - Normalize: lowercase, trim spaces, replace hyphens
     - UOM must match exactly (if uom is in mapping_fields)
     - Unit price must match exactly (if unitPrice is in mapping_fields)

  b. SEMANTIC MATCHING (for services/utilities/rentals):
     - Match by concept/category:
       * "Water usage" → "Utility - Water & Sewerage"
       * "Monthly rent" → "Rental - Storage"
       * "MS Office subscription" → "IT Software"
     - Ignore dates, quantities, billing periods
     - UOM can differ if conceptually compatible

STEP 2: SHORTLIST MATCHES
- return up to 3 best matches for each input item
- if no matches found, return why no matches found

<database>
{database}
</database>

Database Format
- Pipe-delimited (|) text format
- First line: `Columns: index|column1|column2|...`
- Data rows: `1|value1|value2|...`
- Escaped pipes: `|` in data appears as `/`
- Parse database by:
  1. Extract column names from first line
  2. For each data row, map values to column names
  3. Convert escaped pipes back: `/` → `|`

<input>
{formatted_items}
</input>

OUTPUT FORMAT:
Return a JSON array containin database objects. Each object should include:
- All fields and exact values from the matching database row. ensure you are not filling in any hallucinated values or creating fake matchReasons.
- Additional metadata about the match, ensure the matchReason is true 

JSON Structure:
[
    {{
        // All database fields from matching row, fill with "" if it was originally null or empty
        "accountCode": "ABC123",
        "accountName": "BR001",
        "itemDescription": "description/item001",
    }},
    {{
        // Other matching records (if any)
        // Same structure as above
    }}
]

No Match Scenario:
If no matches are found, return an empty array:
[]

Single Match Scenario:
If only one match is found, return an array with one object:
[
    {{
        // Single matching database record with metadata
    }}
]

## Important Notes:
- Return ONLY valid JSON - no markdown, no backticks, no explanations
- JSON must be directly parseable by json.loads()
- Return only database records, not the input item
- Empty array if no matches found
- You MUST NOT create new records or modify existing records, return empty array if no matches found
- You MUST NOT make up any values
"""

VENDOR_MASTER_MAPPING_PROMPT = """
Task: Find and return up to 3 best matching database records for the given input JSON object.

<input>
{input_item}
</input>

<database>
{database}
</database>

Database Format
- Pipe-delimited (|) text format
- First line: `Columns: index|column1|column2|...`
- Data rows: `1|value1|value2|...`
- Escaped pipes: `|` in data appears as `/`
- Parse database by:
  1. Extract column names from first line
  2. For each data row, map values to column names
  3. Convert escaped pipes back: `/` → `|`

Matching Steps:
1. **Branch Match**
   - Check: branchName AND branchLocation exist in input?
   - Match: Input branchName must match a database branchName AND input branchLocation must match the corresponding branchLocation from the same database record with at least 90% similarity for EACH field
   - Each field must independently meet the 90% threshold - not as an average
   - Reject matches where only one field matches well but the other doesn't
   - Stop if match found

2. **Supplier Name Match**
   - Check: supplierName exists in input?
   - Match: input supplierName should match up to 90% to the supplierName in database. ONLY on the supplierName field, no other irrelevant fields
   - Stop if match found
   - Continue to next match after check

Validate Match:
Before accepting a match, verify that the matchReason is logically sound and actually exists in the database.
Reject matches if:
a. database record is not found in the database
b. MatchReason shows fuzzy/similarity matching between completely different business entities (e.g., "TNG Digital Sdn Bhd" vs "TT DOT COM")
c. Supplier names are from entirely different industries or business types
d. Match is based solely on weak string similarity without business logic context
e. Having weak match reasons like "starting with the same letter" or "contains similar words"
f. Mapping reasons based on irrelevant fields like buyerName, buyerAddress, invoice category, etc.

Accept matches only if:
a. Supplier names represent the same business entity (accounting for abbreviations, legal suffixes, etc.)
b. Branch/location details align logically with the supplier relationship
c. Fuzzy matching shows clear business relationship (e.g., "ABC Sdn Bhd" vs "ABC SDN BHD" or "ABC Company")

Matching Rules
- Execute steps in priority order
- Record all matches found (do not stop at first match)
- Sort matches by priority (lower step number = higher priority)
- Return maximum 3 best matches
- If no matches found, return empty array
- Do NOT create new records if no matches found

Database Format
- Pipe-delimited (|) text format
- First line: `Columns: index|column1|column2|...`
- Data rows: `1|value1|value2|...`
- Escaped pipes: `|` in data appears as `/`
- Parse database by:
  1. Extract column names from first line
  2. For each data row, map values to column names
  3. Convert escaped pipes back: `/` → `|`

OUTPUT FORMAT:
Return a JSON array containing 0-3 database objects. Each object should include:
- All fields and exact values from the matching database row. ensure you are not filling in any hallucinated values or creating fake matchReasons.
- Additional metadata about the match, ensure the matchReason is true 

JSON Structure:
[
    {{
        // All database fields from matching row, fill with "" if it was originally null or empty
        "outletCode": "ABC123",
        "erpBranchId": "BR001",
        "branchName": "Main Branch",
        "branchLocation": "City Center",
        "supplierName": "AXY Sdn. Bhd.",
        "contractId": "",
        "accountId": "12345",
        // ... all other database columns
        
        // Match metadata
        "_matchStep": 1,
        "_matchReason": "Step 1: accountNo=12345 matched exactly to accountId=12345 from database",
        "_matchStrength": "Very Strong" 
    }},
    {{
        // Second best match (if exists)
    }},
    {{
        // Third best match (if exists)
    }}
]

No Match Scenario:
If no matches are found, return an empty array:
[]

Single Match Scenario:
If only one match is found, return an array with one object:
[
    {{
        // Single matching database record with metadata
    }}
]

## Important Notes:
- Return ONLY valid JSON - no markdown, no backticks, no explanations
- JSON must be directly parseable by json.loads()
- Return only database records, not the input item
- Include match metadata with underscore prefix (_matchStep, _matchReason, _matchStrength)
- Maximum 3 database records in the response
- Empty array if no matches found
- You MUST NOT create new records or modify existing records, return empty array if no matches found
- You MUST NOT make up any values or hallucinate data and make fake matchReasons
- You MUST NOT modify the database records in any way, ensure the supplierCode are not modified
"""

STANDARDIZATION_PROMPT = """
TASK: You are to standardize the input JSON data

<input>
{invoiceData}
</input>

STEP 1: STANDARDIZE FIELDS
fields and their standardization instructions:
    - currency: standardize to the three letter currency code (e.g. MYR, USD, SGD)
    - dates: standardize to YYYY-MM-DD format

STEP 2: DEFAULT VALUES
fields and their default values and instructions:
invoice:
    - taxRate:0
    - taxType: ""
    - remarks: (DDMMYYYY-DDMMYYY format of the billing period of the invoice)/(invoice category)/Outlet Code (supplierCode)
    if billing period is not clear, you can generate a billing period based on the invoice date:
    a. For WATER, ELECT, and SEWERAGE categories:
        i. Start Date: 30 days before invoice date
        ii. End Date: Invoice date
        iii. Example: Invoice date 13/05/2025 → Billing period: 13/04/2025-13/05/2025 → 13042025-13052025
    b. For other categories:
        i. Start Date: 1st of the month after invoice date
        ii. End Date: Last day of the month after invoice date
        iii. Example: Invoice date 13/05/2025 → Billing period: 01/06/2025-30/06/2025 → 01062025-30062025
lineItems:
    - description: if replaceLineItems is true, set description to the generated remarks
    - quanity: 1 (set all line items quantity to 1)
    - unitPrice: totalPrice (set all all line items unit price to the totalPrice of the line item or the other way around)
    - itemUom: ""

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "dueDate": "2025-03-15",
    "currency": "SGD",
    "supplierName": "AXY Sdn. Bhd.",
    "remarks": "01032025-31032025/Invoice Category/Supplier Code",
    ...,
    "lineItem":[...]    
}}

"""

MISSING_FIELD_PROMPT = """
TASK: You are to check the input data for any missing or empty fields based on only the fields in <required_fields> tag.

<input>
{invoiceData}
</input>

<required_fields>
{{
    "invoice": ["invoice number", "invoice date", "supplier name", "supplier code", "currency", "total invoice Amount", "remarks"],
    "lineItem": ["item description", "item code", "account name", "quantity", "unit price", "line item total price"]
}}
</required_fields>

STEP 1: IGNORE NON-REQUIRED FIELDS
    - Ignore any fields that are not in the <required_fields> tag
    - Ignored fields should not affect the status or missingFieldException

STEP 2: CHECK REQUIRED FIELDS
- Ignore the fields that are not in the <required_fields>
- Ignored fields must not affect the status or missingFieldException
- Check if any field in <required_fields> tag is missing or empty from the input data
- If missing fields found:
    a. Ensure the field is actually missing or empty and not in <required_fields>
    b. Set status="Exceptions"
    c. Set each missingFieldException to new human readable exception message that describes the missing fields 

IMPORTANT:
 - Fields not in <required_fields> MUST NOT affect the status or missingFieldException
 - Ignore fields MUST NOT affect the status or missingFieldException

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "supplierName": "AXY Sdn. Bhd.",
    ...
    "status": "Exceptions"/"Success",
    "missingFieldException": human readable message explaining all issues found or N/A,
    "lineItem":[{{
        "item_list_id": "item_0",
        "description": "item001",
        ...,
        "status": "Success" or "Exceptions",
        "missingFieldException": "N/A" or updated error message for the item,
    }}]
}}
"""

DOCUMENT_UPLOAD_STATUS_CHECK_PROMPT = """
TASK: Categorize a list of document exception statuses into a single exception status and provide a high-level status based on priority ranking.

INPUT:
A list of exception statuses from multiple documents in the following format:
["exception status 1", "exception status 2", "exception status 3", ...]

CATEGORIZATION RULES WITH PRIORITY RANKING:
1. Exception Status Determination (in order of priority):
   a. If ANY statuses contain mention of duplicate invoices, set exceptionStatus to "Duplicate Error" (HIGHEST PRIORITY)
   b. If ANY statuses indicate master data mapping failures, set exceptionStatus to "Master Mapping Error" (SECOND PRIORITY)
   c. If ANY statuses indicate total price validation errors, set exceptionStatus to "Amount Error" (THIRD PRIORITY)
   c. If ANY statuses indicate missing fields or data errors, set exceptionStatus to "Missing Field Error" (FORTH PRIORITY)
   d. else set exception status to "N/A" (LOWEST PRIORITY)

2. High-Level Status Determination:
   a. Set status to "Fail" if exception status is "Duplicate Error" or "Master Mapping Error"
   b. Set status to "Pending Review" if exception status is "Missing Field Error" or "Amount Error"
   c. Set status to "Success" if exception status is "N/A"

EXAMPLES:
- Input: ["Duplicate Invoice Number Found", "Missing required field values"]
  Output: {{"exceptionStatus": "Duplicate Error", "status": "Fail"}}

- Input: ["Missing required field values", "Master Data Mapping Failed"]
  Output: {{"exceptionStatus": "Master Mapping Error", "status": "Fail"}}
  
- Input: ["Missing required field values", "N/A"]
  Output: {{"exceptionStatus": "Missing Field Error", "status": "Pending Review"}}

- Input: ["Missing required field values", "No match found for vendor"]
  Output: {{"exceptionStatus": "Master Mapping Error", "status": "Fail"}}
  
- Input: ["None", "N/A"]
  Output: {{"exceptionStatus": "N/A", "status": "Success"}}

<input>
{all_statuses}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
  "exceptionStatus": [one of: "Duplicate Error", "Master Mapping Error", "Missing Field Error", "N/A"],
  "status": [one of: "Fail", "Pending Review", "Success"]
}}


"""

EXCEPTION_STATUS_CHECKING_PROMPT = """
TASK: Analyze invoice data for exceptions and provide a human-readable summary of issues at both line item and document levels.

INPUT DATA:
{invoiceData}

PROCESSING RULES
- Line Item Level Processing For each line item in the invoice:
1. Exception Message Priority (when exceptions exist):
First: Master data mapping errors (vendor/item not found in master files)
Second: Missing required field errors

2. Status Assignment:
a. Set status = "Success" and exceptionStatus = "N/A" if no exceptions found
b. Set status = "Exceptions" if any exceptions exist

3. Message Generation:
a. Combine all identified issues for the individual line item into a single, concise message
b. Use human-readable language
c. Maintain the priority order listed above

- Document Level Processing
1. Exception Priority Order (when exceptions exist):
First: Duplicate detection errors
Second: Master data mapping errors 
Third: Amount errors
Fourth: Missing required field errors
Fifth: Line item exceptions (aggregated from all line items with issues)

2. Status Assignment:
a. Set status = "Success" and exceptionStatus = "N/A" if no exceptions found at both document and line item levels
b. Set status = "Exceptions" if any exceptions exist at either document or line item level

3. Message Generation:
a. Combine ALL identified issues from document level AND line items into a single, comprehensive message
b. Document level exceptions should appear first, followed by line item exceptions
c. For line item exceptions, group similar issues together (e.g., "Line items missing required fields: Item 9013/0200 (unitPrice), Item 9013/0300 (quantity)")
d. Use human-readable language
e. Maintain the priority order listed above

EXAMPLE EXCEPTION MESSAGES
- Document Level Examples
"N/A" (no exceptions at document or line item level)
"Duplicate invoice detected in the system"
"Total price mismatch: line items sum to $1,250.00 but document total is $1,300.00"
"Vendor name and address not found in master files"
"Missing required field values (supplierName, invoiceDate)"
"Duplicate invoice detected, vendor mapping incomplete and total price mismatch ($1,250.00 vs $1,300.00)"
"Vendor mapping incomplete, total price mismatch detected, missing required field values (invoiceDate), and line item issues: Item ABC123 not found in master files, Item DEF456 missing quantity"
"Master data mapping incomplete for vendor, missing required field values (supplierName, totalAmount), and line item exceptions: Items XYZ789, ABC123 not found in master files"

- Line Item Examples
"N/A" (no exceptions)
"Item not found in master files"
"Missing required field values (quantity, unitPrice)"
"Item mapping incomplete and missing required field values (description, quantity)"


OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "status": "Exceptions"/"Success",
    "exceptionStatus": "Comprehensive message explaining ALL issues found at both document and line item levels, or N/A if no exceptions exist anywhere",
    "lineItem":[
        {{
            "itemCode": <original item code from input>,
            "status": "Exceptions"/"Success",
            "exceptionStatus": "Descriptive message explaining all issues found for this specific line item or N/A",
        }}
    ]
}}
"""

INVOICE_CATEGORY_CLASSIFICATION_PROMPT = """
TASK:
You are a expert in invoice classification. Your task is to classify the invoice into one of the following categories:
1. GTO - Gross Turnover: This category is for invoices related to sales commissions, revenue sharing, or percentage-based fees calculated on gross sales amounts. Examples include:
    a. Sales commission charges
    b. Franchise fees based on revenue
    c. Royalty payments calculated as a percentage of sales
    d. Marketing or distribution fees based on turnover
2. RENTAL - This category covers all invoices related to space or equipment rental, including:
    a. Office space or retail space rental
    b. Warehouse or storage facility charges
    c. Equipment rentals
    d. Vehicle leasing
    e. Property management fees
3. ELECT - This refers to electricity charges and related services:
    a. Regular electricity consumption bills
4. WATER - This category includes all water utility related invoices:
    a. Water consumption charges
5. TELEPHONE - This covers telecommunications and related services: 
    a. Fixed line telephone services
    b. Mobile phone charges
    c. Internet services
    d. Voice over IP services
6. SEWERAGE - This includes waste water and sewage management services:
    a. Sewage disposal charges
    b. Waste water treatment
7. LATE PAYMENT INTEREST - This category is for invoices related to late payment fees or interest charges:
    a. Late payment penalties
    b. Interest charges on overdue invoices
    c. Interest advices

8. UNKNOWN - This category is for invoices that do not fit into any of the above categories or are ambiguous in nature

INVOICE:
{invoiceData}

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "supplierName": "AXY Sdn. Bhd.",
    ...,
    "invoiceCategory": classification type,
    "lineItem":[{{
        "item_list_id": "item_0",
        "description": "item001",
        "itemCode": "code001",
        "unitPrice": "10.00",
        ...
    }},
    {{...}}]
}}
"""

FINAL_VENDOR_MAPPING_PROMPT = """
TASK: You are to map the input JSON data to the best matching database record.

1. Find Best Match
Search through the database records to find the best match using this priority order:
Priority 1: Match by contractNo or accountNo
Priority 2: Match by combination of supplierName, branchName, and branchLocation

2. Validate Match
Reject matches if:
a. MatchReason shows fuzzy/similarity matching between completely different business entities (e.g., "SimDarby SK Sdn Bhd" vs "SD COM")
b. Supplier names are from entirely different industries or business types
c. Match is based solely on weak string similarity without business logic context
d. Having stupid match reasons like "starting with the same letter" 

Accept matches only if:
a. Exact contractNo or accountNo match with reasonable supplier alignment
b. Supplier names represent the same business entity (accounting for abbreviations, legal suffixes, etc.)
c. Branch/location details align logically with the supplier relationship
d. Fuzzy matching shows clear business relationship (e.g., "ABC Sdn Bhd" vs "ABC SDN BHD" or "ABC Company")
e. matchStrength is "Strong" or "Very Strong" with a valid matchReason

3. Determine ReplaceLineItems Setting
If a match is found, compare the supplierName against the list of vendors else SKIP this step.
a. Compare the matched record's supplierName with these vendors:
- TENAGA NASIONAL BERHAD
- Air Selangor
- Indah Water Konsortium
- Syarikat Air
- Aliran Tunas Sdn Bhd
- TT DOT COM
- TELEKOM MALAYSIA BERHAD (UNIFI)

b. If the supplierName is the exact same as the vendors stated above:
- Set replaceLineItems = true
- Set replaceLinteItemsReason = why it was set to true
c. check if replaceLineItemsReason is a valid reason and ensure that it meets the requirement of 90% similarity else
- Set replaceLineItems = false

Example: 
matched supplierName is TELEKOM MALAYSIA BERHAD (UNIFI) 
we set replaceLineItems to false because there is no EXACT match in the vendor list


When a valid match is found, set the following from the database record:
- Set supplierCode from database
- Set supplierName from database
- Set analyticAccountCode from database
- Set completeMapping = true
- Set mappingException = "N/A"
- Set status = "Success"
- Set mappingPoint = the matched database record
- Set replaceLineItems = true/false based on step 3

if no match is found:
- do not change supplierName
- Set supplierCode = "-"
- Set analyticAccountCode = "-"
- Set completeMapping = false
- Set mappingException = short descriptive error message about not being able to find a match
- Set status = "Exceptions"
- Set mappingPoint = "N/A"
- Set replaceLineItems = false

<database>
{database}
</database>

<input>
{invoice}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "supplierName": "AXY Sdn. Bhd.",
    "supplierCode": "outletCode001",
    "analyticAccountCode": "erpBranchId001",
    ...
    "status": "Exceptions"/"Success",
    "mappingException": "Descriptive message explaining on why no match was found"/"N/A",
    "mappingPoint": {{
        ...,
        'branchCode': 'outletCode001', 
        'erpBranchId': 'erpBranchId001', 
        'supplierCode': 'outletCode001', 
        'supplierName': 'AXY Sdn. Bhd.',
        ...
    }},
    "completeMapping": true/false,
    "replaceLineItems": true/false,
}}

"""

FINAL_ITEM_MAPPING_PROMPT = """
TASK: for each item in input JSON data, try and match to any records in the  database.

INSTRUCTIONS:
1. go through the list of records and find the best matching record
2. set the itemCode and accountName to the best matching record

when a match is found:
- Set itemCode to accountCode from database
- Set accountName from database
- Set completeMapping = true
- Set mappingException = "N/A"
- Set status = "Success"

if no match is found:
- Set itemCode = "-"
- Set accountName = "-"
- Set completeMapping = false
- Set mappingException = short descriptive error message about not being able to find a match
- Set status = "Exceptions"

<database>
{database}
</database>

<input>
{formatted_items}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
[{{
    "item_list_id": "item_0",
    "description": "item001",
    "accountName": "accountName001",
    "itemCode": "code001",
    "unitPrice": "10.00",
    "uom": "KG",
    "quantity": "2",
    "totalPrice": "20.00",
    "status": "Success" or "Exceptions",
    "mappingException": "N/A" or error message,
    "completeMapping": True/False
}},
...
]

"""

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

    # logger.info(f"Targeted vendor candidates: {test}")
    
    
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
        logger.info('prompting for vendor master mapping')
        formatted_invoice.pop("buyerName") # remove to prevent mapping on this field
        formatted_invoice.pop("buyerAddress") # remove to prevent mapping on this field
        
        prompt = VENDOR_MASTER_MAPPING_PROMPT.format(
            database=formatted_batch,
            input_item=json.dumps(formatted_invoice)
        )

        result, input_tokens, output_tokens = promptBedrock(prompt)
        result_json = json.loads(result)
        # logger.info(f'VENDOR RESULT: {result_json}')
        
    elif table_type == 'item':
        for item in line_items:
            item.pop("boundingBoxes")

        prompt = LINE_ITEM_MASTER_MAPPING_PROMPT.format(
            database=formatted_batch,
            formatted_items=json.dumps(line_items)
        )
        
        result, input_tokens, output_tokens = promptBedrock(prompt)
        result_json = json.loads(result)

        # logger.info(f'ITEM RESULT: {result_json}')
    
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
def processJsonResult(result_json_list):
    mappedJsonData = []

    field_mapping = {
        "invoiceNumber": ["InvoiceNumber"],
        "invoiceDate": ["InvoiceDate"],
        "supplierName": ["Vendor", "VendorName"],
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
    }

    table_field_mapping = {
        "description": ["Description"],
        "unitPrice": ["UnitPrice"],
        "uom": ["UOM"],
        "quantity": ["Quantity"],
        "totalPrice": ["TotalAmountWithTax"],
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
                            
                            # Convert to Decimal for DynamoDB compatibility
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
                if type(service_table_data) is not list and type(service_table_data) is dict:
                    service_table_data = [service_table_data]
                for service in service_table_data:
                    lineItem = {}
                    lineItem['boundingBoxes'] = {}
                    
                    # Process each line item field based on table_field_mapping
                    for column, possible_keys in table_field_mapping.items():
                        value = ""
                        lineItem['boundingBoxes'][column] = []
                        for key in possible_keys:
                            if key in service:
                                field_info = service[key]
                                # Extract just the value

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
            logger.exception({"message": str(e)})
            # Continue with next file if there's an error

    logger.info(f'MAPPED JSON DATA: {mappedJsonData}')
    return mappedJsonData

@tracer.capture_method
def createExtractedResultRecord(invoiceData, merchantId, documentUploadId, source_file_name, file_path, now):
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
def createTimelineRecord(merchantId, invoiceData, now):
    if 'approvalStatus' in invoiceData:
        if invoiceData['approvalStatus'] == "APPROVED":
            title = "approved"
            description = "Invoice approved"
        else:
            title = "rejected"
            description = invoiceData.get('rejectionReason', "Invoice rejected")
    elif invoiceData['status'] == "Success" or invoiceData['status'] == "Exceptions":
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

    prompt = DOCUMENT_UPLOAD_STATUS_CHECK_PROMPT.format(all_statuses=all_statuses)

    exception_status, input_tokens, output_tokens = promptBedrock(prompt)
    exception_status = json.loads(exception_status)
    # logger.info(f'DOCUMENT EXCEPTION STATUS CHECK RESULT: {exception_status}')
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
    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={
            'documentUploadId': documentUploadId,
        },
        UpdateExpression="set #status_attr = :status, exceptionStatus = :exceptionStatus",
        ExpressionAttributeNames={
            '#status_attr': 'status'
        },
        ExpressionAttributeValues={
            ':status': "Failed",
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
def performStandardization(invoice, merchantId):
    invoiceData, bounding_boxes, line_item_bounding_boxes = strip_bounding_boxes(invoice)

    prompt = STANDARDIZATION_PROMPT.format(invoiceData=json.dumps(invoiceData))
    response, input_tokens, output_tokens = promptBedrock(prompt)
    invoice_json = json.loads(response)
        
    invoice_json = restore_bounding_boxes(invoice_json, bounding_boxes, line_item_bounding_boxes)
    
    return invoice_json, input_tokens, output_tokens

@tracer.capture_method
def performMissingFieldChecking(invoice, merchantId):
    invoiceData = copy.deepcopy(invoice)

    required_fields = {
        'invoice': ['invoiceNumber', 'invoiceDate', 'supplierName', 'supplierCode', 'totalInvoiceAmount'],
        'lineItem': ['itemCode' ,'description', 'unitPrice', 'quantity', 'totalPrice']
    }
    missing_fields = []
    
    # Check invoice level fields
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
                logger.info(f'Missing field in line item: {field}: {item.get(field, 'stupid')}')
                item_missing_fields.append(field)
        
        if item_missing_fields:
            item['status'] = "Exceptions"
            item['missingFieldException'] = f"Missing required fields: {', '.join(item_missing_fields)}"
        else:
            item['missingFieldException'] = "N/A"
    
    return invoiceData, 0, 0 


@tracer.capture_method
def performDuplicateChecking(invoice, merchantId):
    # for mappedJson in mappedJsonData:
    invoiceResp = EXTRACTED_DOCUMENT_DDB_TABLE.query(
        IndexName='gsi-merchantId-invoiceNumber',
        KeyConditionExpression=Key('merchantId').eq(merchantId)&Key('invoiceNumber').eq(invoice.get('invoiceNumber')),
        FilterExpression=Attr('documentType').eq("invoice") & Attr('supplierCode').eq(invoice.get('supplierCode')) & Attr('documentStatus').eq("Success"),
    ).get('Items', [])

    if invoiceResp:
        invoice["isDuplicate"] = True
    else:
        invoice["isDuplicate"] = False
    
    return invoice

# @tracer.capture_method(capture_response=False)
# def get_vendor_by_branch_fields(merchantId, limit=100):
#     all_items = []
#     last_key = None

#     excluded_attributes = ['createdAt', 'createdBy', 'udpdatedAt', 'updatedBy', 'merchantId','isActive']
    
#     while True:
#         params = {
#             'IndexName': 'gsi-merchantId',
#             'KeyConditionExpression': Key('merchantId').eq(merchantId),
#             'FilterExpression': Attr('branchName').exists() | Attr('branchLocation').exists(),
#             'Select': 'ALL_ATTRIBUTES',
#             'Limit': limit
#         }
        
#         if last_key:
#             params['ExclusiveStartKey'] = last_key
        
#         response = SUPPLIER_DDB_TABLE.query(**params)
#         items = response.get('Items', [])

#         for item in items:
#             for attr in excluded_attributes:
#                 if attr in item:
#                     del item[attr]

#         all_items.extend(items)
        
#         last_key = response.get('LastEvaluatedKey')
#         if not last_key:
#             break
    
#     return all_items, None

# @tracer.capture_method
# def get_vendor_by_both_branch_fields(merchantId, limit=100):
#     all_items = []
#     last_key = None

#     excluded_attributes = ['createdAt', 'createdBy', 'udpdatedAt', 'updatedBy', 'merchantId','isActive']
    
#     while True:
#         params = {
#             'IndexName': 'gsi-merchantId',
#             'KeyConditionExpression': Key('merchantId').eq(merchantId),
#             'FilterExpression': Attr('branchName').exists() & Attr('branchLocation').exists(),
#             'Select': 'ALL_ATTRIBUTES',
#             'Limit': limit
#         }
        
#         if last_key:
#             params['ExclusiveStartKey'] = last_key
        
#         response = SUPPLIER_DDB_TABLE.query(**params)
#         items = response.get('Items', [])

#         for item in items:
#             for attr in excluded_attributes:
#                 if attr in item:
#                     del item[attr]

#         all_items.extend(items)
        
#         last_key = response.get('LastEvaluatedKey')
#         if not last_key:
#             break
    
#     return all_items, None


@tracer.capture_method
def strip_bounding_boxes(invoice_data):
    """
    Strip bounding boxes from invoice data before sending to prompt.
    
    Args:
        invoice_data: Original invoice data with bounding boxes
        
    Returns:
        tuple: (stripped_data, bounding_boxes, line_item_boxes)
            - stripped_data: Invoice data without bounding boxes
            - bounding_boxes: Top-level bounding boxes
            - line_item_boxes: Dictionary of line item bounding boxes
    """
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
    """
    Restore bounding boxes to invoice data after prompt processing.
    
    Args:
        invoice_json: Processed invoice data from prompt
        bounding_boxes: Top-level bounding boxes to restore
        line_item_bounding_boxes: Line item bounding boxes to restore
        
    Returns:
        dict: Invoice data with bounding boxes restored
    """
    # Restore top-level bounding boxes
    invoice_json["boundingBoxes"] = bounding_boxes
    
    # Restore line item bounding boxes
    if "lineItem" in invoice_json:
        for item in invoice_json.get("lineItem", []):
            item_id = item.get('item_list_id')
            if item_id and item_id in line_item_bounding_boxes:
                item.pop('item_list_id', None)
                item["boundingBoxes"] = line_item_bounding_boxes[item_id]
    
    return invoice_json

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