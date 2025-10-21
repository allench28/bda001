import json
import boto3
import csv
import io
import os
import uuid
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Tuple
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key, Attr
from authorizationHelper import is_authenticated, has_permission, Permission, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException

# Environment variables and clients remain the same...
EXTRACTED_DOCUMENTS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_TABLE')
EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE = os.environ.get('EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE')
EXTRACTED_GRN_TABLE = os.environ.get('EXTRACTED_GRN_TABLE')
EXTRACTED_GRN_LINE_ITEMS_TABLE = os.environ.get('EXTRACTED_GRN_LINE_ITEMS_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
USER_TABLE = os.environ.get('USER_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')
JOB_TRACKING_TABLE = os.environ.get('JOB_TRACKING_TABLE')

S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb')
SQS_CLIENT = boto3.client('sqs')

EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE)
EXTRACTED_GRN_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_TABLE)
EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_LINE_ITEMS_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)
JOB_TRACKING_DDB_TABLE = DDB_RESOURCE.Table(JOB_TRACKING_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # Extract merchantId first and get merchant_config once at the top
        merchantId = None
        merchant_config = {}
        
        # Handle DynamoDB stream events
        if event.get('Records') and 'dynamodb' in event['Records'][0]:
            merchantId = extract_merchant_id_from_event(event)
            if merchantId:
                merchant_config = getMerchantConfiguration(merchantId)
            return processCompletedInvoice(event, merchant_config)
        
        # Handle API Gateway events (manual invocation)
        elif event.get('httpMethod') or event.get('requestContext'):
            try:
                sub, _, _ = is_authenticated(event)
                user = get_user(sub)
                merchantId = user.get('merchantId')
                user_group = get_user_group(user.get('userGroupId')).get('userGroupName')
                has_permission(user_group, Permission.GET_THREE_WAY_MATCHING_RESULTS.value)
                
                # Get merchant_config once here
                merchant_config = getMerchantConfiguration(merchantId)
                
                # Find the most recent PO file for this merchant
                poKey = retrieveCSV(merchantId, 'po', merchant_config)
                if not poKey:
                    return create_response(400, "No PO files found for this merchant")
                
                # Process three-way matching
                result = processThreeWayMatching(merchantId, merchant_config, poKey)
                
                # Return success response
                return create_response(200, "Success", {
                    'status': True,
                    'processed': result.get('processed'),
                    'jobTrackingId': result.get('jobTrackingId')
                })
                
            except (AuthenticationException, AuthorizationException, BadRequestException) as ex:
                return create_response(400, str(ex))
            except Exception as ex:
                logger.exception(f"Error processing API request: {str(ex)}")
                return create_response(500, "Internal server error")
        
        # Handle direct Lambda invocation (test events)
        else:
            merchantId = event.get('merchantId')
            if merchantId:
                merchant_config = getMerchantConfiguration(merchantId)
            return processAllInvoices(event, merchant_config)

    except Exception as e:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(e))
        logger.exception({"message": str(e)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}

@tracer.capture_method
def processCompletedInvoice(event, merchant_config):
    processInvoice = []
    merchantId = None
    oldDeserializedRecord = None
    
    for record in event['Records']:
        # Reset oldDeserializedRecord for each record
        oldDeserializedRecord = None
        
        if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
            deserializedRecord = deserializeDdbRecord(record.get('dynamodb').get('NewImage'))
        elif record['eventName'] == 'REMOVE':
            continue
        
        if record['eventName'] == 'MODIFY':
            oldDeserializedRecord = deserializeDdbRecord(record.get('dynamodb').get('OldImage'))

        # Modified condition to process both INSERT and MODIFY events
        if (deserializedRecord.get('documentStatus') == 'Success' and 
            deserializedRecord.get('exceptionStatus') == 'N/A' and
            (record['eventName'] == 'INSERT' or  # Process INSERT events
             (oldDeserializedRecord and deserializedRecord.get('documentStatus') != oldDeserializedRecord.get('documentStatus')))): 
            processInvoice.append(deserializedRecord)
    
    for invoice in processInvoice:
        invoiceNumber = invoice.get('invoiceNumber')
        merchantId = invoice.get('merchantId')
        poNumber = invoice.get('purchaseOrderNo')  # This is the supplier PO number from invoice
                    
        if merchantId and poNumber:
            invoiceLineItems = getInvoiceLineItems(invoice.get('extractedDocumentsId'))
            invoice['lineItems'] = invoiceLineItems
            invoiceData = normalizeInvoiceData([invoice], merchant_config)[0]
        
            # Process the 3-way matching for all invoices with this merchant and PO
            result = processThreeWayMatching(merchantId, merchant_config, poKey=None, specificInvoices=invoiceData)

        else:
            logger.info(f"Missing merchantId or PO number for invoice {invoiceNumber}. Skipping processing.")
            continue
    
    return {'status': True, 'processed': len(processInvoice), 'merchantId': merchantId, 'poFile': ''}

@tracer.capture_method
def processAllInvoices(event, merchant_config):
    # Handle manual invocation
    merchantId = event.get('merchantId') 
    poKey = event.get('poKey')
    
    if not merchantId or not poKey:
        return {'status': False, 'message': 'Required parameters missing. Please provide merchantId and poKey.'}
    
    # Process the 3-way matching for all invoices with this merchant and PO
    return processThreeWayMatching(merchantId, merchant_config, poKey)

@tracer.capture_method
def retrieveCSV(merchantId, fileType, merchant_config):
    try:
        s3Contents = []
        
        if fileType == "grn":
            prefix = f"grn-csv/{merchantId}/input/"
        elif fileType == "po":
            prefix = f"purchase-order/{merchantId}/input/"
        else:
            return None

        response = S3_CLIENT.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix
        )
        s3Contents.extend(response.get('Contents', []))

        while response.get('IsTruncated', False):
            response = S3_CLIENT.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=prefix,
                ContinuationToken=response['NextContinuationToken']
            )
            s3Contents.extend(response.get('Contents', []))

        # Filter for CSV files
        csvFiles = [obj for obj in s3Contents if obj['Key'].endswith('.csv')]
        if not csvFiles:
            return None
        # Sort by last modified date
        csvFiles.sort(key=lambda x: x['LastModified'], reverse=True)
        # Return the most recent CSV file
        return csvFiles[0]['Key']
    
    except Exception as ex:
        logger.exception(f"Error retrieving CSV: {str(ex)}")
        return None

@tracer.capture_method
def getCellValue(row, column, default=None):
    cell = row[column]
    if not pd.isna(cell):
        return cell
    else:
        return default

@tracer.capture_method
def readGrnCsvFromS3(merchant_config, key: str, filterPoNumber: str = None) -> List[Dict]:
    try:
        custom_logics = merchant_config.get('customLogics', {})
        useDualPoNumbers = custom_logics.get('useDualPoNumbers', False)

        data = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        csvContent = pd.read_csv(data['Body'], dtype=str).to_dict('records')
        mappedCSVContent = []
        grnDataMap = {} 

        # Field mapping remains the same for both dual and single PO scenarios
        field_mapping = {
            'GRN Number': 'grnNumber',
            'GRN Date': 'grnDate',
            'PO Number': 'purchaseOrderNo',  # This will contain Internal PO Number for dual PO merchants
            'Supplier Name': 'supplierName',
            'Supplier ID': 'supplierCode',
            'Item Code': 'itemCode',
            'Description': 'description',
            'Quantity': 'quantity',
            'UOM': 'itemUom',
            'Total Amount': 'totalAmount'
        }

        amount_fields = ['Total Amount']
        number_fields = ['Quantity']

        for record in csvContent:
            record = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            
            mappedRecord = {}
            for key in field_mapping.keys():
                if key in amount_fields:
                    value = getCellValue(record, key)
                    if isinstance(value, str):
                        value = value.replace(',', '')  # Remove commas from the amount
                    mappedRecord[field_mapping.get(key)] = float(value)
                elif key in number_fields:
                    mappedRecord[field_mapping.get(key)] = int(getCellValue(record, key))
                else:
                    mappedRecord[field_mapping.get(key)] = str(getCellValue(record, key))
            
            mappedCSVContent.append(mappedRecord)

        for mappedRecord in mappedCSVContent:
            grnNumber = mappedRecord['grnNumber']
            if grnNumber not in grnDataMap:
                grnDataMap[grnNumber] = {
                    'grnNumber': grnNumber,
                    'grnDate': mappedRecord['grnDate'],
                    'purchaseOrderNo': mappedRecord['purchaseOrderNo'],  # Internal PO Number for dual PO merchants
                    'supplierName': mappedRecord['supplierName'],
                    'supplierCode': mappedRecord['supplierCode'],
                    'lineItems': [],
                }
            
            lineItem = {
                'itemCode': mappedRecord['itemCode'],
                'description': mappedRecord['description'],
                'quantity': mappedRecord['quantity'],
                'itemUom': mappedRecord['itemUom'],
                'totalAmount': mappedRecord['totalAmount']
            }
            grnDataMap[grnNumber]['lineItems'].append(lineItem)
        
        # Calculate the total quantity for each GRN
        for grnNumber, grnData in grnDataMap.items():
            totalQuantity = sum(item['quantity'] for item in grnData['lineItems'])
            totalAmount = sum(item['totalAmount'] for item in grnData['lineItems'])
            grnData['receivedQuantity'] = totalQuantity
            grnData['totalAmount'] = totalAmount

        # Apply PO number filter if specified (filterPoNumber is Internal PO Number for dual PO merchants)
        if filterPoNumber:
            grnDataMap = {k: v for k, v in grnDataMap.items() if v['purchaseOrderNo'] == filterPoNumber}

        # Convert the map to a list
        grnDataList = list(grnDataMap.values())

        return grnDataList

    except Exception as ex:
        logger.exception(f"Error reading GRN CSV from S3: {str(ex)}")
        return []

@tracer.capture_method
def readPurchaseOrderFromS3(merchant_config, key: str, filterPoNumber: str = None) -> List[Dict]:
    try:
        custom_logics = merchant_config.get('customLogics', {})
        useDualPoNumbers = custom_logics.get('useDualPoNumbers', False)

        data = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        csvContent = pd.read_csv(data['Body'], dtype=str).to_dict('records')
        mappedCSVContent = []
        poDataMap = {}
        
        # Merchant-specific field mapping
        if useDualPoNumbers:  # For BWY or similar merchants
            field_mapping = {
                'Supplier PO Number': 'poNumber',  # Used for invoice matching (supplier PO)
                'Internal PO Number': 'internalPoNumber',  # Used for GRN matching
                'PO Date': 'poDate',
                'Supplier Name': 'supplierName',
                'Supplier ID': 'supplierCode',
                'Currency': 'currency',
                'Payment Term': 'paymentTerm',
                'Unit price': 'unitPrice',
                'Total Invoice Amount': 'totalInvoiceAmount',
                'Tax Details (SST/VAT/GST)': 'taxDetails',
                'Tax Amount': 'taxAmount',
                'Item Code': 'itemCode',
                'Description': 'description',
                'Ordered Quantity': 'orderedQuantity',
                'UOM': 'uom'
            }
        else:  # For all other merchants
            field_mapping = {
                'PO Number': 'poNumber',
                'PO Date': 'poDate',
                'Supplier Name': 'supplierName',
                'Supplier ID': 'supplierCode',
                'Currency': 'currency',
                'Payment Term': 'paymentTerm',
                'Unit price': 'unitPrice',
                'Total Invoice Amount': 'totalInvoiceAmount',
                'Tax Details (SST/VAT/GST)': 'taxDetails',
                'Tax Amount': 'taxAmount',
                'Item Code': 'itemCode',
                'Description': 'description',
                'Ordered Quantity': 'orderedQuantity',
                'UOM': 'uom'
            }

        amount_fields = [
            'Unit price',
            'Total Invoice Amount',
            'Tax Amount'
        ]

        number_fields = [
            'Ordered Quantity'
        ]

        for record in csvContent:
            record = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            mappedRecord = {}
            for key in field_mapping.keys():
                if key in amount_fields:
                    value = getCellValue(record, key)
                    if isinstance(value, str):
                        value = value.replace(',', '')  # Remove commas from the amount
                        # Handle special characters for numeric fields
                        if value == '-' or value == '' or value.lower() == 'n/a':
                            mappedRecord[field_mapping.get(key)] = 0.0
                        else:
                            try:
                                mappedRecord[field_mapping.get(key)] = float(value)
                            except ValueError:
                                # Log the error and use default value
                                logger.warning(f"Could not convert '{value}' to float for field {key}, using 0")
                                mappedRecord[field_mapping.get(key)] = 0.0
                    else:
                        mappedRecord[field_mapping.get(key)] = 0.0 if value is None else float(value)
                elif key in number_fields:
                    value = getCellValue(record, key)
                    if value is None or pd.isna(value) or value == '':
                        mappedRecord[field_mapping.get(key)] = 0  # Default to 0 if None
                    else:
                        try:
                            mappedRecord[field_mapping.get(key)] = int(value)
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert '{value}' to int for field {key}, using 0")
                            mappedRecord[field_mapping.get(key)] = 0
                else:
                    mappedRecord[field_mapping.get(key)] = str(getCellValue(record, key))
            mappedCSVContent.append(mappedRecord)

        for mappedRecord in mappedCSVContent:
            poNumber = mappedRecord['poNumber']  # Supplier PO Number for dual PO merchants
            if poNumber not in poDataMap:
                poDataMap[poNumber] = {
                    'poNumber': poNumber,  # Supplier PO Number
                    'poDate': mappedRecord['poDate'],
                    'supplierName': mappedRecord['supplierName'],
                    'supplierCode': mappedRecord['supplierCode'],
                    'currency': mappedRecord['currency'],
                    'paymentTerm': mappedRecord['paymentTerm'],
                    'taxDetails': mappedRecord['taxDetails'],
                    'lineItems': [],
                }
                
                # Add internal PO number for dual PO merchants
                if useDualPoNumbers:
                    poDataMap[poNumber]['internalPoNumber'] = mappedRecord.get('internalPoNumber')
            
            lineItem = {
                'itemCode': mappedRecord['itemCode'],
                'description': mappedRecord['description'],
                'unitPrice': mappedRecord['unitPrice'],
                'orderedQuantity': mappedRecord['orderedQuantity'],
                'uom': mappedRecord['uom'],
                'totalInvoiceAmount': mappedRecord['totalInvoiceAmount'],
                'taxAmount': mappedRecord['taxAmount'],
            }
            poDataMap[poNumber]['lineItems'].append(lineItem)

        for poNumber, poData in poDataMap.items():
            totalAmount = sum(item['totalInvoiceAmount'] for item in poData['lineItems'])
            totalTaxAmount = sum(item['taxAmount'] for item in poData['lineItems'])
            poData['totalAmount'] = totalAmount
            poData['totalTaxAmount'] = totalTaxAmount

        # Apply PO number filter if specified (filterPoNumber is Supplier PO Number)
        if filterPoNumber:
            poDataMap = {k: v for k, v in poDataMap.items() if k == filterPoNumber}

        # Convert the map to a list
        poDataList = list(poDataMap.values())

        return poDataList

    except Exception as ex:
        logger.exception(f"Error reading Purchase Order from S3: {str(ex)}")
        return []

@tracer.capture_method
def getGrnRecords(merchantId, poNumbers, merchant_config):
    grnRecords = []

    grnTableResp = EXTRACTED_GRN_DDB_TABLE.query(
        IndexName='gsi-merchantId-grnNumber',
        KeyConditionExpression=Key('merchantId').eq(merchantId),
        FilterExpression=Attr('documentType').eq('grn')
    )

    grnRecords.extend(grnTableResp.get('Items', []))
    
    while 'LastEvaluatedKey' in grnTableResp:
        grnTableResp = EXTRACTED_GRN_DDB_TABLE.query(
            IndexName='gsi-merchantId-grnNumber',
            KeyConditionExpression=Key('merchantId').eq(merchantId),
            FilterExpression=Attr('documentType').eq('grn'),
            ExclusiveStartKey=grnTableResp['LastEvaluatedKey']
        )
        
        grnRecords.extend(grnTableResp.get('Items', []))

    # Filter GRN records from DDB by PO numbers if provided 
    # For dual PO merchants, poNumbers will contain Internal PO Numbers
    if poNumbers:
        grnRecords = [record for record in grnRecords if record.get('purchaseOrderNo') in poNumbers]

    for record in grnRecords:
        lineItems = getGrnLineItems(record.get('extractedGrnId'))
        record['lineItems'] = lineItems
    
    # normalize grn data to remove unnecessary fields
    filteredGrnRecords = normalizeGrnData(grnRecords, merchant_config)

    return filteredGrnRecords

@tracer.capture_method
def getGrnLineItems(grnId):
    grnLineItems = []

    grnLineItemResp = EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE.query(
        IndexName='gsi-extractedGrnId',
        KeyConditionExpression=Key('extractedGrnId').eq(grnId)
    )

    grnLineItems.extend(grnLineItemResp.get('Items', []))
    
    while 'LastEvaluatedKey' in grnLineItemResp:
        grnLineItemResp = EXTRACTED_GRN_LINE_ITEMS_DDB_TABLE.query(
            IndexName='gsi-extractedGrnId',
            KeyConditionExpression=Key('extractedGrnId').eq(grnId),
            ExclusiveStartKey=grnLineItemResp['LastEvaluatedKey']
        )
        
        grnLineItems.extend(grnLineItemResp.get('Items', []))

    return grnLineItems

@tracer.capture_method
def normalizeGrnData(grnRecords, merchant_config):
    filteredGrnRecords = []
    for record in grnRecords:
        filteredRecord = {
            'grnNumber': record.get('grnNumber'),
            'grnDate': record.get('grnDate'),
            'purchaseOrderNo': record.get('purchaseOrderNo'),  # Internal PO Number for dual PO merchants
            'supplierName': record.get('supplierName'),
            'supplierCode': record.get('supplierCode'),
            'lineItems': []
        }
  
        for lineItem in record.get('lineItems', []):
            filteredLineItem = {
                'itemCode': lineItem.get('itemCode'),
                'description': lineItem.get('description'),
                'quantity': int(lineItem.get('quantity') or 0),  # Convert to int
                'uom': lineItem.get('itemUom'),
                'totalAmount': float(lineItem.get('totalAmount') or 0)  # Convert to float
            }
            
            filteredRecord['lineItems'].append(filteredLineItem)

        filteredGrnRecords.append(filteredRecord)

    for record in filteredGrnRecords:
        totalQuantity = sum(item['quantity'] for item in record['lineItems'])
        totalAmount = sum(item['totalAmount'] for item in record['lineItems'])
        record['receivedQuantity'] = totalQuantity
        record['totalAmount'] = totalAmount

    return filteredGrnRecords

@tracer.capture_method
def getInvoiceRecords(merchantId, merchant_config):
    invoiceRecords = []

    invoiceTableResp = EXTRACTED_DOCUMENTS_DDB_TABLE.query(
        IndexName='gsi-merchantId-invoiceNumber',
        KeyConditionExpression=Key('merchantId').eq(merchantId),
        FilterExpression=Attr('documentStatus').eq('Success')
    )

    invoiceRecords.extend(invoiceTableResp.get('Items', []))
    
    while 'LastEvaluatedKey' in invoiceTableResp:
        invoiceTableResp = EXTRACTED_DOCUMENTS_DDB_TABLE.query(
            IndexName='gsi-merchantId-invoiceNumber',
            KeyConditionExpression=Key('merchantId').eq(merchantId),
            FilterExpression=Attr('documentStatus').eq('Success'),
            ExclusiveStartKey=invoiceTableResp['LastEvaluatedKey']
        )
        
        invoiceRecords.extend(invoiceTableResp.get('Items', []))

    for record in invoiceRecords:
        invoiceLineItems = getInvoiceLineItems(record.get('extractedDocumentsId'))
        record['lineItems'] = invoiceLineItems
        
    # normalize invoice data to remove unnecessary fields
    filteredInvoiceRecords = normalizeInvoiceData(invoiceRecords, merchant_config)
  
    return filteredInvoiceRecords

@tracer.capture_method
def getInvoiceLineItems(invoiceId):
    invoiceLineItems = []

    invoiceLineItemResp = EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.query(
        IndexName='gsi-extractedDocumentsId',
        KeyConditionExpression=Key('extractedDocumentsId').eq(invoiceId)
    )

    invoiceLineItems.extend(invoiceLineItemResp.get('Items', []))
    
    while 'LastEvaluatedKey' in invoiceLineItemResp:
        invoiceLineItemResp = EXTRACTED_DOCUMENTS_LINE_ITEMS_DDB_TABLE.query(
            IndexName='gsi-extractedDocumentsId',
            KeyConditionExpression=Key('extractedDocumentsId').eq(invoiceId),
            ExclusiveStartKey=invoiceLineItemResp['LastEvaluatedKey']
        )
        
        invoiceLineItems.extend(invoiceLineItemResp.get('Items', []))

    return invoiceLineItems

@tracer.capture_method
def normalizeInvoiceData(invoiceRecords, merchant_config):
    filteredInvoiceRecords = []
    for record in invoiceRecords:
        filteredRecord = {
            'invoiceNumber': record.get('invoiceNumber'),
            'invoiceDate': record.get('invoiceDate'),
            'purchaseOrderNo': record.get('purchaseOrderNo'),  # Supplier PO Number for dual PO merchants
            'supplierName': record.get('supplierName'),
            'supplierCode': record.get('supplierCode'),
            'description': record.get('description'),
            'currency': record.get('currency'),
            'taxAmount': float(record.get('taxAmount') or 0),
            'taxCode': record.get('taxCode'),
            'taxType': record.get('taxType'),  
            'taxRate': record.get('taxRate'),
            'lineItems': []
        }

        # PRESERVE ORIGINAL TOTAL INVOICE AMOUNT if available
        if 'totalInvoiceAmount' in record:
            filteredRecord['totalAmount'] = float(record.get('totalInvoiceAmount') or 0)
        
        for lineItem in record.get('lineItems', []):
            filteredLineItem = {
                'itemCode': lineItem.get('itemCode'),
                'description': lineItem.get('description'),
                'quantity': int(lineItem.get('quantity') or 0),
                'uom': lineItem.get('itemUom'),
                'totalPrice': float(lineItem.get('totalPrice') or 0)
            }
            filteredRecord['lineItems'].append(filteredLineItem)

        filteredInvoiceRecords.append(filteredRecord)

    for record in filteredInvoiceRecords:
        # Calculate line items total for information only
        lineItemsTotal = sum(float(item['totalPrice']) for item in record['lineItems'])
        totalQuantity = sum(int(item['quantity']) for item in record['lineItems'])
        taxAmount = record.get('taxAmount', 0)
        
        # ONLY SET totalAmount if NOT already set from original data
        if 'totalAmount' not in record:
            record['totalAmount'] = lineItemsTotal
            
        record['totalQuantity'] = totalQuantity

    return filteredInvoiceRecords

@tracer.capture_method
def processThreeWayMatching(merchantId, merchant_config, poKey=None, specificInvoices=None):
    custom_logics = merchant_config.get('customLogics', {})
    useDualPoNumbers = custom_logics.get('useDualPoNumbers', False)
    
    # Get specific PO number if processing a single invoice
    filterPo = None
    internalPoNumbers = set()  # To store Internal PO Numbers for dual PO merchants
    
    if poKey:
        poFilename = os.path.basename(poKey)
        if specificInvoices:
            filterPo = specificInvoices.get('purchaseOrderNo')  # Supplier PO Number
            # Read filtered PO data
            poDataList = readPurchaseOrderFromS3(merchant_config, poKey, filterPoNumber=filterPo)
        else:
            # Read PO data
            poDataList = readPurchaseOrderFromS3(merchant_config, poKey)
    else:
        poKey = retrieveCSV(merchantId, 'po', merchant_config)
        poFilename = os.path.basename(poKey)
        # Read PO data
        poDataList = readPurchaseOrderFromS3(merchant_config, poKey)
    
    # Extract PO numbers based on merchant configuration
    if useDualPoNumbers:
        # For dual PO merchants: collect Internal PO Numbers for GRN matching
        poNumbers = set()  # Supplier PO Numbers for invoice matching
        for po in poDataList:
            if po.get('poNumber'):
                poNumbers.add(str(po.get('poNumber', '')).strip())
            if po.get('internalPoNumber'):
                internalPoNumbers.add(str(po.get('internalPoNumber', '')).strip())
    else:
        # For single PO merchants: use the same PO numbers for both invoice and GRN matching
        poNumbers = set(str(po.get('poNumber', '')).strip()
                       for po in poDataList if po.get('poNumber'))
        internalPoNumbers = poNumbers  # Same as supplier PO numbers
    
    # Check if a GRN CSV file exists in S3 for this merchant
    grnKey = retrieveCSV(merchantId, 'grn', merchant_config)
    
    # Fetch GRN data from the appropriate source
    if grnKey:
        if useDualPoNumbers and filterPo:
            # For dual PO merchants, find the corresponding Internal PO Number
            filterInternalPo = None
            for po in poDataList:
                if po.get('poNumber') == filterPo:
                    filterInternalPo = po.get('internalPoNumber')
                    break
            grnDataList = readGrnCsvFromS3(merchant_config, grnKey, filterPoNumber=filterInternalPo)
        else:
            grnDataList = readGrnCsvFromS3(merchant_config, grnKey, filterPoNumber=filterPo)
    else:
        # Use Internal PO Numbers for GRN matching
        grnPoNumbers = internalPoNumbers if not filterPo else None
        grnDataList = getGrnRecords(merchantId, poNumbers=grnPoNumbers, merchant_config=merchant_config)
    
    # Get invoices if not provided
    if specificInvoices:
        invoiceDataList = [specificInvoices]
    else:
        invoiceDataList = getInvoiceRecords(merchantId, merchant_config)
    
    # Create a dictionary keyed by 'invoice_number' for faster lookups
    invoiceDataDict = {str(inv.get('invoiceNumber', '')).strip(): inv for inv in invoiceDataList}
    
    # Perform matching
    matchResults = threeWayMatch(poDataList, grnDataList, invoiceDataList, merchant_config)
    
    # Create job tracking record
    jobTrackingId = createJobTracking(merchantId, len(matchResults))
    
    # Send filtered data and results to SQS for analysis
    processMatchingResults(merchantId, matchResults, poDataList, grnDataList, 
                          invoiceDataDict, poKey, poFilename, grnKey, jobTrackingId)
    
    return {
        'status': True, 
        'processed': len(matchResults), 
        'merchantId': merchantId, 
        'poFile': poFilename,
        'jobTrackingId': jobTrackingId
    }

@tracer.capture_method
def threeWayMatch(poDataList, grnDataList, invoiceDataList, merchant_config):
    """
    Perform three-way matching between PO, GRN, and Invoice data arrays
    with enhanced matchResult structure and dual PO number support.
    """
    results = []
    custom_logics = merchant_config.get('customLogics', {})
    useDualPoNumbers = custom_logics.get('useDualPoNumbers', False)

    # Group PO data by Supplier PO number (poNumber) for invoice matching
    poMap = {}
    for po in poDataList:
        poNumber = po.get("poNumber", "")  # Supplier PO Number
        if poNumber:
            if poNumber not in poMap:
                poMap[poNumber] = []
            poMap[poNumber].append(po)
    
    # Group GRN data by PO reference
    grnMap = {}
    for grn in grnDataList:
        poReference = grn.get("purchaseOrderNo", "")  # Internal PO Number for dual PO merchants
        if poReference:
            if poReference not in grnMap:
                grnMap[poReference] = []
            grnMap[poReference].append(grn)

    # Process each invoice
    for invoice in invoiceDataList:
        invoiceNumber = invoice.get("invoiceNumber")
        supplierPoReference = None
        
        # Look for Supplier PO reference in invoice
        if invoice.get('purchaseOrderNo'):
            supplierPoReference = invoice['purchaseOrderNo']  # Supplier PO Number
        
        # Include invoices without PO reference in results 
        if not supplierPoReference:
            results.append({
                'invoiceNumber': invoiceNumber,
                'poNumber': supplierPoReference,
                'status': 'Missing PO Reference',
                'mismatchDetails': {
                    'poMatch': {
                        'error': 'No PO reference found in invoice',
                        'expected': 'Valid PO number',
                        'found': 'No PO reference'
                    }
                }
            })
            continue
            
        supplierPoReference = str(supplierPoReference).strip()
        
        # Find corresponding PO using Supplier PO Number
        matchingPo = poMap.get(supplierPoReference, [])
        
        # For dual PO merchants, find GRN using Internal PO Number
        matchingGrn = []
        if useDualPoNumbers and matchingPo:
            # Get Internal PO Number from the matched PO
            internalPoNumber = matchingPo[0].get('internalPoNumber')
            if internalPoNumber:
                matchingGrn = grnMap.get(internalPoNumber, [])
        else:
            # For single PO merchants, use the same PO number for GRN matching
            matchingGrn = grnMap.get(supplierPoReference, [])

        # Enhanced match result with line item details
        matchResult = {
            'invoiceNumber': invoiceNumber,
            'poNumber': supplierPoReference,  # Supplier PO Number
            'poReference': supplierPoReference,
            'status': 'Ok',
            'matchDetails': {
                'poNumber': supplierPoReference,
                'poMatched': True,
                'grnMatched': True,
                'amountMatched': True,
                'quantityMatched': True,
                'lineItems': []
            }
        }
        
        # Add Internal PO Number information for dual PO merchants
        if useDualPoNumbers and matchingPo:
            matchResult['internalPoNumber'] = matchingPo[0].get('internalPoNumber')
        
        # Include validation results for PO and GRN not found
        if not matchingPo:
            matchResult['status'] = f"PO {supplierPoReference} not found"
            matchResult['mismatchDetails'] = {
                'poMatch': {
                    'error': f"PO {supplierPoReference} not found",
                    'expected': "Valid PO",
                    'found': "No matching PO"
                }
            }
            results.append(matchResult)
            continue
        
        if not matchingGrn:
            if useDualPoNumbers:
                internalPoNumber = matchingPo[0].get('internalPoNumber')
                matchResult['status'] = f"GRN for Internal PO {internalPoNumber} not found"
                matchResult['mismatchDetails'] = {
                    'grnMatch': {
                        'error': f"GRN for Internal PO {internalPoNumber} not found",
                        'expected': "GRN document",
                        'found': "No matching GRN"
                    }
                }
            else:
                matchResult['status'] = f"GRN for PO {supplierPoReference} not found"
                matchResult['mismatchDetails'] = {
                    'grnMatch': {
                        'error': f"GRN for PO {supplierPoReference} not found",
                        'expected': "GRN document",
                        'found': "No matching GRN"
                    }
                }
            results.append(matchResult)
            continue
        
        # Add detailed line item matching
        singleMatchingPo = matchingPo[0]
        for poLine in singleMatchingPo.get('lineItems', []):
            grnQuantity = sum(
                int(gLine.get('quantity', 0)) 
                for grn in matchingGrn 
                for gLine in grn.get('lineItems', []) 
                if gLine.get('itemCode') == poLine.get('itemCode')
            )
            invoiceQuantity = sum(
                int(iLine.get('quantity', 0)) 
                for iLine in invoice.get('lineItems', []) 
                if iLine.get('itemCode') == poLine.get('itemCode')
            )
            lineResult = {
                'itemCode': poLine.get('itemCode'),
                'description': poLine.get('description'),
                'poQuantity': poLine.get('orderedQuantity'),
                'grnQuantity': grnQuantity,
                'invoiceQuantity': invoiceQuantity,
                'status': 'Matched'
            }
            matchResult['matchDetails']['lineItems'].append(lineResult)

        results.append(matchResult)
    
    return results

@tracer.capture_method
def processMatchingResults(merchantId, matchResults, poDataList, grnDataList, invoiceDataDict, poKey, poFilename, grnKey, jobTrackingId):
    custom_logics = getMerchantConfiguration(merchantId).get('customLogics', {})
    useDualPoNumbers = custom_logics.get('useDualPoNumbers', False)
    
    for matchResult in matchResults:
        # Construct message with filtered data relevant to this invoice
        invoiceNumber = matchResult.get('invoiceNumber')
        supplierPoReference = matchResult.get('poReference') or matchResult.get('poNumber')

        # Filter PO data relevant to this invoice (using Supplier PO Number)
        poDataDict = {str(po.get('poNumber', '')).strip(): po for po in poDataList}
        
        if supplierPoReference and supplierPoReference in poDataDict:
            relevantPoData = poDataDict[supplierPoReference]
        else:
            relevantPoData = {}
        
        # Filter GRN data relevant to this invoice
        if useDualPoNumbers and relevantPoData.get('internalPoNumber'):
            # For dual PO merchants, filter GRN by Internal PO Number
            internalPoNumber = relevantPoData.get('internalPoNumber')
            relevantGrnData = [grn for grn in grnDataList if str(grn.get('purchaseOrderNo', '')) == str(internalPoNumber)]
        elif supplierPoReference:
            # For single PO merchants, filter GRN by Supplier PO Number
            relevantGrnData = [grn for grn in grnDataList if str(grn.get('purchaseOrderNo', '')) == str(supplierPoReference)]
        else:
            relevantGrnData = []

        if invoiceNumber in invoiceDataDict:
            relevantInvoiceData = [invoiceDataDict[invoiceNumber]]
        else:
            relevantInvoiceData = []

        # Construct SQS message
        message = {
            'merchantId': merchantId,
            'poData': relevantPoData,
            'grnData': relevantGrnData,
            'invoiceData': relevantInvoiceData,
            'matchResult': matchResult,
            'poKey': poKey,
            'poFilename': poFilename,
            'grnKey': grnKey if grnKey else "",
            'grnFilename': os.path.basename(grnKey) if grnKey else "",
            'grnNumbers': list(set(grn.get('grnNumber') for grn in relevantGrnData if grn.get('grnNumber') is not None and isinstance(grn.get('grnNumber'), (str, int, float)))),
            'invoiceNumbers': [invoiceNumber] if invoiceNumber else [],
            'jobTrackingId': jobTrackingId
        }
        # Send to SQS
        sendToSQS(message)

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

def deserializeDdbRecord(record, type_deserializer=TypeDeserializer()):
    return type_deserializer.deserialize({"M": record})

@tracer.capture_method
def createJobTracking(merchantId, totalInvoices=0):
    jobTrackingId = str(uuid.uuid4())
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    jobData = {
        'jobTrackingId': jobTrackingId,
        'merchantId': merchantId,
        'module': '3waymatching',
        'status': 'IN_PROGRESS',
        'totalInvoices': totalInvoices,
        'totalCompletedRecords': 0,
        'totalFailedRecords': 0,
        'createdAt': timestamp,
        'createdBy': "System",
        'updatedAt': timestamp,
        'updatedBy': "System"
    }
    
    JOB_TRACKING_DDB_TABLE.put_item(Item=jobData)
    return jobTrackingId

def create_response(status_code, message, payload=None):
    if not payload:
        payload = {}
    
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Content-Security-Policy': "default-src 'self'; script-src 'self'",
            'X-Content-Type-Options': 'nosniff',
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
            'Cache-control': 'no-store',
            'Pragma': 'no-cache',
            'X-Frame-Options': 'SAMEORIGIN'
        },
        'body': json.dumps({
            "statusCode": status_code,
            "message": message,
            **payload
        }, default=decimalDefault)
    }

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
                'enableExceptionFields': False,
                'useDualPoNumbers': False,
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
                'standardizationPrompt': None,
                'threeWayMatchingPrompt': None
            },
            'requiredFields': {}
        }

@tracer.capture_method
def extract_merchant_id_from_event(event):
    """Extract merchantId from DynamoDB stream event"""
    for record in event['Records']:
        if record['eventName'] in ['INSERT', 'MODIFY']:
            new_image = record.get('dynamodb', {}).get('NewImage', {})
            if 'merchantId' in new_image:
                return new_image['merchantId']['S']  # String value from DynamoDB
    return None