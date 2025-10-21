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
from datetime import datetime, timedelta
import uuid
from bedrock_function import promptBedrock
import defaultPrompts
import urllib.parse

FIXED_ASSET_TABLE = os.environ.get('FIXED_ASSET_TABLE')
ACQUISITION_JOURNAL_TABLE = os.environ.get('ACQUISITION_JOURNAL_TABLE')
SEQUENCE_NUMBER_GENERATOR_TABLE = os.environ.get('SEQUENCE_NUMBER_GENERATOR_TABLE')
SMART_EYE_BUCKET = os.environ.get('SMART_EYE_BUCKET')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
AGENT_MAPPING_BUCKET = os.environ.get('AGENT_MAPPING_BUCKET')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')

S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

FIXED_ASSET_DDB_TABLE = DDB_RESOURCE.Table(FIXED_ASSET_TABLE)
ACQUISITION_JOURNAL_DDB_TABLE = DDB_RESOURCE.Table(ACQUISITION_JOURNAL_TABLE)
SEQUENCE_NUMBER_GENERATOR_DDB_TABLE = DDB_RESOURCE.Table(SEQUENCE_NUMBER_GENERATOR_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)

logger = Logger()
tracer = Tracer()

# Field mapping for Fixed Asset data
faFieldMapping = {
    "Buy-from Vendor No_": "vendorNumber",
    "Posting Group": "postingGroup", 
    "Description": "description",
    "Direct Unit Cost": "directUnitCost",
    "Shortcut Dimension 1 Code": "dimensionCode",
    "Document Date": "documentDate",
    "Invoice No(External Document No)": "invoiceNumber",
    "Quantity": "quantity",
    "Unit of Measure Code": "unitOfMeasure",
    "Location Code": "locationCode",
    "Item No_": "itemNumber",
    "Type": "type",
    "No_": "number"
}

# Mapping table field mappings
faClassFieldMapping = {
    "faClassCode": "faClassCode",
    "faClassName": "faClassName"
}

depreciationFieldMapping = {
    "depreciationCategory": "depreciationCategory",
    "capexCategory": "capexCategory",
    "depreciationRate": "depreciationRate"
}

# glCodeFieldMapping = {
#     "glCode": "glCode",
#     "acquisitionCostAccount": "acquisitionCostAccount"
# }

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """
    Main Lambda handler for Fixed Asset Automation with key-value pair processing
    """
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        totalInputTokens = 0
        totalOutputTokens = 0
        
        for record in event.get('Records', []):
            # Extract S3 bucket and key information
            bucketName = record['s3']['bucket']['name']
            objectKey = record['s3']['object']['key']
            
            # Decode URL-encoded object key
            decodedObjectKey = urllib.parse.unquote_plus(objectKey)
            
            # Extract merchantId and documentUploadId from path
            merchantId, documentUploadId = extractPathComponents(decodedObjectKey)
            
            # Get merchant configuration 
            merchantConfig = getMerchantConfiguration(merchantId)
            
            # Process files in key-value pair format
            faRecords = processFixedAssetExcelFile(bucketName, decodedObjectKey)
            faClassData = getFaClassMapping(merchantConfig)
            depreciationData = getDepreciationMapping(merchantConfig)
            # glCodeData = getGlCodeMapping(merchantConfig) 
                        
            # Process each record with Bedrock
            processedRecords = []
            for faRecord in faRecords:
                processedRecord, inputTokens, outputTokens = processPurchaseOrderRecordWithBedrock(
                    faRecord, 
                    faClassData,
                    depreciationData,
                    # glCodeData,  
                    merchantId,
                    now
                )
                totalInputTokens += inputTokens
                totalOutputTokens += outputTokens
                processedRecords.append(processedRecord)
            
            storeProcessedRecords(processedRecords, merchantId, now)
            createTimelineRecords(processedRecords, merchantId, now)
            
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": True,
                "message": "Fixed Asset Automation process completed successfully",
                "recordsProcessed": len(processedRecords),
                "totalInputTokens": totalInputTokens,
                "totalOutputTokens": totalOutputTokens,
                "merchantId": merchantId,
                "documentUploadId": documentUploadId,
            })
        }
        
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": False,
                "message": "Error processing fixed asset automation",
                "error": str(ex)
            })
        }

@tracer.capture_method
def extractPathComponents(decodedObjectKey):
    pathParts = decodedObjectKey.split('/')
    merchantId = None
    documentUploadId = None
    
    if len(pathParts) >= 4:  
        if pathParts[0] == 'fixed_asset' and pathParts[1] == 'upload': 
            merchantId = pathParts[2]       
            if len(pathParts) == 4:      
                filename = pathParts[3]     
                documentUploadId = filename.split('.')[0]  
            elif len(pathParts) == 5:      
                documentUploadId = pathParts[3]
                filename = pathParts[4]
    
    if not merchantId:
        raise ValueError(f"Invalid path structure. Expected: fixed_asset/upload/{{merchantId}}/{{filename.xlsx}}, got: {decodedObjectKey}")
    
    return merchantId, documentUploadId

@tracer.capture_method
def processPurchaseOrderRecordWithBedrock(faRecord, faClassData, depreciationData, merchantId, now):
    """Process a single purchase order record using Bedrock with key-value pair data"""
    totalInputTokens = 0
    totalOutputTokens = 0
    
    # Extract fields from key-value pair format
    vendorNo = getFieldValue(faRecord, 'vendorNumber') or ''
    postingGroup = getFieldValue(faRecord, 'postingGroup') or ''
    description = getFieldValue(faRecord, 'description') or ''
    directUnitCost = float(getFieldValue(faRecord, 'directUnitCost') or '0')
    dimensionCode = getFieldValue(faRecord, 'dimensionCode') or ''
    documentDate = getFieldValue(faRecord, 'documentDate') or ''
    invoiceNo = getFieldValue(faRecord, 'invoiceNumber') or ''
    quantity = float(getFieldValue(faRecord, 'quantity') or '0')
    unitOfMeasure = getFieldValue(faRecord, 'unitOfMeasure') or ''  
    
    faClassMapping, inputTokens, outputTokens = performFaClassMapping(
        postingGroup, 
        description,
        faClassData
    )
    totalInputTokens += inputTokens
    totalOutputTokens += outputTokens
    
    faClassCode = faClassMapping.get('faClassCode', postingGroup)
    faClassName = getFaClassName(faClassCode, faClassData)
    
    depreciationMapping, inputTokens, outputTokens = performDepreciationMapping(
        faClassCode,
        description,
        depreciationData,
        faClassName  
    )
    totalInputTokens += inputTokens
    totalOutputTokens += outputTokens
    
    depreciationRate = depreciationMapping.get('depreciationRate', 10.0)
    
    # Comment out GL mapping and use fixed value:
    # glMapping, inputTokens, outputTokens = performGlCodeMapping(
    #     faClassCode,
    #     description,
    #     glCodeData
    # )
    # totalInputTokens += inputTokens
    # totalOutputTokens += outputTokens
    # glAccountCode = glMapping.get('glAccountCode', '1023500014')
    
    glAccountCode = '1023500014'  # ← Fixed value
    
    # Generate FA Number
    faNumber = generateFaNumber(faClassCode, merchantId, now)
    
    # Calculate posting date (Friday of current week)
    postingDate = calculatePostingDate()
    
    # Calculate depreciation start date (1st of posting month)
    depreciationStartDate = calculateDepreciationStartDate(postingDate)
    
    # Generate document number
    documentNo = generateDocumentNumber(merchantId, now)
    
    return {
        'faNumber': faNumber,
        'description': description,
        'faClassCode': faClassCode,
        'faClassName': faClassName, 
        'vendorNo': vendorNo,
        'postingGroup': postingGroup,
        'locationCode': dimensionCode,
        'directUnitCost': directUnitCost,
        'quantity': quantity,
        'unitOfMeasure': unitOfMeasure,
        'postingDate': postingDate,
        'documentDate': documentDate,
        'invoiceNo': invoiceNo,
        'depreciationRate': depreciationRate,
        'depreciationStartDate': depreciationStartDate,
        'documentNo': documentNo,
        'glAccountCode': glAccountCode,
        'merchantId': merchantId,
    }, totalInputTokens, totalOutputTokens

@tracer.capture_method
def performFaClassMapping(postingGroup, description, faClassData):
    """Use Bedrock to intelligently map posting group to FA Class Code"""
    
    # Format FA class data for Bedrock
    formattedDatabase = formatMappingDatabase(faClassData, 'fa_class')
    
    inputItem = {
        "postingGroup": postingGroup,
        "description": description
    }
    
    # Get default prompt
    defaultPrompt = defaultPrompts.FA_CLASS_MAPPING_PROMPT
    prompt = defaultPrompt.format(
        database=formattedDatabase,
        input_item=json.dumps(inputItem)
    )
    
    response, inputTokens, outputTokens = promptBedrock(prompt)
    result = json.loads(response)
    return result, inputTokens, outputTokens

@tracer.capture_method
def performDepreciationMapping(faClassCode, description, depreciationData, faClassName=None):
    """Use Bedrock to map FA Class Code to depreciation rate"""
    
    formattedDatabase = formatMappingDatabase(depreciationData, 'depreciation')
    
    inputItem = {
        "faClassCode": faClassCode,
        "faClassName": faClassName or "", 
        "description": description
    }
    
    defaultPrompt = defaultPrompts.DEPRECIATION_RATE_MAPPING_PROMPT
    prompt = defaultPrompt.format(
        database=formattedDatabase,
        input_item=json.dumps(inputItem)
    )
    
    response, inputTokens, outputTokens = promptBedrock(prompt)
    
    result = json.loads(response)
    return result, inputTokens, outputTokens

# @tracer.capture_method
# def performGlCodeMapping(faClassCode, description, glCodeData):
#     """Use Bedrock to map FA Class Code to GL account code"""
    
#     formattedDatabase = formatMappingDatabase(glCodeData, 'gl_code')
    
#     inputItem = {
#         "faClassCode": faClassCode,
#         "description": description
#     }
    
#     defaultPrompt = defaultPrompts.GL_CODE_MAPPING_PROMPT
#     prompt = defaultPrompt.format(
#         database=formattedDatabase,
#         input_item=json.dumps(inputItem)
#     )
    
#     response, inputTokens, outputTokens = promptBedrock(prompt)
    
#     try:
#         result = json.loads(response)
#         logger.info(f"GL Code mapping result: {result}")
#         return result, inputTokens, outputTokens
#     except Exception as e:
#         logger.error(f"Error parsing GL code mapping response: {str(e)}")
#         return {
#             'glAccountCode': '1023500014',
#             'completeMapping': False,
#             'confidence': 0.0
#         }, inputTokens, outputTokens

@tracer.capture_method
def formatMappingDatabase(mappingData, databaseType):
    """Format mapping database for Bedrock prompts"""
    
    # Handle key-value pair format
    if isinstance(mappingData, dict) and 'content' in mappingData:
        content = mappingData['content']
        
        if databaseType == 'fa_class':
            columns = "faClassCode|faClassName"
            rows = []
            for item in content:
                row = f"{item.get('faClassCode', '')}|{item.get('faClassName', '')}"
                rows.append(row)
                
        elif databaseType == 'depreciation':
            columns = "depreciationCategory|capexCategory|depreciationRate"
            rows = []
            for item in content:
                row = f"{item.get('depreciationCategory', '')}|{item.get('capexCategory', '')}|{item.get('depreciationRate', '')}"
                rows.append(row)
                
        elif databaseType == 'gl_code':
            columns = "glCode|acquisitionCostAccount"
            rows = []
            for item in content:
                row = f"{item.get('glCode', '')}|{item.get('acquisitionCostAccount', '')}"
                rows.append(row)
                
        formattedDb = f"Columns: {columns}\n" + "\n".join(rows)
        
        return formattedDb
    
    # Fallback for traditional DataFrame format (if still needed)
    else:
        if databaseType == 'fa_class':
            columns = "Code|Name"
            columnNames = ['Code', 'Name']
        elif databaseType == 'depreciation':
            columns = "Category|CAPEX Category|Rate"
            columnNames = ['CATEGORY', 'CAPEX Category', 'RATE']
        elif databaseType == 'gl_code':
            columns = "Code|Acquisition Cost Account"
            columnNames = ['Code', 'Acquisition Cost Account']
        else:
            columns = "Code|Description"
            columnNames = list(mappingData.columns)
        
        availableColumns = [col for col in columnNames if col in mappingData.columns]
        
        rows = []
        for _, row in mappingData.iterrows():
            formattedRow = "|".join(str(row.get(col, "")) for col in availableColumns)
            rows.append(formattedRow)
        
        actualColumns = "|".join(availableColumns)
        formattedDb = f"Columns: {actualColumns}\n" + "\n".join(rows)
        
        return formattedDb

@tracer.capture_method
def generateFaNumber(faClassCode, merchantId, now):
    """Generate FA Number with category-based sequence"""

    categoryPrefix = getCategoryPrefix(faClassCode)
    sequenceKey = f'FA-{merchantId}-{categoryPrefix}'
    
    sequenceResp = getSequenceNumberGenerator(sequenceKey)
    
    if not sequenceResp:
        createSequenceNumberGenerator(sequenceKey, now)
        faNumber = f'{categoryPrefix}0001'
    else:
        latestValue = int(sequenceResp.get('latestValue', '0001'))
        newValue = str(latestValue + 1).zfill(4)
        updateSequenceNumberGenerator(sequenceKey, newValue, now)
        faNumber = f'{categoryPrefix}{newValue}'
    
    return faNumber

@tracer.capture_method
def getCategoryPrefix(faClassCode):
    """Get category prefix based on FA Class Code"""
    prefixMapping = {
        'S-EQUIP': 'SE',
        'S-COM': 'SC',
        'MV': 'MV',
        'OE': 'OE',
        'P-EQUIP': 'PE',
        'RE': 'RE',
        'S-FF': 'SF',
        'S-RE': 'SR',
        'WH-EQUIP': 'WE',
        'WH-FF': 'WF',
        'WH-COM': 'WC',
        'CK-E': 'CE',
        'F.BLDG': 'FB',
        'F.LAND': 'FL',
        'FC': 'FC',
        'IT': 'IT',
        'KITCHEN': 'KI'
    }
    return prefixMapping.get(faClassCode, 'FA')

@tracer.capture_method
def calculatePostingDate():
    """Calculate posting date as Friday of current week"""
    today = datetime.now()
    daysUntilFriday = (4 - today.weekday()) % 7
    if daysUntilFriday == 0 and today.weekday() != 4:
        daysUntilFriday = 7
    friday = today + timedelta(days=daysUntilFriday)
    return friday.strftime('%m/%d/%Y')

@tracer.capture_method
def calculateDepreciationStartDate(postingDate):
    """Calculate depreciation start date as 1st of posting month"""
    postingDatetime = datetime.strptime(postingDate, '%m/%d/%Y')
    firstOfMonth = postingDatetime.replace(day=1)
    return firstOfMonth.strftime('%m/%d/%Y')

@tracer.capture_method
def generateDocumentNumber(merchantId, now):
    """Generate document number in format FJNL-YYMM-XXXX"""

    currentDate = datetime.now()
    yearMonth = currentDate.strftime('%y%m')
    docPrefix = f'FJNL-{yearMonth}'  
    
    sequenceResp = getSequenceNumberGenerator(docPrefix)
    
    if not sequenceResp:
        createSequenceNumberGenerator(docPrefix, now)
        docNumber = f'{docPrefix}-000001'
    else:
        latestValue = int(sequenceResp.get('latestValue', '000001'))
        newValue = str(latestValue + 1).zfill(6)
        updateSequenceNumberGenerator(docPrefix, newValue, now)
        docNumber = f'{docPrefix}-{newValue}'
    
    return docNumber

@tracer.capture_method
def storeProcessedRecords(processedRecords, merchantId, now):
    
    fixedAssetPayloads = []
    acquisitionJournalPayloads = []
    
    for record in processedRecords:
        fixedAssetId = str(uuid.uuid4())
        fixedAssetPayload = {
            'fixedAssetId': fixedAssetId, 
            'merchantId': record.get('merchantId', ''),
            'faNumber': record['faNumber'],
            'description': record['description'],
            'responsibleEmployee': '',
            'faClassCode': record['faClassCode'],
            'faSubclassCode': '',
            'faLocationCode': record['locationCode'],
            'searchDescription': record['description'],
            'acquired': True,
            'blocked': False,
            'faPostingGroup': record['faClassCode'],
            'outletDimCode': record['locationCode'],
            'vendorNo': record['vendorNo'],
            'maintenanceVendorNo': '',
            'underMaintenance': False,
            'nextServiceDate': '',
            'warrantyDate': '',
            'insuredValue': Decimal('0'),
            'salvageValue': Decimal('0'),
            'depreciationMethod': 'Straight-Line',
            'depreciationStartingDate': record['depreciationStartDate'],
            'depreciationEndingDate': '',
            'depreciationRate': Decimal(str(record['depreciationRate'])),
            'isDepreciation': 'Yes',
            'depreciationBookCode': 'COMPANY',
            'status': 'Success',
            'createdAt': now,
            'createdBy': 'System',
            'updatedAt': now,
            'updatedBy': 'System'
        }
        fixedAssetPayloads.append(convertFloatsToDecimals(fixedAssetPayload))
        
        acquisitionJournalId = str(uuid.uuid4())
        journalPayload = {
            'acquisitionJournalId': acquisitionJournalId, 
            'merchantId': record.get('merchantId', ''),
            'postingDate': record['postingDate'],
            'documentDate': record['documentDate'],
            'externalDocumentNo': record['invoiceNo'],
            'documentNo': record['documentNo'],
            'accountType': 'Fixed Asset',
            'accountNo': record['faNumber'],
            'depreciationBookCode': 'COMPANY',
            'faPostingType': 'Acquisition Cost',
            'description': record['description'],
            'amount': Decimal(str(record['directUnitCost'])),
            'balAccountType': 'G/L Account',
            'balAccountNo': '1023500014',
            'outletDimCode': record['locationCode'],
            'status': 'Success',
            'createdAt': now,
            'createdBy': 'System',
            'updatedAt': now,
            'updatedBy': 'System'
        }
        acquisitionJournalPayloads.append(convertFloatsToDecimals(journalPayload))
    
    with FIXED_ASSET_DDB_TABLE.batch_writer() as batch:
        for payload in fixedAssetPayloads:
            batch.put_item(Item=payload)
    
    # Batch write Acquisition Journal records
    with ACQUISITION_JOURNAL_DDB_TABLE.batch_writer() as batch:
        for payload in acquisitionJournalPayloads:
            batch.put_item(Item=payload)

@tracer.capture_method
def createTimelineRecords(processedRecords, merchantId, now):
    """Create timeline records for tracking """
    if not TIMELINE_DDB_TABLE:
        return
    
    timelinePayloads = []
    for record in processedRecords:
        timelinePayload = {
            'timelineId': str(uuid.uuid4()),
            'merchantId': merchantId,
            'timelineForId': record.get('faNumber'),
            'title': 'Fixed Asset Created',
            'type': 'fixed_asset',
            'description': f"Fixed Asset {record['faNumber']} - {record['description']} created successfully",
            'createdAt': now,
            'createdBy': 'System',
            'updatedAt': now,
            'updatedBy': 'System'
        }
        timelinePayloads.append(timelinePayload)
    
    with TIMELINE_DDB_TABLE.batch_writer() as batch:
        for payload in timelinePayloads:
            batch.put_item(Item=payload)

@tracer.capture_method
def getSequenceNumberGenerator(sequenceNumberGeneratorId):
    sequenceNumResp = SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.get_item(
        Key={'sequenceNumberGeneratorId': sequenceNumberGeneratorId}
    ).get('Item')
    
    return sequenceNumResp

@tracer.capture_method
def createSequenceNumberGenerator(sequenceNumberGeneratorId, now):
    SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.put_item(Item={
        'sequenceNumberGeneratorId': sequenceNumberGeneratorId,
        'latestValue': '0001',
        'updatedAt': now
    })

@tracer.capture_method
def updateSequenceNumberGenerator(sequenceNumberGeneratorId, latestValue, now):
    SEQUENCE_NUMBER_GENERATOR_DDB_TABLE.update_item(
        Key={'sequenceNumberGeneratorId': sequenceNumberGeneratorId},
        UpdateExpression='SET latestValue=:latestValue, updatedAt=:updatedAt',
        ExpressionAttributeValues={
            ':latestValue': latestValue,
            ':updatedAt': now
        }
    )

def convertFloatsToDecimals(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convertFloatsToDecimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convertFloatsToDecimals(item) for item in obj]
    else:
        return obj

@tracer.capture_method
def processFixedAssetExcelFile(bucketName, objectKey):
    """Process the Excel file and extract data in key-value pair format"""

    response = S3_CLIENT.get_object(Bucket=bucketName, Key=objectKey)
    excelContent = response['Body'].read()
    excelData = pd.ExcelFile(io.BytesIO(excelContent))
    
    # Extract only the store db extracted entry sheet
    if 'store db extracted entry' not in excelData.sheet_names:
        raise ValueError("Required sheet 'store db extracted entry' not found in Excel file")
    
    rawData = excelData.parse('store db extracted entry')
    
    # Clean and validate data
    rawData = rawData.dropna(how='all')  # Remove completely empty rows
    rawData = rawData.fillna('')  # Fill NaN values with empty strings
    
    # Convert to key-value pair format 
    faData = convertToKeyValueFormat(rawData, faFieldMapping, "Fixed Asset Data")
    
    return faData

@tracer.capture_method
def convertToKeyValueFormat(dataframe, fieldMapping, tableName):
    """Convert dataframe to key-value pair format"""

    records = dataframe.to_dict('records')
    convertedRecords = []
    
    # Fields that should preserve leading zeros
    zeroPaddedFields = ['Shortcut Dimension 1 Code', 'Location Code', 'Item No_']
    
    for index, record in enumerate(records):
        mappedContent = []
        
        for excelHeader, standardizedField in fieldMapping.items():
            if excelHeader in dataframe.columns:
                value = record.get(excelHeader, '')
                
                if pd.isna(value):
                    value = ''
                else:
                    # Special handling for zero-padded fields
                    if excelHeader in zeroPaddedFields:
                        # Ensure leading zeros are preserved
                        if isinstance(value, (int, float)) and not pd.isna(value):
                            # Convert to string and pad with zeros if needed
                            value = str(int(value)).zfill(4)  # Pad to 4 digits
                        else:
                            value = str(value).zfill(4) if str(value).isdigit() else str(value)
                    else:
                        value = str(value)
                
                mappedContent.append({
                    "fieldName": standardizedField,
                    "fieldValue": value
                })
        
        convertedRecord = {
            "table": tableName,
            "recordIndex": index,
            "content": mappedContent
        }
        convertedRecords.append(convertedRecord)
    
    return convertedRecords

@tracer.capture_method
def convertMappingToKeyValueFormat(dataframe, fieldMapping, tableName):
    """Convert mapping dataframes to key-value pair format"""

    records = dataframe.to_dict('records')
    mappedContent = []
    
    # Process all records into a single content array (for mapping tables)
    for record in records:
        recordContent = {}
        for excelHeader, standardizedField in fieldMapping.items():
            if excelHeader in dataframe.columns:
                value = record.get(excelHeader, '')
                if pd.isna(value):
                    value = ''
                else:
                    value = str(value)
                recordContent[standardizedField] = value
        mappedContent.append(recordContent)
    
    return {
        "table": tableName,
        "content": mappedContent
    }

@tracer.capture_method
def getFaClassMapping(merchantConfig):
    """Load FA Class mapping data from S3 CSV file in key-value format"""
    mappingPaths = merchantConfig.get('mappingPaths', {})
    faClassMappingPath = mappingPaths.get('faClassMapping')
    
    if not faClassMappingPath:
        raise ValueError("FA Class mapping path not configured for merchant")
    
    rawData = parseMappings(faClassMappingPath)
    faClassData = convertMappingToKeyValueFormat(rawData, faClassFieldMapping, "FA Class Mapping")
    
    return faClassData

@tracer.capture_method
def getDepreciationMapping(merchantConfig):
    """Load depreciation mapping data from S3 CSV file in key-value format"""
    mappingPaths = merchantConfig.get('mappingPaths', {})
    depreciationMappingPath = mappingPaths.get('depreciationMapping')
    
    if not depreciationMappingPath:
        raise ValueError("Depreciation mapping path not configured for merchant")
    
    rawData = parseMappings(depreciationMappingPath)
    depreciationData = convertMappingToKeyValueFormat(rawData, depreciationFieldMapping, "Depreciation Mapping")
    
    return depreciationData

# @tracer.capture_method
# def getGlCodeMapping(merchantConfig):
#     """Load GL Code mapping data from S3 CSV file in key-value format"""
#     mappingPaths = merchantConfig.get('mappingPaths', {})
#     glCodeMappingPath = mappingPaths.get('glCodeMapping')
    
#     if not glCodeMappingPath:
#         raise ValueError("GL Code mapping path not configured for merchant")
    
#     rawData = parseMappings(glCodeMappingPath)
#     glCodeData = convertMappingToKeyValueFormat(rawData, glCodeFieldMapping, "GL Code Mapping")
    
#     logger.info(f"Loaded {len(glCodeData['content'])} GL code records from S3: {glCodeMappingPath}")
#     return glCodeData

@tracer.capture_method
def getFieldValue(record, fieldName):
    """Get field value from key-value pair format record"""
    if not record or not isinstance(record, dict):
        return None
    
    content = record.get("content", [])
    return next((item["fieldValue"] for item in content if item["fieldName"] == fieldName and item["fieldValue"] not in [None, ""]), None)

@tracer.capture_method
def parseMappings(objectKey):
    """Parse CSV mapping file from S3"""
    response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=objectKey)
    csvContent = response['Body'].read().decode('utf-8')
    
    df = pd.read_csv(io.StringIO(csvContent))
    
    # Clean the data
    df = df.dropna(how='all')  # Remove completely empty rows
    df = df.fillna('')  # Fill NaN values with empty strings
    
    return df

@tracer.capture_method
def getMerchantConfiguration(merchantId):
    """
    Get merchant configuration for Fixed Asset automation
    """
    response = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchantId})
    merchant = response.get('Item', {})
    fixedAssetConfig = merchant.get('fixedAssetConfig', {})
    
    merchantConfig = {
        'merchantId': merchantId,
        'mappingPaths': {
            'faClassMapping': merchant.get('faClassMapping'),
            'depreciationMapping': merchant.get('depreciationMapping'),
            # 'glCodeMapping': merchant.get('glCodeMapping'),
        },
        'promptPaths': fixedAssetConfig.get('promptPaths', {}),
        'customLogics': fixedAssetConfig.get('customLogics', {}),
    }
    
    return merchantConfig

@tracer.capture_method
def getFaClassName(faClassCode, faClassData):
    """Get FA Class Name from FA Class Code"""
    if isinstance(faClassData, dict) and 'content' in faClassData:
        for item in faClassData['content']:
            if item.get('faClassCode') == faClassCode:
                return item.get('faClassName', '')
    return ''
