import os
import boto3
import json
import time
import urllib.parse
from datetime import datetime, timedelta
import uuid
import pymupdf
import io
from PIL import Image, ImageDraw, ImageFont
import requests
from aws_lambda_powertools import Logger, Tracer
from botocore.exceptions import NoCredentialsError, ClientError
from bedrock_function import call_bedrock_converse
from constants import INVOICE_FIELD_MAPPING, BDA_COMPATIBLE_FIELDS
from constants import INVOICE_PROMPTS
import re

# Environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "aap-smarteye-documents-bucket-dev")
SQS_QUEUE = os.environ.get("SQS_QUEUE", "https://sqs.ap-southeast-1.amazonaws.com/582554346432/AAPTEST-ConverseFM")
DOCUMENT_UPLOAD_TABLE = os.environ.get("DOCUMENT_UPLOAD_TABLE", "AAP-DocumentUpload")

# AWS clients
S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client("sqs", region_name='ap-southeast-1')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)

# Constants
MAX_IMAGE_SIZE_BYTES = 5 * 1024 * 1024  # 5MB Bedrock limit
DPI_FALLBACK_SEQUENCE = [300, 200, 150, 100]  # DPI fallback sequence

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Main Lambda handler for invoice extraction using Converse API"""
    try:        
        for record in event.get('Records', []):
            bucketName = record.get('s3').get('bucket').get('name')
            fileKey = record.get('s3').get('object').get('key')
                        
            # Validate if the file is in the input folder and is a PDF
            if not fileKey.startswith("input/") or not fileKey.lower().endswith('.pdf'):
                continue
            
            # Extract metadata from file path
            merchantId = fileKey.split('/')[1]
            documentUploadId = fileKey.split('/')[2]
                        
            # Get document upload details
            documentUpload = getDocumentUpload(documentUploadId)
            documentType = documentUpload.get('documentType')
            
            # Only process invoices
            if documentType != 'invoice':
                continue
            
            # Process the file
            objectKey = urllib.parse.unquote_plus(fileKey).replace('+', ' ')
            fileName = objectKey.split('/')[-1]
                        
            # Extract invoice data
            extractionResult = extractInvoiceData(bucketName, objectKey, fileName, merchantId, documentUploadId)
            
            if extractionResult:
                sendToSQS(extractionResult, fileName, fileKey, merchantId, documentUploadId)
                newKey = objectKey.replace("input/", "processed/")
            else:
                logger.error(f" Extraction failed for {fileName}")
        
        return {
            "statusCode": 200,
            "body": json.dumps(" Invoice extraction completed successfully")
        }
        
    except Exception as ex:
        logger.exception(f" Error processing invoice extraction: {str(ex)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"ConverseFM Error: {str(ex)}")
        }

@tracer.capture_method
def extractInvoiceData(bucketName, objectKey, fileName, merchantId, documentUploadId):
    """Extract invoice data using Converse API with image size handling"""
    try:        
        # Download and process PDF
        localPath = f"/tmp/{fileName}"
        S3_CLIENT.download_file(bucketName, objectKey, localPath)

        # Try extraction with DPI fallback strategy
        for dpi in DPI_FALLBACK_SEQUENCE:            
            # Extract PDF pages as images
            pdfPages, pdfPageDimensions = extractPngBytesFromPdf(localPath, dpi=dpi)
            
            if not pdfPages:
                logger.error(f" No valid images after compression at {dpi} DPI")
                continue
            
            # Perform entity extraction
            extractionStart = time.time()
            extractionResult = performEntityExtraction(pdfPages, fileName)
            extractionDuration = time.time() - extractionStart
            
            if extractionResult:                
                # Perform bounding box extraction
                boundingBoxStart = time.time()
                boundingBoxResult = performBoundingBoxExtraction(pdfPages, extractionResult, pdfPageDimensions)
                boundingBoxDuration = time.time() - boundingBoxStart
                                
                # Format as simplified output for PBEOFM
                simplifiedOutput = formatAsSimplifiedOutput(extractionResult, boundingBoxResult, fileName, extractionDuration, boundingBoxDuration)
                
                # Cleanup
                if os.path.exists(localPath):
                    os.remove(localPath)
                
                return simplifiedOutput
            else:
                continue
        
        # If all DPI levels failed
        logger.error(f" All DPI levels failed for {fileName}")
        
        # Cleanup
        if os.path.exists(localPath):
            os.remove(localPath)
            
        return None
        
    except Exception as ex:
        logger.exception(f" Error extracting invoice data for {fileName}: {str(ex)}")
        # Cleanup on error
        if 'localPath' in locals() and os.path.exists(localPath):
            os.remove(localPath)
        return None

@tracer.capture_method
def performEntityExtraction(pdfPages, fileName):
    """Extract entities using Converse API"""
    try:        
        prompt = INVOICE_PROMPTS['INVOICE']['extraction'].format(
            actualPageCount=len(pdfPages),
            fileName=fileName
        )
        bedrockResult = call_bedrock_converse(pdfPages, prompt, mode="extraction", document_type="invoice")
        
        if not bedrockResult.get("success", False):
            logger.error(f" Bedrock extraction failed for {fileName}: {bedrockResult.get('error')}")
            return None
        
        # Parse response
        modelResponse = bedrockResult["response"]
        responseContent = modelResponse["output"]["message"]["content"][0]["text"]
        extractedData = json.loads(responseContent)
        
        return extractedData
        
    except Exception as ex:
        logger.exception(f" Error in entity extraction for {fileName}: {str(ex)}")
        return None

@tracer.capture_method
def performBoundingBoxExtraction(pdfPages, extractedData, pdfPageDimensions):
    """Extract bounding boxes for the entities"""
    try:        
        entitiesForBoundingBox = formatEntitiesForBoundingBoxPrompt(extractedData)
        prompt = INVOICE_PROMPTS['INVOICE']['bounding_box'].format(
            actualPageCount=len(pdfPages),
            extractedEntities=entitiesForBoundingBox
        )
        bedrockResult = call_bedrock_converse(pdfPages, prompt, mode="bounding_box", document_type="invoice")
        
        if not bedrockResult.get("success", False):
            logger.warning(f" Bounding box extraction failed: {bedrockResult.get('error')}")
            return {}
        
        # Parse bounding box response
        modelResponse = bedrockResult["response"]
        responseContent = modelResponse["output"]["message"]["content"][0]["text"]
        boundingBoxData = json.loads(responseContent)
        
        # Process bounding boxes
        processedBoundingBoxes = processBoundingBoxResponse(boundingBoxData, pdfPages, pdfPageDimensions)
        
        return processedBoundingBoxes
        
    except Exception as ex:
        logger.exception(f" Error in bounding box extraction: {str(ex)}")
        return {}

@tracer.capture_method
def formatAsSimplifiedOutput(extractionResult, boundingBoxResult, fileName, extractionDuration, boundingBoxDuration):
    """Format the extraction results as simplified output for PBEOFM processing"""
    try:        
        # Calculate overall confidence (convert to 0-100 scale)
        totalConfidence = 0
        validFields = 0
        
        for entity in extractionResult:
            if entity.get('entityValue') and entity.get('entityValue') != "":
                totalConfidence += entity.get('confidence', 0)
                validFields += 1
        
        overallConfidence = (totalConfidence / validFields) if validFields > 0 else 0
        
        # Separate invoice-level and line-item data
        invoiceData = {}
        serviceTable = []
        explainabilityInfo = []
        
        # Process entities
        for entity in extractionResult:
            entityName = entity.get('entityName')
            entityValue = entity.get('entityValue', '')
            confidence = entity.get('confidence', 0) / 100.0  # Keep 0-1 scale for explainability
            pageNumber = normalizePageNumber(entity.get('pageNumber', 1))
                        
            # Get bounding box info
            boundingBoxInfo = boundingBoxResult.get(entityName, {})
            
            # Create simplified explainability info entry
            explainabilityEntry = {
                entityName: {
                    "success": True,
                    "confidence": confidence,
                    "type": "string" if not entityName.endswith(('Amount', 'Rate', 'Price', 'Quantity')) else "number",
                    "value": entityValue
                }
            }
            
            # Add geometry if bounding box exists
            if boundingBoxInfo:
                explainabilityEntry[entityName]["geometry"] = [{
                    "boundingBox": boundingBoxInfo,
                    "vertices": convertBoundingBoxToVertices(boundingBoxInfo),
                    "page": pageNumber
                }]
            
            # Only add invoice-level fields to explainability_info (not line item fields)
            if not isLineItemField(entityName):
                explainabilityInfo.append(explainabilityEntry)
                
                # Map to BDA field names for invoice level
                bdaFieldName = INVOICE_FIELD_MAPPING.get(entityName, entityName)
                if bdaFieldName in BDA_COMPATIBLE_FIELDS:
                    invoiceData[bdaFieldName] = entityValue
        
        # Process line items (service_table) - create multiple line items from extracted fields
        serviceTable = processLineItems(extractionResult, boundingBoxResult)
        
        # Create simplified output structure for PBEOFM
        simplifiedOutput = {
            "matched_blueprint": {
                "arn": "arn:aws:bedrock:us-east-1:582554346432:blueprint/converse-invoice",
                "name": "Invoice_Converse_Extraction",
                "confidence": overallConfidence / 100.0  # Convert to 0-1 scale for blueprint confidence
            },
            "document_class": {
                "type": "Invoice"
            },
            "split_document": {
                "page_indices": list(range(len(extractionResult)))
            },
            "inference_result": {
                **invoiceData,
                "service_table": serviceTable
            },
            "explainability_info": explainabilityInfo
        }
        
        return simplifiedOutput
        
    except Exception as ex:
        logger.exception(f" Error formatting simplified output for {fileName}: {str(ex)}")
        return None

@tracer.capture_method
def processLineItems(extractionResult, boundingBoxResult):
    """Process and group line item data into multiple line items"""
    try:
        
        # Group line item fields by their number (lineItem1_, lineItem2_, etc.)
        lineItemGroups = {}
        
        for entity in extractionResult:
            entityName = entity.get('entityName')
            entityValue = entity.get('entityValue', '')
            
            if isLineItemField(entityName):
                # Extract line item number and field name
                lineItemNumber, fieldName = parseLineItemField(entityName)
                
                if lineItemNumber and fieldName:
                    if lineItemNumber not in lineItemGroups:
                        lineItemGroups[lineItemNumber] = {}
                    
                    # Map to BDA field name
                    bdaFieldName = INVOICE_FIELD_MAPPING.get(fieldName, fieldName)
                    lineItemGroups[lineItemNumber][bdaFieldName] = entityValue
                            
        # Convert groups to list of line items
        lineItems = []
        for lineItemNumber in sorted(lineItemGroups.keys()):
            lineItem = lineItemGroups[lineItemNumber]
            
            # Only add line items that have at least a description or item code
            if lineItem.get('Description') or lineItem.get('itemCode'):
                lineItems.append(lineItem)
        
        return lineItems
        
    except Exception as ex:
        logger.exception(f" Error processing line items: {str(ex)}")
        return []

@tracer.capture_method
def isLineItemField(entityName):
    """Check if an entity name represents a line item field"""
    return entityName.startswith('lineItem') and '_' in entityName

@tracer.capture_method
def parseLineItemField(entityName):
    """Parse line item field name to extract line item number and field name"""
    try:
        # Expected format: lineItem1_Description, lineItem2_UnitPrice, etc.
        pattern = r'^lineItem(\d+)_(.+)$'
        match = re.match(pattern, entityName)
        
        if match:
            lineItemNumber = int(match.group(1))
            fieldName = match.group(2)
            return lineItemNumber, fieldName
        
        return None, None
        
    except Exception as ex:
        logger.error(f" Error parsing line item field {entityName}: {str(ex)}")
        return None, None

@tracer.capture_method
def formatEntitiesForBoundingBoxPrompt(extractedData):
    """Format extracted entities for bounding box prompt"""
    try:
        entitiesText = []
        
        for idx, entity in enumerate(extractedData, 1):
            entityName = entity.get('entityName')
            entityValue = entity.get('entityValue', '')
            pageNumber = normalizePageNumber(entity.get('pageNumber', 1))
            
            if entityValue and entityValue != "":
                entitiesText.append(
                    f"{idx}. Entity: {entityName}\n"
                    f"Target Value: \"{entityValue}\"\n"
                    f"Expected Page: {pageNumber}"
                )
        
        return "\n\n".join(entitiesText)
        
    except Exception as ex:
        logger.exception(f" Error formatting entities for bounding box prompt: {str(ex)}")
        return ""

@tracer.capture_method
def processBoundingBoxResponse(boundingBoxData, pdfPages, pdfPageDimensions):
    """Process bounding box response and convert coordinates"""
    try:        
        processedBoxes = {}

        for item in boundingBoxData:
            entityName = item.get('entityName', '')
            boundingBox = item.get('boundingBox', [0, 0, 0, 0])
            pageNumber = normalizePageNumber(item.get('pageNumber', 1))

            if len(boundingBox) == 4 and boundingBox != [0, 0, 0, 0]:
                if pageNumber > 0 and pageNumber <= len(pdfPageDimensions):
                    pngDimensions = pdfPages[pageNumber - 1][1]
                    pdfDimensions = pdfPageDimensions[pageNumber - 1]
                    normalizedBox = convertPngToNormalizedPdfCoordinates(
                        boundingBox, pngDimensions, pdfDimensions
                    )
                    processedBoxes[entityName] = normalizedBox

        return processedBoxes
        
    except Exception as ex:
        logger.exception(f" Error processing bounding box response: {str(ex)}")
        return {}

@tracer.capture_method
def normalizePageNumber(pageNumber):
    """Normalize page number to ensure it's an integer"""
    try:
        # Handle nested lists and various data types
        while isinstance(pageNumber, list):
            if len(pageNumber) > 0:
                pageNumber = pageNumber[0]
            else:
                pageNumber = 1
                break
        
        # Convert to integer
        if isinstance(pageNumber, (int, float)):
            return int(pageNumber)
        elif isinstance(pageNumber, str) and pageNumber.isdigit():
            return int(pageNumber)
        else:
            logger.warning(f" Invalid page number format: {pageNumber}, defaulting to 1")
            return 1
            
    except Exception as ex:
        logger.warning(f" Error normalizing page number {pageNumber}: {str(ex)}, defaulting to 1")
        return 1

@tracer.capture_method
def convertBoundingBoxToVertices(boundingBox):
    """Convert bounding box to vertices format"""
    try:
        if not boundingBox or not isinstance(boundingBox, dict):
            return []
        
        left = boundingBox.get('left', 0)
        top = boundingBox.get('top', 0)
        width = boundingBox.get('width', 0)
        height = boundingBox.get('height', 0)
        
        return [
            {"x": left, "y": top},
            {"x": left + width, "y": top},
            {"x": left + width, "y": top + height},
            {"x": left, "y": top + height}
        ]
        
    except Exception as ex:
        logger.error(f" Error converting bounding box to vertices: {ex}")
        return []

@tracer.capture_method
def convertPngToNormalizedPdfCoordinates(pngBbox, pngDimensions, pdfDimensions):
    """Convert PNG coordinates to normalized PDF coordinates"""
    try:
        x1, y1, x2, y2 = pngBbox
        
        # Normalize to 0-1 scale
        left = x1 / 1000.0
        top = y1 / 1000.0
        width = (x2 - x1) / 1000.0
        height = (y2 - y1) / 1000.0
        
        return {
            'top': max(0, min(1, top)),
            'left': max(0, min(1, left)),
            'width': max(0, min(1, width)),
            'height': max(0, min(1, height))
        }
    except Exception as ex:
        logger.error(f" Error converting coordinates: {ex}")
        return {}

@tracer.capture_method
def extractPngBytesFromPdf(pdfPath, dpi=300):
    """Extract PDF pages as PNG images"""
    try:        
        pngBytesList = []
        pdfPageDimensions = []
        doc = pymupdf.open(pdfPath)
        
        try:
            for pageNum in range(len(doc)):
                page = doc.load_page(pageNum)
                
                # Get PDF dimensions
                pdfRect = page.rect
                pdfPageDimensions.append((pdfRect.width, pdfRect.height))
                
                # Create PNG
                zoom = dpi / 72.0
                mat = pymupdf.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat)
                
                imgBytes = pix.tobytes("png")
                imgSize = (pix.width, pix.height)
                
                pngBytesList.append((imgBytes, imgSize))
            
            return pngBytesList, pdfPageDimensions
        finally:
            doc.close()
            
    except Exception as ex:
        logger.exception(f" Error extracting PNG bytes from PDF: {str(ex)}")
        return [], []

@tracer.capture_method
def getDocumentUpload(documentUploadId):
    """Get document upload details from DynamoDB"""
    try:        
        response = DOCUMENT_UPLOAD_DDB_TABLE.get_item(
            Key={'documentUploadId': documentUploadId}
        )
        result = response.get('Item', {})
        
        return result
        
    except Exception as ex:
        logger.error(f" Error getting document upload {documentUploadId}: {str(ex)}")
        return {}

@tracer.capture_method
def sendToSQS(extractionResult, fileName, fileKey, merchantId, documentUploadId):
    """Send extraction results directly to SQS"""
    try:        
        payload = {
            'invocationId': str(uuid.uuid4()),
            'extractionResult': extractionResult,  # Direct data instead of S3 keys
            'sourceFileName': fileName,
            'merchantId': merchantId,
            'documentUploadId': documentUploadId,
            'filePath': fileKey,
            'extractionMethod': 'converse'  # Flag to identify converse vs BDA
        }
        
        SQS_CLIENT.send_message(
            QueueUrl=SQS_QUEUE,
            MessageBody=json.dumps(payload)
        )
                
    except Exception as ex:
        logger.exception(f" Error sending to SQS for {fileName}: {str(ex)}")