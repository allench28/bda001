import os
import boto3
import json
import time
import csv
import io
import uuid
import ast
from constants import FILENAME_LISTS, COLUMN_HEADER_MAPPING, DOCUMENT_ENTITIES, EXAMPLE_INSTRUCTIONS, FIELD_ORDER, FORM_TABLE_DATA_MAPPING, DOCUMENT_PROMPTS
from prompt import create_bounding_box_prompt
from datetime import datetime
import pymupdf   # PyMuPDF
from aws_lambda_powertools import Logger, Tracer
from decimal import Decimal
import time
import ast
import re


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Import the bedrock utilities
from bedrock_function import call_bedrock_converse, extraction_markdown_bedrock
from bda_parser import BedrockOutputParser
from bda_function import process_pdf_with_bda

# Environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "aap-smarteye-documents-bucket-dev")
SQS_QUEUE = os.environ.get("SQS_QUEUE", "https://sqs.ap-southeast-1.amazonaws.com/582554346432/RHB-DocumentsQueue")
TEST_SQS_QUEUE = os.environ.get("TEST_SQS_QUEUE", "https://sqs.ap-southeast-1.amazonaws.com/582554346432/RHB-TestDocumentsQueue")
EXTRACTED_DOCUMENTS_TABLE = os.environ.get("EXTRACTED_DOCUMENTS_TABLE", "AAPTEST-ExtractedLoanDocuments")
DOCUMENT_UPLOAD_TABLE = os.environ.get("DOCUMENT_UPLOAD_TABLE", "AAP-DocumentUpload")



# AWS clients - initialized once
S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client("sqs", region_name='ap-southeast-1')
DDB_RESOURCE = boto3.resource('dynamodb')


EXTRACTED_DOCUMENTS_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)

BOUNDING_BOX = False

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Main Lambda handler - orchestrates the document processing pipeline"""
    try:
        logger.info(event)
        # Step 1: Parse input and initialize processing context
        processing_context = initialize_processing_context(event)
        logger.info(f"Processing file: {processing_context['fileName']} for merchant: {processing_context['merchantId']}")
        
        # Step 2: Download PDF and process with BDA
        processing_context = download_and_process_with_bda(processing_context)
        
        # Step 3: Call Bedrock for entity extraction using BDA markdown
        processing_context = extract_entities_with_bedrock(processing_context, BOUNDING_BOX)
        
        # Step 4: Process extraction results
        processing_context = process_extraction_results(processing_context)

        if BOUNDING_BOX:
            # Step 5: Get bounding boxes with Bedrock (still uses PDF pages for visual context)
            processing_context = get_bounding_boxes_with_bedrock(processing_context)

        # Step 6: Create and store outputs
        processing_context = create_and_store_outputs(processing_context)
        
        # Step 7: Update tracking and send notifications
        finalize_processing(processing_context)
        
        # Cleanup
        cleanup_temp_files(processing_context)
        
        return create_success_response(processing_context)
        
    except ProcessingError as e:
        logger.error(f"Processing error: {e.message}")
        return create_error_response(e.status_code, e.message, getattr(e, 'context', {}))
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return create_error_response(500, f"Unexpected error: {str(e)}", {})


class ProcessingError(Exception):
    """Custom exception for processing errors with context"""
    def __init__(self, message, status_code=500, context=None):
        self.message = message
        self.status_code = status_code
        self.context = context or {}
        super().__init__(self.message)


@tracer.capture_method
def initialize_processing_context(event):
    """Parse event and initialize processing context"""
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Handle SQS message vs direct invocation
    if 'Records' in event and len(event['Records']) > 0:
        file_data = json.loads(event['Records'][0]['body'])
    else:
        file_data = event

    logger.info(f'Processing file data: {file_data}')
    
    # Extract file configuration
    filename_without_ext = file_data.get('fileName', '').rsplit('.', 1)[0]
    file_config = get_configuration_from_filename(filename_without_ext)
    subDocumentType = file_config.get('documentType', 'SPA') if file_config else 'SPA'
    batchName = get_batch_name_from_uploaded_document(file_data.get('documentUploadId'))
    
    return {
        'merchantId': file_data.get('merchantId'),
        'fileName': file_data.get('fileName'),
        'filePath': file_data.get('filePath'),
        'batchName': batchName,
        'documentUploadId': file_data.get('documentUploadId'),
        'documentType': file_data.get('documentType'),
        'subDocumentType': subDocumentType,
        'timestamp': timestamp,
        'local_path': f"/tmp/{file_data.get('fileName')}",
        'extractionDuration': 0,
        'entitiesCount': 0,
        'documentStatus': 'PROCESSING',
        'bdaDuration': 0
    }


@tracer.capture_method
def get_batch_name_from_uploaded_document(documentUploadId):
    """Retrieve batch name from the uploaded document record"""
    try:
        response = DOCUMENT_UPLOAD_DDB_TABLE.get_item(Key={'documentUploadId': documentUploadId})
        if 'Item' in response:
            return response['Item'].get('fileName', 'default_batch')
        else:
            return 'default_batch'
    except Exception as e:
        logger.error(f"Error retrieving batch name: {str(e)}")
        return 'default_batch'


@tracer.capture_method
def download_and_process_with_bda(context):
    """
    Download PDF from S3 and process with Bedrock Data Automation to get markdown content.
    Also extracts PDF pages for bounding box processing.
    """
    try:
        # Download file
        S3_CLIENT.download_file(S3_BUCKET, context['filePath'], context['local_path'])
        file_size = os.path.getsize(context['local_path'])
        logger.info(f"Downloaded file: {file_size / (1024*1024):.2f} MB")
        
        # Process with BDA to get markdown content
        bda_start = time.time()
        markdown_content, markdown_s3_key, bbox_s3_key  = process_pdf_with_bda(context['filePath'])
        context['bdaDuration'] = round(time.time() - bda_start, 2)
        
        if not markdown_content:
            raise ProcessingError("BDA processing failed to extract markdown content", 500, context)
        
        context['markdown_content'] = markdown_content
        context['markdownS3Key'] = markdown_s3_key
        context['bboxS3Key'] = bbox_s3_key
        logger.info(f"BDA processing completed in {context['bdaDuration']}s, extracted {len(markdown_content)} characters")
        
        # Still extract PDF pages for bounding box processing
        # Try DPI strategies: 300 -> 200 -> 150
        for dpi in [300, 200, 150]:
            try:
                logger.info(f"Extracting PDF pages at {dpi} DPI for bounding box processing")
                pdf_pages, pdf_page_dimensions = extract_png_bytes_from_pdf(
                    context['local_path'], 
                    dpi=dpi
                )
                
                context['pdf_pages'] = pdf_pages
                context['pdf_page_dimensions'] = pdf_page_dimensions
                logger.info(f"Successfully extracted {len(pdf_pages)} pages at {dpi} DPI")
                break
                
            except Exception as e:
                error_msg = str(e)
                if is_size_error(error_msg) and dpi > 150:
                    logger.info(f"{dpi} DPI extraction too large, trying lower DPI")
                    continue
                else:
                    raise e
        
        return context
        
    except ProcessingError:
        raise
    except Exception as e:
        raise ProcessingError(f"PDF download/BDA processing failed: {str(e)}", 500, context)






@tracer.capture_method
def extract_entities_with_bedrock(context, bounding_box_extraction):
    """Call Bedrock for entity extraction using BDA markdown content"""
    try:
        extraction_start = time.time()
        
        # Use the markdown content from BDA
        markdown_content = context['markdown_content']
        logger.info(f'markdown: {markdown_content}')
        
        # Create prompt with actual page count
        prompt = create_document_prompt(
            context['subDocumentType'], 
            len(context['pdf_pages']),  # Still need page count for prompt
            markdown_content, 
            bounding_box_extraction
        )
        
        bedrock_result = extraction_markdown_bedrock(prompt)

        context['extractionDuration'] = round(time.time() - extraction_start, 2)
        
        if not bedrock_result.get("success", False):
            raise ProcessingError(f"Bedrock error: {bedrock_result.get('error', 'Unknown error')}", 500, context)
        
        context['bedrock_result'] = bedrock_result
        logger.info(f"Bedrock extraction completed in {context['extractionDuration']}s using BDA markdown")
        
        return context
        
    except ProcessingError:
        raise
    except Exception as e:
        raise ProcessingError(f"Bedrock extraction failed: {str(e)}", 500, context)


def create_document_prompt(subDocumentType, actual_page_count, markdown_content, sentence_extraction=False):
    """Create appropriate prompt based on document type"""
    prompt_template = DOCUMENT_PROMPTS.get(subDocumentType)
    
    if not prompt_template:
        raise ValueError(f"No prompt template found for document type: {subDocumentType}")
    
    # Format the page count into the pre-built prompt
    prompt = prompt_template.format(actual_page_count=actual_page_count, sentence_extraction=sentence_extraction)
    complete_prompt = f"##Document Content:\n{markdown_content} \n ##Instructions: \n{prompt}"

    return complete_prompt


@tracer.capture_method
def process_extraction_results(context):
    """Process Bedrock response and convert to structured format"""
    try:
        logger.info(f"Processing extraction results for {len(context['pdf_pages'])} pages")
        
        # Process Bedrock response
        all_entities = process_bedrock_response(context['bedrock_result'], context['pdf_pages'], context)
        
        logger.info(f'ALL ENTITIES: {all_entities}')
        # Log intermediate counts
        total_entities_from_bedrock = sum(len(page_entities) for page_entities in all_entities)
        logger.info(f"Got {total_entities_from_bedrock} total entities from Bedrock processing")
        
        # Convert to DynamoDB format
        logger.info(f"Converting to DynamoDB format for document type: {context['subDocumentType']}")
        structuredData, failedExtractionCount = convert_entities_to_dynamodb_format(all_entities, context['subDocumentType'])
        
        context['structuredData'] = structuredData
        context['entitiesCount'] = len(structuredData)
        context['documentStatus'] = 'PROCESSED'
        context['failedExtractionCount'] = failedExtractionCount
        
        # Calculate average confidence score
        context['avgConfidenceScore'] = calculate_average_confidence_score(structuredData)
        
        logger.info(f"Extraction summary - Entities: {len(structuredData)}, Failed: {failedExtractionCount}, Avg Confidence: {context['avgConfidenceScore']:.2f}")
        
        return convert_floats_to_decimals(context)
        
    except Exception as e:
        logger.error(f"Results processing failed: {str(e)}")
        raise ProcessingError(f"Results processing failed: {str(e)}", 500, context)


@tracer.capture_method
def process_bedrock_response(bedrock_result, pdf_pages, context):
    """Process the Bedrock response and extract entities"""
    try:
        model_response = bedrock_result["response"]
        response_content = model_response["output"]["message"]["content"][0]["text"]
        logger.info(f"Raw Bedrock response: {response_content}")

        thinking_pattern = r'<thinking>.*?</thinking>\s*'
        clean_response = re.sub(thinking_pattern, '', response_content, flags=re.DOTALL | re.IGNORECASE)
        clean_response = response_content.strip()

        try:
            raw_output = json.loads(clean_response)
        except json.JSONDecodeError:
            # Fallback to ast.literal_eval for single quotes
            raw_output = ast.literal_eval(clean_response)
        
        # Log raw extraction counts
        raw_entity_count = len(raw_output) if isinstance(raw_output, list) else 0
        logger.info(f"Raw Bedrock extraction returned {raw_entity_count} entities")
        
        # Process entities - organize by page
        all_pages_entities = process_extracted_entities(raw_output, len(pdf_pages))
        
        # Count entities across all pages
        total_processed_entities = sum(len(page_entities) for page_entities in all_pages_entities)
        logger.info(f'total_processed_entities: {total_processed_entities}')
        
        # Log token usage
        usage = model_response.get("usage", {})
        logger.info(f"Token usage - Input: {usage.get('inputTokens', 0)}, Output: {usage.get('outputTokens', 0)}, SET: {context['batchName']}")
        
        return all_pages_entities
        
    except Exception as e:
        logger.error(f"Error processing Bedrock response: {str(e)}")
        # Return default entities if any error occurs
        sub_doc_type = context.get('subDocumentType', 'SPA')
        expected_entities = DOCUMENT_ENTITIES.get(sub_doc_type, [])
        default_entities = []
        page_entities = []
        for entity_name in expected_entities:
            page_entities.append({
                'entity_name': entity_name,
                'entity_value': 'N/A',
                'page_number': 1,
                'confidence': 0,
                'sentence': ''
            })
        default_entities.append(page_entities)
        return default_entities


@tracer.capture_method
def process_extracted_entities(raw_output, num_pages):
    """Process the raw entity output from Bedrock into page-organized format"""
    all_pages_entities = []
    
    logger.info(f"Processing {len(raw_output) if isinstance(raw_output, list) else 0} raw entities for {num_pages} pages")
    
    # Track processing statistics
    processing_stats = {
        'total_input': len(raw_output) if isinstance(raw_output, list) else 0,
        'processed_entities': 0,
        'unknown_values': 0
    }
    
    if isinstance(raw_output, list) and len(raw_output) > 0:
        # Check if entities include page information
        if raw_output and 'page_number' in raw_output[0]:
            logger.info("Entities include page information")
            # Organize entities by page
            page_entities_dict = {}
            for entity in raw_output:
                page_num = entity.get('page_number', 1)
                
                if page_num == 0:
                    page_num = 1

                if page_num not in page_entities_dict:
                    page_entities_dict[page_num] = []
                
                # Enhanced value processing
                entity_value = entity.get('entity_value', '')
                
                if entity_value is None or (isinstance(entity_value, str) and not entity_value.strip()):
                    entity_value = 'N/A'
                    processing_stats['unknown_values'] += 1
                elif not isinstance(entity_value, str):
                    entity_value = str(entity_value)
                else:
                    entity_value = entity_value.strip()
                
                processed_entity = {
                    'entity_name': entity.get('entity_name', ''),
                    'entity_value': entity_value,
                    'confidence': entity.get('confidence', 0),
                    'sentence': entity.get('sentence', ""),
                }
                
                page_entities_dict[page_num].append(processed_entity)
                processing_stats['processed_entities'] += 1
            
            # Convert to list format
            for page_num in range(1, num_pages + 1):
                page_entities = page_entities_dict.get(page_num, [])
                all_pages_entities.append(page_entities)
        else:
            logger.info("No page information found, assigning all to page 1")
            # Process entities for page 1 only
            page_entities = []
            for entity in raw_output:
                entity_value = entity.get('entity_value', '')
                
                if entity_value is None or (isinstance(entity_value, str) and not entity_value.strip()):
                    entity_value = 'N/A'
                    processing_stats['unknown_values'] += 1
                elif not isinstance(entity_value, str):
                    entity_value = str(entity_value)
                else:
                    entity_value = entity_value.strip()
                
                processed_entity = {
                    'entity_name': entity.get('entity_name', ''),
                    'entity_value': entity_value,
                    'confidence': entity.get('confidence', 0)
                }
                
                page_entities.append(processed_entity)
                processing_stats['processed_entities'] += 1
            
            all_pages_entities.append(page_entities)
            # Add empty lists for remaining pages
            for i in range(1, num_pages):
                all_pages_entities.append([])
    else:
        logger.warning("No entities found in raw output or invalid format")
        for i in range(num_pages):
            all_pages_entities.append([])
    
    # Log processing statistics
    logger.info(f"Entity processing stats - Input: {processing_stats['total_input']}, Processed: {processing_stats['processed_entities']}, Unknown: {processing_stats['unknown_values']}")
    
    return all_pages_entities


@tracer.capture_method
def convert_entities_to_dynamodb_format(all_entities, subDocumentType):
    """Convert extracted entities from all pages to DynamoDB format with consistent ordering"""
    structuredData = []
    failedExtractionCount = 0
    
    # Get the expected entities for this document type
    expected_entities = DOCUMENT_ENTITIES.get(subDocumentType, [])
    
    # Create a dictionary to store entities by their column name
    entities_dict = {}
    
    logger.info(f"Converting {len(expected_entities)} expected entities for document type {subDocumentType}")
    
    # First, process all entities that were found by Bedrock
    for page_num, page_entities in enumerate(all_entities, 1):
        for entity in page_entities:
            entity_name = entity.get('entity_name', '')
            entity_value = entity.get('entity_value', 'N/A')
            confidence = entity.get('confidence', 0)
            page_number = entity.get('page_number', page_num)
            sentence = entity.get('sentence', "")
            
            # Skip entities with empty names
            if not entity_name:
                continue
            
            # Set to 'UNKNOWN' if empty
            if not entity_value or entity_value == "UNKNOWN" or entity_value == "NIL":
                entity_value = 'N/A'
            
            display_name = COLUMN_HEADER_MAPPING.get(entity_name, entity_name)
            
            # Store entity data
            entities_dict[entity_name] = {
                'columnName': entity_name,
                'columnValue': entity_value,
                'confidenceScore': confidence,
                'pageNumber': page_number,
                'displayName': display_name,
                'sentence': sentence
            }
    
    # Ensure ALL expected entities are present - add missing ones with UNKNOWN
    for expected_entity in expected_entities:
        if expected_entity not in entities_dict:
            display_name = COLUMN_HEADER_MAPPING.get(expected_entity, expected_entity)
            
            # Add missing entity with UNKNOWN value
            entities_dict[expected_entity] = {
                'columnName': expected_entity,
                'columnValue': 'N/A',
                'confidenceScore': 0,
                'pageNumber': 1,
                'displayName': display_name
            }
    
    # Build structuredData in the order defined by FIELD_ORDER
    for field_name in FIELD_ORDER:
        if field_name in entities_dict:
            structuredData.append(entities_dict[field_name])
    
    # Add expected entities even if not in FIELD_ORDER
    for entity_name in entities_dict:
        if entity_name not in FIELD_ORDER and entity_name in expected_entities:
            structuredData.append(entities_dict[entity_name])
    
    # Calculate failed extractions (entities with UNKNOWN values)
    failedExtractionCount = sum(1 for entity in structuredData if entity['columnValue'] == 'N/A')
    
    logger.info(f"DynamoDB conversion complete - Final entities: {len(structuredData)}, Failed extractions: {failedExtractionCount}")
    
    return structuredData, failedExtractionCount


@tracer.capture_method
def create_and_store_outputs(context):
    """Create CSV outputs and prepare extraction payload"""
    try:
        if context['entitiesCount'] > 0:
            # Create CSV outputs
            csv_outputs = create_dual_csv_outputs_from_dynamodb_data(
                context['structuredData'], 
                context['merchantId'], 
                context['batchName'],
                context['filePath'], 
                context['timestamp']
            )
            context['detailedCsv'] = csv_outputs['detailedCsv']
            context['summaryCsv'] = csv_outputs['summaryCsv']
        else:
            context['detailedCsv'] = None
            context['summaryCsv'] = None
        
        # Separate data into formData and tableData based on mapping
        separated_data = separate_data_by_mapping(context.get('structuredData', []))
        
        # Create MD and JSON files
        md_path = create_markdown_file(context)
        json_path = create_json_file(context, separated_data)
        
        # Create extraction payload for easy extension
        context['extraction_payload'] = {
            'merchantId': context['merchantId'],
            'documentUploadId': context['documentUploadId'],
            'filePath': context['filePath'],
            'data': separated_data,
            'fileName': context['fileName'],
            'documentType': context['documentType'],
            'subDocumentType': context['subDocumentType'],
            'detailedCsv': context.get('detailedCsv'),
            'summaryCsv': context.get('summaryCsv'),
            'markdownPath': md_path,
            'jsonPath': json_path,
            'entitiesCount': context['entitiesCount'],
            'failedExtractionCount': context['failedExtractionCount'],
            'extractionDuration': context['extractionDuration'],
            'bdaDuration': context['bdaDuration'],
            'documentStatus': context['documentStatus'],
            'boundingBoxStatus': context.get('boundingBoxStatus', 'NOT_ATTEMPTED'),
            'boundingBoxDuration': context.get('boundingBoxDuration', 0),
            'avgConfidenceScore': context.get('avgConfidenceScore', 0),
            'markdownS3Key': context.get('markdownS3Key'),
            'bboxS3Key': context.get('bboxS3Key')
        }
        
        return context
        
    except Exception as e:
        raise ProcessingError(f"Output creation failed: {str(e)}", 500, context)


@tracer.capture_method
def get_bounding_boxes_with_bedrock(context):
    """Take extracted entities and pass them to Bedrock to get bounding boxes"""
    try:
        # Skip if no entities were extracted
        if context['entitiesCount'] == 0:
            logger.info("No entities to extract bounding boxes for")
            context['boundingBoxStatus'] = 'SKIPPED'
            return convert_floats_to_decimals(context)
        
        # Create bounding box prompt
        bounding_box_prompt = create_bounding_box_prompt(
            country='Malaysia',
            document_type=context['subDocumentType'],
            document_language='English/Malay',
            number_of_entities=context['entitiesCount'],
            extracted_entities=format_entities_for_bounding_box_prompt(context['structuredData']),
            actual_page_count=len(context['pdf_pages'])
        )
        
        # Call Bedrock with fallback strategy
        logger.info(f"Calling Bedrock for bounding box extraction")
        bounding_box_start = time.time()
        
        bedrock_result = call_bedrock_with_fallback(context, bounding_box_prompt, mode="bounding_box")
        
        context['boundingBoxDuration'] = round(time.time() - bounding_box_start, 2)
        
        if not bedrock_result.get("success", False):
            logger.warning(f"Bounding box extraction failed: {bedrock_result.get('error', 'Unknown error')}")
            context['structuredData'] = add_empty_bounding_boxes(context['structuredData'])
            context['boundingBoxStatus'] = 'FAILED'
        else:
            # Process bounding box results with PDF dimensions
            context['structuredData'] = process_bounding_box_response(
                bedrock_result, 
                context['structuredData'], 
                context['pdf_pages'], 
                context['pdf_page_dimensions']
            )
            context['boundingBoxStatus'] = 'SUCCESS'
            logger.info(f"Bounding box extraction completed in {context['boundingBoxDuration']}s for {len(context['structuredData'])} entities")
        
        return convert_floats_to_decimals(context)
        
    except Exception as e:
        logger.error(f"Bounding box extraction failed: {str(e)}")
        context['structuredData'] = add_empty_bounding_boxes(context.get('structuredData', []))
        context['boundingBoxStatus'] = 'ERROR'
        context['boundingBoxError'] = str(e)
        return convert_floats_to_decimals(context)


def format_entities_for_bounding_box_prompt(structured_data):
    """Format the extracted entities for the bounding box prompt"""
    entities_text = []
    
    logger.info(f"Formatting {len(structured_data)} entities for bounding box prompt")
    
    for idx, entity in enumerate(structured_data, 1):
        entities_text.append(f"{idx}. Entity: {entity['columnName']}\nTarget Value: \"{entity['columnValue']}\"\nSearch From Sentence: \"{entity['sentence']}\"\nExpected Page: {entity['pageNumber']}")
    
    return "\n".join(entities_text)


@tracer.capture_method
def process_bounding_box_response(bedrock_result, original_entities, pdf_pages, pdf_page_dimensions):
    """Process the bounding box response from Bedrock with S3 image data"""
    try:
        model_response = bedrock_result["response"]
        response_content = model_response["output"]["message"]["content"][0]["text"]
        bounding_box_data = json.loads(response_content)
        
        # Create a mapping of entity names to bounding boxes
        bounding_box_map = {}
        for item in bounding_box_data:
            entity_name = item.get('entity_name', '')
            try: 
                png_bounding_box = item.get('bounding_box', [0,0,0,0])
                page_number = item.get('page_number', 1)
                confidence = item.get('confidence', 0)
                
                # Validate confidence field
                if not isinstance(confidence, (int, float)):
                    logger.warning(f"Invalid confidence format for entity '{entity_name}': {confidence}")
                    confidence = 0
                
                # Validate bounding box format
                if not isinstance(png_bounding_box, list) or len(png_bounding_box) != 4:
                    logger.warning(f"Invalid bounding box format for entity '{entity_name}': {png_bounding_box}")
                    png_bounding_box = [0, 0, 0, 0]
                
                # Validate that all coordinates are numeric
                try:
                    png_bounding_box = [float(coord) for coord in png_bounding_box]
                except (ValueError, TypeError) as e:
                    logger.warning(f"Non-numeric bounding box coordinates for entity '{entity_name}': {png_bounding_box}")
                    png_bounding_box = [0, 0, 0, 0]
                
                # Convert PNG coordinates to normalized PDF coordinates
                if page_number > 0 and page_number <= len(pdf_page_dimensions):
                    # Get PNG dimensions from S3 image info
                    png_dimensions = pdf_pages[page_number - 1][1] 
                    pdf_dimensions = pdf_page_dimensions[page_number - 1]
                    
                    normalized_bbox = convert_png_to_normalized_pdf_coordinates(
                        png_bounding_box, 
                        png_dimensions, 
                        pdf_dimensions
                    )
                    normalized_bbox['page'] = page_number
                else:
                    normalized_bbox = {}
            except Exception as e:
                logger.error(f"Error processing bounding box for entity '{entity_name}': {str(e)}")
                normalized_bbox = {}
            
            bounding_box_map[entity_name] = normalized_bbox
        
        # Merge bounding boxes with original entities
        entities_with_bounding_boxes = []
        for entity in original_entities:
            entity_name = entity['columnName']
            entity_value = entity['columnValue']
            entity_page = entity['pageNumber']
            
            # Handle UNKNOWN values - Use empty object instead of zeros
            if entity_value == 'N/A' or not entity_value:
                bounding_box = {}
            else:
                bounding_box = bounding_box_map.get(entity_name, {})
                bounding_box['page'] = entity_page
            
            entities_with_bounding_boxes.append({
                'columnName': entity['columnName'],
                'columnValue': entity['columnValue'],
                'confidenceScore': entity['confidenceScore'],
                'pageNumber': entity['pageNumber'],
                'displayName': entity['displayName'],
                'boundingBox': bounding_box
            })
        
        # Log token usage for bounding box extraction
        usage = model_response.get("usage", {})
        logger.info(f"Bounding box token usage - Input: {usage.get('inputTokens', 0)}, Output: {usage.get('outputTokens', 0)}")
        
        return entities_with_bounding_boxes
        
    except json.JSONDecodeError as json_error:
        logger.error(f"JSON parse error in bounding box response: {str(json_error)}")
        return add_empty_bounding_boxes(original_entities)
    except Exception as e:
        logger.error(f"Error processing bounding box response: {str(e)}")
        return add_empty_bounding_boxes(original_entities)


def add_empty_bounding_boxes(entities):
    """Add empty bounding boxes to entities when bounding box extraction fails"""
    return [dict(entity, boundingBox={}) for entity in entities]


@tracer.capture_method
def finalize_processing(context):
    """Create database records and handle batch completion"""
    try:
        # Create extracted documents record
        extracted_document_id = create_extracted_documents_record(
            context['extraction_payload'], 
            context['timestamp']
        )
        context['extracted_document_id'] = extracted_document_id
        
        # Update upload document record with confidence score
        upload_record_response = update_upload_document_record(
            context['documentUploadId'], 
            context['timestamp'],
            context.get('avgConfidenceScore')
        )
        
        # Check batch completion
        check_and_send_sqs_if_batch_complete(upload_record_response, context['timestamp'])
        
        return context
        
    except Exception as e:
        raise ProcessingError(f"Finalization failed: {str(e)}", 500, context)


def cleanup_temp_files(context):
    """Clean up temporary files"""
    try:
        if 'local_path' in context and os.path.exists(context['local_path']):
            os.remove(context['local_path'])
    except:
        pass


def create_success_response(context):
    """Create success response"""
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "File processed successfully" if context['entitiesCount'] > 0 else "No entities extracted",
            "merchantId": context['merchantId'],
            "documentUploadId": context['documentUploadId'],
            "fileName": context['fileName'],
            "documentType": context['documentType'],
            "entitiesCount": context['entitiesCount'],
            'failedExtractionCount': context['failedExtractionCount'],
            "bdaDuration": context['bdaDuration'],
            "extractionDuration": context['extractionDuration'],
            "detailedCsv": context.get('detailedCsv'),
            "summaryCsv": context.get('summaryCsv')
        }, cls=DecimalEncoder)
    }


def create_error_response(status_code, message, context):
    """Create error response"""
    return {
        "statusCode": status_code,
        "body": json.dumps({
            "error": message,
            "merchantId": context.get('merchantId'),
            "documentUploadId": context.get('documentUploadId')
        })
    }


# === HELPER FUNCTIONS ===

@tracer.capture_method
def get_configuration_from_filename(filename):
    """Infer document type from filename using FILENAME_LISTS configuration"""
    filename_lower = filename.lower()
    for doc_type, config in FILENAME_LISTS.items():
        for possible_filename in config['filenames']:
            if possible_filename.lower() in filename_lower:
                return {'documentType': config['documentType']}
    return None


@tracer.capture_method
def calculate_average_confidence_score(structured_data):
    """Calculate the average confidence score from all entities"""
    if not structured_data:
        return 0
    
    total_confidence = 0
    valid_entities = 0
    
    for entity in structured_data:
        confidence = entity.get('confidenceScore', 0)
        # Only include entities that have actual values (not UNKNOWN)
        if entity.get('columnValue') and entity.get('columnValue') != 'N/A':
            total_confidence += float(confidence) if confidence else 0
            valid_entities += 1
    
    if valid_entities == 0:
        return 0
    
    avg_confidence = total_confidence / valid_entities
    return round(avg_confidence, 2)


@tracer.capture_method
def extract_png_bytes_from_pdf(pdf_path, dpi=300):
    """
    Renders each page of the PDF to a lossless PNG image using PyMuPDF and returns PDF dimensions.
    
    Args:
        pdf_path: Path to the PDF file
        dpi: Resolution for rendering (default 300)
    """    
    png_bytes_list = []
    pdf_page_dimensions = []
    doc = pymupdf.open(pdf_path)
    
    try:
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            
            # Get original PDF page dimensions (in points)
            pdf_rect = page.rect
            pdf_width = pdf_rect.width
            pdf_height = pdf_rect.height
            pdf_page_dimensions.append((pdf_width, pdf_height))
            
            # Render at specified DPI
            zoom = dpi / 72.0
            mat = pymupdf.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat)
            
            # Convert directly to PNG bytes
            final_img_bytes = pix.tobytes("png")
            img_size = (pix.width, pix.height)
            
            logger.info(f'Page {page_num + 1}: {img_size}')
            
            png_bytes_list.append((final_img_bytes, img_size))
            
            # Cleanup
            del pix
        
        return png_bytes_list, pdf_page_dimensions
        
    finally:
        doc.close()


@tracer.capture_method
def create_dual_csv_outputs_from_dynamodb_data(structuredData, merchantId, batchName, source_filePath, timestamp):
    """Create detailed and summary CSV files from DynamoDB structured data"""
    if not structuredData:
        raise ValueError("No data to write to CSV")
    
    source_filename = source_filePath.split('/')[-1].replace('.pdf', '') if source_filePath else "extraction_result"
    
    detailedCsvOutput = create_detailedCsv_from_dynamodb_data(structuredData, merchantId, batchName, source_filename, timestamp)
    summaryCsvOutput = create_summaryCsv_from_dynamodb_data(structuredData, merchantId, batchName, source_filename, timestamp)
    
    return {
        "detailedCsv": detailedCsvOutput,
        "summaryCsv": summaryCsvOutput
    }


@tracer.capture_method
def create_detailedCsv_from_dynamodb_data(structuredData, merchantId, batchName, source_filename, timestamp):
    """Create the detailed CSV with all metadata from DynamoDB structured data"""
    csv_data = []
    for record in structuredData:
        csv_row = {
            'filename': source_filename,
            'timestamp': timestamp,
            'page_number': record.get('pageNumber', 1),
            'confidence': record.get('confidenceScore', 0),
            'column_name': record.get('columnName', ''),
            'display_name': record.get('displayName', ''),
            'column_value': record.get('columnValue', '')
        }
        csv_data.append(csv_row)
    
    headers = ['filename', 'timestamp', 'page_number', 'confidence', 'column_name', 'display_name', 'column_value']
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=headers, restval='')
    writer.writeheader()
    writer.writerows(csv_data)
    
    csv_filename = f"{source_filename}_detailed_{timestamp}.csv"
    output_key = f"csv_output/{merchantId}/{batchName}/detailed/{csv_filename}"
    
    S3_CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    return output_key


@tracer.capture_method
def create_summaryCsv_from_dynamodb_data(structuredData, merchantId, batchName, source_filename, timestamp):
    """Create a clean summary CSV with single row from DynamoDB structured data in field order"""
    summary_record = {}
    
    # Build summary record maintaining field order
    for field_name in FIELD_ORDER:
        # Find the entity data for this field
        for record in structuredData:
            if record.get('columnName') == field_name:
                display_name = record.get('displayName', record.get('columnName', ''))
                column_value = record.get('columnValue', '')
                summary_record[display_name] = column_value
                break
    
    # Use ordered headers (maintain the field order)
    headers = list(summary_record.keys())
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=headers, restval='')
    writer.writeheader()
    writer.writerow(summary_record)
    
    csv_filename = f"{source_filename}_summary_{timestamp}.csv"
    output_key = f"csv_output/{merchantId}/{batchName}/summary/{csv_filename}"
    
    S3_CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    return output_key


@tracer.capture_method
def create_extracted_documents_record(payload, timestamp):
    """Create a record in RHB-ExtractedDocuments and return the document ID"""
    extracted_document_id = str(uuid.uuid4())
    
    record_data = {
        'extractedLoanDocumentsId': extracted_document_id,
        'merchantId': payload.get('merchantId'),
        'documentUploadId': payload.get('documentUploadId'),
        'filePath': payload.get('filePath'),
        'data': payload.get('data', []),
        'fileName': payload.get('fileName'),
        'documentType': payload.get('documentType'),
        'subDocumentType': payload.get('subDocumentType'),
        'detailedCsv': payload.get('detailedCsv'),
        'summaryCsv': payload.get('summaryCsv'),
        'entitiesCount': payload.get('entitiesCount'),
        'failedExtractionCount': payload.get('failedExtractionCount', 0),
        'extractionDuration': payload.get('extractionDuration'),
        'bdaDuration': payload.get('bdaDuration', 0),
        'documentStatus': payload.get('documentStatus'),
        'boundingBoxStatus': payload.get('boundingBoxStatus', 'NOT_ATTEMPTED'),
        'boundingBoxDuration': payload.get('boundingBoxDuration', 0),
        'avgConfidenceScore': payload.get('avgConfidenceScore', 0),
        'markdownS3Key': payload.get('markdownS3Key'),
        'bboxS3Key': payload.get('bboxS3Key'),
        'createdAt': timestamp,
        'createdBy': 'system',
        'updatedAt': timestamp,
        'updatedBy': 'system'
    }
    
    EXTRACTED_DOCUMENTS_DDB_TABLE.put_item(Item=record_data)
    return extracted_document_id


@tracer.capture_method
def update_upload_document_record(documentUploadId, timestamp, avgConfidenceScore=None):
    """Update the upload document record with processed count and confidence score"""
    # Base update expression and values
    update_expression = "ADD processedCount :inc SET updatedAt = :now"
    expression_values = {':inc': 1, ':now': timestamp}
    
    # Add confidence score update if provided
    if avgConfidenceScore:
        update_expression += ", avgConfidenceScore = :avgConf"
        expression_values[':avgConf'] = avgConfidenceScore
    
    response = DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={'documentUploadId': documentUploadId},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
        ReturnValues="ALL_NEW"
    )
    return response


@tracer.capture_method
def check_and_send_sqs_if_batch_complete(upload_document_record, timestamp):
    """Check if processed count matches batch size and send SQS message if complete"""
    completed = upload_document_record['Attributes']['processedCount']
    expected = upload_document_record['Attributes']['batchSize']
    documentUploadId = upload_document_record['Attributes']['documentUploadId']
    
    if completed >= expected:
        response = DOCUMENT_UPLOAD_DDB_TABLE.update_item(
            Key={'documentUploadId': documentUploadId},
            UpdateExpression="SET updatedAt = :now, #status = :documentStatus",
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':now': timestamp, ':documentStatus': 'Mapping In Progress'},
            ReturnValues="ALL_NEW"
        )
        
        payload = {'documentUploadId': documentUploadId}
        response = SQS_CLIENT.send_message(
            QueueUrl=SQS_QUEUE,
            MessageBody=json.dumps(payload)
        )

        logger.info(f"Batch complete. SQS message sent for {documentUploadId}")
    else:
        logger.info(f"Batch progress: {completed}/{expected}")


def convert_png_to_normalized_pdf_coordinates(png_bbox, png_dimensions, pdf_dimensions):
    """Convert Nova Pro bounding box coordinates to normalized PDF coordinates"""
    # Handle invalid bounding box format
    if not isinstance(png_bbox, list) or len(png_bbox) != 4:
        logger.warning(f"Invalid bounding box format: {png_bbox}")
        return {}
    
    # Handle zero bounding box
    if png_bbox == [0, 0, 0, 0]:
        return {}
    
    try:
        x1, y1, x2, y2 = png_bbox

        # Ensure coordinates are in correct order
        x1, x2 = min(x1, x2), max(x1, x2)
        y1, y2 = min(y1, y2), max(y1, y2)
        
        # Normalize to 0-1 scale relative to the image
        left = (x1 / 1000.0)  
        top = (y1 / 1000.0)  
        width = ((x2 / 1000.0) - left) 
        height = ((y2 / 1000.0) - top) 
        
        # Ensure values are within valid range [0, 1]
        left = max(0, min(1, left))
        top = max(0, min(1, top))
        width = max(0, min(1, width))
        height = max(0, min(1, height))
        
        return {
            'top': round(top, 6),
            'left': round(left, 6),
            'width': round(width, 6),
            'height': round(height, 6)
        }
        
    except (ValueError, TypeError, ZeroDivisionError) as e:
        logger.error(f"Error converting Nova Pro coordinates: {e}. Bbox: {png_bbox}")
        return {}


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
def separate_data_by_mapping(structured_data):
    """
    Separate structured data into formData and tableData based on FORM_TABLE_DATA_MAPPING
    
    Args:
        structured_data: List of extracted entities
        
    Returns:
        dict: {'formData': [...], 'tableData': [...]}
    """
    form_data = []
    table_data = []
    
    logger.info(f"Separating {len(structured_data)} entities into formData and tableData")
    
    for entity in structured_data:
        column_name = entity.get('columnName', '')
        mapping_type = FORM_TABLE_DATA_MAPPING.get(column_name)
        
        if mapping_type == 'formData':
            form_data.append(entity)
        elif mapping_type == 'tableData':
            table_data.append(entity)
        else:
            # Default to formData if not found in mapping
            form_data.append(entity)
            logger.warning(f"Column '{column_name}' not found in FORM_TABLE_DATA_MAPPING, defaulting to formData")
    
    logger.info(f"Data separation complete - formData: {len(form_data)}, tableData: {len(table_data)}")
    
    return {
        'formData': form_data,
        'tableData': table_data
    }


def is_size_error(error_msg):
    """Check if error is related to size limits"""
    return "length less than or equal to 25000000" in error_msg or "Input is too long for requested model" in error_msg


@tracer.capture_method
def create_markdown_file(context):
    """Create and upload markdown file to S3"""
    try:
        source_filename = context['filePath'].split('/')[-1].replace('.pdf', '') if context['filePath'] else "extraction_result"
        md_filename = f"{source_filename}_{context['timestamp']}.md"
        md_key = f"markdown_output/{context['merchantId']}/{context['batchName']}/{md_filename}"
        
        S3_CLIENT.put_object(
            Bucket=S3_BUCKET,
            Key=md_key,
            Body=context['markdown_content'].encode('utf-8'),
            ContentType='text/markdown'
        )
        
        logger.info(f"Uploaded markdown to s3://{S3_BUCKET}/{md_key}")
        return md_key
        
    except Exception as e:
        logger.error(f"Failed to create markdown file: {str(e)}")
        return None


@tracer.capture_method
def create_json_file(context, separated_data):
    """Create and upload JSON file to S3"""
    try:
        source_filename = context['filePath'].split('/')[-1].replace('.pdf', '') if context['filePath'] else "extraction_result"
        json_filename = f"{source_filename}_{context['timestamp']}.json"
        json_key = f"json_output/{context['merchantId']}/{context['batchName']}/{json_filename}"
        
        json_content = {
            'fileName': context['fileName'],
            'documentType': context['documentType'],
            'subDocumentType': context['subDocumentType'],
            'extractionData': separated_data,
            'metadata': {
                'entitiesCount': context['entitiesCount'],
                'failedExtractionCount': context['failedExtractionCount'],
                'avgConfidenceScore': context.get('avgConfidenceScore', 0),
                'extractionDuration': context['extractionDuration'],
                'bdaDuration': context['bdaDuration'],
                'timestamp': context['timestamp']
            }
        }
        
        S3_CLIENT.put_object(
            Bucket=S3_BUCKET,
            Key=json_key,
            Body=json.dumps(json_content, cls=DecimalEncoder, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Uploaded JSON to s3://{S3_BUCKET}/{json_key}")
        return json_key
        
    except Exception as e:
        logger.error(f"Failed to create JSON file: {str(e)}")
        return None


@tracer.capture_method
def call_bedrock_with_fallback(context, prompt, mode):
    """Call Bedrock with fallback strategy: 300 DPI -> 200 DPI -> 150 DPI"""
    document_type = context.get('subDocumentType')
    
    strategies = [
        (300, context['pdf_pages'], "existing 300 DPI"),
        (200, None, "200 DPI"),
        (150, None, "150 DPI (last resort)")
    ]
    
    for dpi, pages, desc in strategies:
        logger.info(f"Attempting {mode} with {desc} byte data")
        
        try:
            # Extract pages if not provided
            if pages is None:
                pages, _ = extract_png_bytes_from_pdf(context['local_path'], dpi=dpi)
            
            bedrock_result = call_bedrock_converse(pages, prompt, mode=mode, document_type=document_type)
            
            if bedrock_result.get("success", False):
                logger.info(f"{dpi} DPI strategy succeeded for {mode}")
                # Update context with successful DPI data
                context['pdf_pages'] = pages
                return bedrock_result
            
            error_msg = bedrock_result.get('error', '')
            if is_size_error(error_msg) and dpi > 150:
                logger.info(f"{dpi} DPI data too large for {mode}, trying lower DPI")
            else:
                log_level = logger.error if dpi == 150 else logger.warning
                log_level(f"{dpi} DPI strategy failed for {mode}: {error_msg}")
                if dpi == 150:
                    return bedrock_result
                
        except Exception as e:
            log_level = logger.error if dpi == 150 else logger.warning
            log_level(f"{dpi} DPI strategy failed for {mode} with exception: {str(e)}")
            if dpi == 150:
                return {
                    "success": False,
                    "error": f"All fallback strategies failed for {mode}. Last error: {str(e)}",
                    "error_type": type(e).__name__
                }