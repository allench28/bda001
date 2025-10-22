import os
import boto3
import json
import time
import uuid
from aws_lambda_powertools import Logger, Tracer
import time

from bda_parser import BedrockOutputParser

S3_BDA_BUCKET = os.environ.get("S3_BDA_BUCKET", "aap-rhb-bda-processing-dev")

S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client("sqs", region_name='ap-southeast-1')
DDB_RESOURCE = boto3.resource('dynamodb')


# BDA Configuration
BDA_PROJECT_ARN = os.environ.get("BDA_PROJECT_ARN", "arn:aws:bedrock:us-east-1:582554346432:data-automation-project/0d32bd960c28")
BDA_PROFILE_ARN = os.environ.get("BDA_PROFILE_ARN", "arn:aws:bedrock:us-east-1:582554346432:data-automation-profile/us.data-automation-v1")
BDA_REGION = os.environ.get("BDA_REGION", "us-east-1")
BDA_MAX_WAIT_TIME = int(os.environ.get("BDA_MAX_WAIT_TIME", "300"))  # 5 minutes
BDA_POLL_INTERVAL = int(os.environ.get("BDA_POLL_INTERVAL", "10"))  # 10 seconds

BDA_CLIENT = boto3.client('bedrock-data-automation-runtime', region_name=BDA_REGION)


logger = Logger()
tracer = Tracer()


class ProcessingError(Exception):
    """Custom exception for processing errors with context"""
    def __init__(self, message, status_code=500, context=None):
        self.message = message
        self.status_code = status_code
        self.context = context or {}
        super().__init__(self.message)



@tracer.capture_method
def extract_markdown_from_bda_output(output_s3_prefix: str, original_pdf_key: str, job_metadata_uri: str = None) -> tuple[str, str, str]:
    """
    Extract markdown content from BDA output JSON file and save to structured location.
    
    Args:
        output_s3_prefix: S3 prefix where BDA outputs are stored
        original_pdf_key: Original PDF key to derive output filename
        job_metadata_uri: S3 URI to job_metadata.json from BDA status response
    
    Returns:
        Markdown content string
    
    Raises:
        ProcessingError: If extraction fails
    """
    try:
        result_json_s3_uri = None
        
        # Parse job_metadata.json to get the standard_output_path
        if job_metadata_uri:
            logger.info(f"Retrieving job metadata from: {job_metadata_uri}")
            
            # Extract bucket and key from S3 URI
            uri_parts = job_metadata_uri.replace('s3://', '').split('/', 1)
            metadata_bucket = uri_parts[0]
            metadata_key = uri_parts[1] if len(uri_parts) > 1 else ''
            
            # Download and parse job_metadata.json
            metadata_response = S3_CLIENT.get_object(Bucket=metadata_bucket, Key=metadata_key)
            metadata_content = metadata_response['Body'].read().decode('utf-8')
            job_metadata = json.loads(metadata_content)
            
            logger.info(f"Job metadata retrieved successfully")
            
            # Extract standard_output_path from job metadata
            output_metadata_list = job_metadata.get('output_metadata', [])
            if output_metadata_list:
                segment_metadata = output_metadata_list[0].get('segment_metadata', [])
                if segment_metadata:
                    result_json_s3_uri = segment_metadata[0].get('standard_output_path')
                    logger.info(f"Found standard_output_path: {result_json_s3_uri}")
        
        if not result_json_s3_uri:
            raise ProcessingError("Could not find standard_output_path in job metadata", 500)
        
        # Extract bucket and key from result.json S3 URI
        result_uri_parts = result_json_s3_uri.replace('s3://', '').split('/', 1)
        result_bucket = result_uri_parts[0]
        result_key = result_uri_parts[1] if len(result_uri_parts) > 1 else ''
        
        logger.info(f"Retrieving BDA result from: s3://{result_bucket}/{result_key}")
        
        # Download the result.json output
        response = S3_CLIENT.get_object(Bucket=result_bucket, Key=result_key)
        json_content = response['Body'].read().decode('utf-8')
        data = json.loads(json_content)
        
        # Parse using the BDA parser
        parser = BedrockOutputParser()
        content_list = parser.extract_markdown_content(data)
        bounding_boxes = parser.extract_bounding_boxes(data)
        
        if not content_list:
            raise ProcessingError("No content extracted from BDA output", 500)
        
        logger.info(f"Extracted {len(content_list)} content sections and {len(bounding_boxes)} bounding boxes")
        
        # Combine all markdown sections
        markdown_parts = []
        for context_info, text in content_list:
            if context_info.startswith("Page"):
                markdown_parts.append(text)
            else:
                markdown_parts.append(f"## {context_info}\n{text}")
        
        markdown_content = '\n\n---\n\n'.join(markdown_parts)
        
        # Save markdown and bounding box files to S3
        pdf_dir = '/'.join(original_pdf_key.split('/')[:-1])
        base_filename = original_pdf_key.split('/')[-1].replace('.pdf', '')
        
        markdown_s3_key = f"{pdf_dir}/bda_{base_filename}.md"
        bbox_s3_key = f"{pdf_dir}/bda_{base_filename}_bounding_box.json"
        
        # Save markdown file
        S3_CLIENT.put_object(
            Bucket=S3_BDA_BUCKET,
            Key=markdown_s3_key,
            Body=markdown_content.encode('utf-8'),
            ContentType='text/markdown'
        )
        logger.info(f"✓ Saved markdown to: s3://{S3_BDA_BUCKET}/{markdown_s3_key}")
        
        # Save bounding box JSON
        bbox_output = {
            'metadata': {
                'source_file': original_pdf_key,
                'bda_result_file': result_json_s3_uri,
                'bda_job_metadata': job_metadata_uri,
                'total_content_sections': len(content_list),
                'total_bounding_boxes': len(bounding_boxes),
                'total_pages': len(data.get('pages', [])),
                'total_elements': len(data.get('elements', []))
            },
            'bounding_boxes': bounding_boxes
        }
        
        S3_CLIENT.put_object(
            Bucket=S3_BDA_BUCKET,
            Key=bbox_s3_key,
            Body=json.dumps(bbox_output, indent=2, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"✓ Saved bounding boxes to: s3://{S3_BDA_BUCKET}/{bbox_s3_key}")
        
        return markdown_content, markdown_s3_key, bbox_s3_key
        
    except ProcessingError:
        raise
    except Exception as e:
        logger.error(f"Error extracting markdown from BDA output: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise ProcessingError(f"Failed to extract markdown from BDA output: {str(e)}", 500)



@tracer.capture_method
def wait_for_bda_completion(invocation_arn: str, output_s3_prefix: str, original_pdf_key: str) -> tuple[str, str, str]:
    """
    Poll BDA job status and retrieve markdown output when complete.
    
    Args:
        invocation_arn: The ARN of the BDA invocation
        output_s3_prefix: S3 prefix where BDA outputs are stored
        original_pdf_key: Original PDF S3 key
    
    Returns:
        Markdown content string
    
    Raises:
        ProcessingError: If BDA job fails or times out
    """
    start_time = time.time()
    
    while time.time() - start_time < BDA_MAX_WAIT_TIME:
        try:
            status_response = BDA_CLIENT.get_data_automation_status(invocationArn=invocation_arn)
            status = status_response.get('status')
            
            logger.info(f"BDA Status: {status}")
            
            if status == 'Success':
                # Get the job_metadata.json S3 URI from the status response
                job_metadata_uri = status_response.get('outputConfiguration', {}).get('s3Uri')
                logger.info(f"BDA job metadata URI: {job_metadata_uri}")
                
                # Parse the BDA output and extract markdown
                markdown_content, markdown_s3_key, bbox_s3_key = extract_markdown_from_bda_output(
                    output_s3_prefix, 
                    original_pdf_key, 
                    job_metadata_uri
                )
                
                if not markdown_content:
                    raise ProcessingError("Failed to extract markdown from BDA output", 500)
                
                return markdown_content, markdown_s3_key, bbox_s3_key
                
            elif status in ['Failed', 'Aborted']:
                raise ProcessingError(f"BDA processing failed with status: {status}", 500)
            
            # Status is still 'InProgress' or 'Pending'
            time.sleep(BDA_POLL_INTERVAL)
            
        except ProcessingError:
            raise
        except Exception as e:
            logger.error(f"Error checking BDA status: {str(e)}")
            raise ProcessingError(f"BDA status check failed: {str(e)}", 500)
    
    raise ProcessingError(f"BDA processing timed out after {BDA_MAX_WAIT_TIME} seconds", 500)


@tracer.capture_method
def process_pdf_with_bda(pdf_s3_key: str) -> tuple[str, str, str]:
    """
    Process PDF using Bedrock Data Automation and return markdown content.
    
    Args:
        pdf_s3_key: S3 key of the PDF file
    
    Returns:
        Markdown content string
    
    Raises:
        ProcessingError: If BDA processing fails
    """
    s3_url = f"s3://{S3_BDA_BUCKET}/{pdf_s3_key}"
    output_s3_prefix = pdf_s3_key.rsplit('/', 1)[0] + '/bda_output/'
    output_s3_uri = f"s3://{S3_BDA_BUCKET}/{output_s3_prefix}"
    
    logger.info(f"Starting BDA processing for: {s3_url}")
    
    try:
        # Invoke Bedrock Data Automation
        response = BDA_CLIENT.invoke_data_automation_async(
            dataAutomationConfiguration={
                'dataAutomationProjectArn': BDA_PROJECT_ARN,
                'stage': 'LIVE',
            },
            dataAutomationProfileArn=BDA_PROFILE_ARN,
            inputConfiguration={'s3Uri': s3_url},
            outputConfiguration={'s3Uri': output_s3_uri},
            clientToken=str(uuid.uuid4()),
        )
        
        invocation_arn = response['invocationArn']
        logger.info(f"BDA invocation started: {invocation_arn}")
        
        # Poll for completion and get markdown
        markdown_content, markdown_s3_key, bbox_s3_key = wait_for_bda_completion(invocation_arn, output_s3_prefix, pdf_s3_key)
        
        if not markdown_content:
            raise ProcessingError("BDA processing completed but no markdown content was extracted", 500)
        
        return markdown_content, markdown_s3_key, bbox_s3_key
        
    except ProcessingError:
        raise
    except Exception as e:
        logger.error(f"Error processing PDF with BDA: {str(e)}")
        raise ProcessingError(f"BDA processing failed: {str(e)}", 500)

