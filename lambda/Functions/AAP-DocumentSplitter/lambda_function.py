import os
import json
import boto3
import io
from typing import Dict, List, Any
from aws_lambda_powertools import Logger, Tracer
from PyPDF2 import PdfReader, PdfWriter

# Get environment variables
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "input")
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET")

# Configure AWS clients
s3_client = boto3.client("s3")


logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """
    Lambda handler that processes a document classification result and splits
    the original PDF into multiple files based on document types and page ranges.
    Uses streaming to avoid downloading to the filesystem.

    Expected event format:
    {
        "classification": {...},
        "original_filepath": "multi-input/multi-document.pdf",
        "job_id": "job123"
    }
    """
    try:
        logger.info(f"Received event: {event}")

        # Extract information from event
        classification = event.get("classification", {})
        job_id = event.get("job_id")

        # Handle the nested original_file_metadata structure
        original_document_location = event.get("original_document_location")
        original_bucket = original_document_location.get("S3Bucket")
        original_filepath = original_document_location.get("S3ObjectName")

        merchant_id = original_filepath.split("/")[1]
        document_upload_id = original_filepath.split("/")[2]

        if not original_bucket or not original_filepath:
            raise ValueError("Missing original document location in event")

        if not classification:
            raise ValueError("Missing classification in event")

        # Stream the original PDF from S3
        logger.info(f"Streaming original PDF from {original_document_location}")
        response = s3_client.get_object(Bucket=original_bucket, Key=original_filepath)
        pdf_stream = io.BytesIO(response["Body"].read())

        # Create a PDF reader from the stream
        reader = PdfReader(pdf_stream)
        logger.info(f"Original PDF has {len(reader.pages)} pages")

        # Process and split the PDF by document types
        document_types = ["invoice", "purchase_order", "goods_received", "unclassified"]
        for doc_type in document_types:
            if doc_type not in classification:
                continue

            doc_instances = classification[doc_type]

            # Handle case where unclassified is just a list of page numbers, not objects
            if isinstance(doc_instances, list) and all(
                isinstance(x, int) for x in doc_instances
            ):
                if doc_instances:  # Only process if there are unclassified pages
                    doc_instances = [
                        {"pages": doc_instances, "identifier": "Unclassified"}
                    ]
                else:
                    continue

            # Process each instance of this document type
            for i, instance in enumerate(doc_instances):
                # Get the pages for this document instance
                pages = instance.get("pages", [])
                identifier = instance.get("identifier", f"{doc_type}_{i+1}")

                if not pages:
                    logger.warning(f"No pages found for {doc_type} instance {i+1}")
                    continue

                logger.info(f"Processing {doc_type} {i+1} with pages {pages}")

                # Create a new PDF with just these pages
                writer = PdfWriter()

                # Add each specified page to the new PDF
                # Adjust for 0-based indexing in PyPDF2
                for page_num in pages:
                    # PDF page numbers are 1-based, but PyPDF2 uses 0-based indexing
                    pdf_page_index = page_num - 1

                    if 0 <= pdf_page_index < len(reader.pages):
                        writer.add_page(reader.pages[pdf_page_index])
                    else:
                        logger.warning(
                            f"Page {page_num} is out of range (PDF has {len(reader.pages)} pages)"
                        )

                # Write the new PDF to a stream
                output_stream = io.BytesIO()
                writer.write(output_stream)
                output_stream.seek(0)

                # Upload the split PDF to S3
                # Use the index (i+1) as the filename instead of the identifier
                output_key = f"{OUTPUT_PREFIX}/{merchant_id}/{document_upload_id}/{doc_type}_{i+1}.pdf"

                logger.info(f"Uploading split PDF to {original_bucket}/{output_key}")
                s3_client.upload_fileobj(output_stream, original_bucket, output_key)

    except Exception as e:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(e))
        logger.exception({"message": str(e)})
