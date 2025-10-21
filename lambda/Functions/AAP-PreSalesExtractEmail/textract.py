import os
import re
import urllib.parse
import uuid
import boto3
import json
from aws_lambda_powertools import Logger, Tracer

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
SNS_ROLE_ARN = os.environ.get("SNS_ROLE_ARN")
ROOT_OUTPUT_PREFIX = os.environ.get("ROOT_OUTPUT_PREFIX")

S3_CLIENT = boto3.client("s3")
TEXTRACT_CLIENT = boto3.client("textract")

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # Get the key and make sure it's properly URL-decoded
        original_filepath = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"]
        )
        bucket = event["Records"][0]["s3"]["bucket"]["name"]

        merchant_id = original_filepath.split("/")[1]
        document_upload_id = original_filepath.split("/")[-2]
        output_prefix = f"{ROOT_OUTPUT_PREFIX}/{merchant_id}/{document_upload_id}"

        logger.info(f"Processing file: {original_filepath}")

        # Start Textract extraction directly on the original file
        response = start_textract_extraction(
            bucket,
            original_filepath,
            document_upload_id,
            SNS_TOPIC_ARN,
            SNS_ROLE_ARN,
            output_prefix,
        )

        job_id = response.get("JobId")
        logger.info(f"Textract job started with JobId: {job_id}")

    except Exception as e:
        logger.exception({"message": str(e)})
        raise  # Re-raise to ensure AWS Lambda sees the error


@tracer.capture_method
def start_textract_extraction(
    bucket, filepath, tag, sns_topic, sns_role, output_prefix
):
    logger.info(f"Beginning start_document_text_detection for JobTag: {tag}")
    response = TEXTRACT_CLIENT.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": filepath}},
        JobTag=tag,
        NotificationChannel={
            "RoleArn": sns_role,
            "SNSTopicArn": sns_topic,
        },
        OutputConfig={"S3Bucket": bucket, "S3Prefix": output_prefix},
    )
    logger.info(response)

    return response
