import boto3
import os
from aws_lambda_powertools import Logger, Tracer
from constants import validate_file_extension
from datetime import datetime
from botocore.exceptions import ClientError

logger = Logger()
tracer = Tracer()

s3 = boto3.client('s3')


@tracer.capture_method
def get_s3_object_binary(bucket_name, object_key):
    # Get S3 object
    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # Read binary content
    binary_content = response['Body'].read()

    return binary_content


@tracer.capture_method
def get_valid_file_extension(object_key):
    file_extension = object_key.lower().split('.')[-1] if '.' in object_key else ''
    validate_file_extension(file_extension)
    return file_extension


@tracer.capture_method
def get_archive_path(merchantId):
    today = datetime.now()
    archive_path = f"archive/{merchantId}/year={today.year}/month={today.month:02d}/day={today.day:02d}/"

    return archive_path


@tracer.capture_method
def move_s3_object(bucket_name, object_key, new_key):
    if not s3_file_exists(bucket_name, object_key): return
    s3.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': object_key},
        Key=new_key
    )
    s3.delete_object(Bucket=bucket_name, Key=object_key)
    logger.info(f"Document archived successfully to {new_key}")

@tracer.capture_method
def delete_s3_folder(bucket_name, folder_path):
    objects_to_delete = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    
    if 'Contents' in objects_to_delete:
        # Collect all object keys in the folder
        keys = [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]
        
        # Delete all objects in the folder
        s3.delete_objects(Bucket=bucket_name, Delete={'Objects': keys})
        logger.info(f"Deleted all objects in {folder_path}")
    else:
        logger.info("Folder {folder_path} is empty or does not exist.")
        
@tracer.capture_method
def s3_file_exists(bucket_name, s3_key):
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        return True  # File exists
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info(f"File {s3_key} does not exist in bucket {bucket_name}")
            return False  
        else:
            raise

@tracer.capture_method
def archive_documents(bucket_name, inputS3Path, merchantId, inputSourceS3Path=None):
    archive_path = get_archive_path(merchantId)
    
    filename = os.path.basename(inputS3Path)
    new_key = archive_path + filename
    move_s3_object(bucket_name, inputS3Path, new_key)
    
    if inputSourceS3Path and inputSourceS3Path != inputS3Path:
        filename = os.path.basename(inputSourceS3Path)
        new_key = archive_path + filename
        move_s3_object(bucket_name, inputSourceS3Path, new_key)
    
    directory = inputS3Path.split("/")[1]
    logger.warning(f"DIRECTORY: {directory}")
    delete_s3_folder(bucket_name, directory)

    return new_key