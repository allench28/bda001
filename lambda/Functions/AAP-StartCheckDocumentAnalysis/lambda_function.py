import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
import urllib
from datetime import datetime, timezone
import pymupdf
from PIL import Image, ImageOps
import io

INPUT_FOLDER = os.environ.get('INPUT_FOLDER')
OUTPUT_FOLDER = os.environ.get('OUTPUT_FOLDER')
EXTRACTED_DOCUMENT_TABLE = os.environ.get('EXTRACTED_DOCUMENT_TABLE')
EXTRACTED_DOCUMENT_PRIMARY_KEY = os.environ.get('EXTRACTED_DOCUMENT_PRIMARY_KEY')
PROCESS_DOCUMENT_DATA_LAMBDA = os.environ.get('PROCESS_DOCUMENT_DATA_LAMBDA')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

DDB_RESOURCE = boto3.resource('dynamodb')
LAMBDA_CLIENT = boto3.client('lambda')
S3_CLIENT = boto3.client('s3')
EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_method
def processImage(objectKey, filename):
    logger.info("processing image")
    response = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=objectKey)
    fileExtension = get_file_extension(filename)
    if 'Body' in response and response['Body'] is not None:
        image_data = response['Body'].read()
        # Open image while preserving orientation
        image = Image.open(io.BytesIO(image_data))
        
        # Log original dimensions and EXIF
        width, height = image.size
        logger.info(f"Original image dimensions - Width: {width}, Height: {height}")
        
        try:
            exif = image.getexif()
            if exif is not None:
                orientation = exif.get(274)  # 274 is the orientation tag
                logger.info(f"Original orientation: {orientation}")
                logger.info(f"Original exif: {exif}")
        except Exception as e:
            logger.warning(f"Error reading EXIF: {str(e)}")
            
        logger.info(f"Original image format: {fileExtension}")
        
        if fileExtension.lower() == '.jpg':
            logger.info("Converting JPG to JPEG")
            img_byte_arr = io.BytesIO()
            # Save with original EXIF to preserve orientation
            save_kwargs = {'format': 'JPEG', 'quality': 85}
            exif_data = image.info.get('exif')
            if exif_data is not None:
                save_kwargs['exif'] = exif_data
            image.save(img_byte_arr, **save_kwargs)
            image_bytes = img_byte_arr.getvalue()
            logger.info(f"Converted image size: {len(image_bytes)} bytes")
            upload_processed_files(image_bytes, objectKey, ".jpeg")
            return False
        
        elif fileExtension.lower() == '.png':
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            size_in_mb = len(img_byte_arr.getvalue()) / (1024 * 1024)
            
            if size_in_mb > 4.5:
                logger.info(f"PNG file size ({size_in_mb}MB) exceeds 4.5MB, converting to JPEG")
                img_byte_arr = io.BytesIO()
                if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
                    background = Image.new('RGB', image.size, (255, 255, 255))
                    background.paste(image, mask=image.split()[-1] if image.mode == 'RGBA' else None)
                    image = background
                # Save with original EXIF data if it exists
                save_kwargs = {'format': 'JPEG', 'quality': 85, 'optimize': True}
                exif_data = image.info.get('exif')
                if exif_data is not None:
                    save_kwargs['exif'] = exif_data
                image.save(img_byte_arr, **save_kwargs)
                upload_processed_files(img_byte_arr.getvalue(), objectKey, ".jpeg")
                return False
            else:
                return True
        
        return True
    else:
        raise Exception(f"Error reading image file: {objectKey}")
        
@tracer.capture_method  
def createImageFromPdf(objectKey):
    logger.info("converting pdf to image")
    response = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=objectKey)
    logger.info(f"pdf file get response: {response}")
    
    if 'Body' in response and response['Body'] is not None:
        pdfFile = response['Body'].read()
        # Open PDF file using PyMuPDF
        doc = pymupdf.open(stream=pdfFile, filetype="pdf")
        
        # Get the first page
        page = doc[0]
        
        # Log original PDF dimensions
        logger.info(f"Original PDF dimensions - Width: {page.rect.width}, Height: {page.rect.height}")
        
        # Get original page rotation
        orig_rotation = page.rotation
        logger.info(f"Original PDF rotation: {orig_rotation}")
        
        # Create matrix with scale and preserve original rotation
        matrix = pymupdf.Matrix(2.0, 2.0)
        
        # Get pixmap preserving original rotation
        pix = page.get_pixmap(matrix=matrix)
        
        # Convert to PIL Image
        img_data = pix.samples
        img = Image.frombytes("RGB", [pix.width, pix.height], img_data)
        
        # Create EXIF data with original rotation if needed
        exif_bytes = None
        if orig_rotation == 90:
            exif_dict = Image.Exif()
            exif_dict[274] = 6  # 90 degrees clockwise
            exif_bytes = exif_dict.tobytes()
        elif orig_rotation == 180:
            exif_dict = Image.Exif()
            exif_dict[274] = 3  # 180 degrees
            exif_bytes = exif_dict.tobytes()
        elif orig_rotation == 270:
            exif_dict = Image.Exif()
            exif_dict[274] = 8  # 270 degrees clockwise
            exif_bytes = exif_dict.tobytes()
        
        # Convert to bytes preserving orientation
        img_byte_arr = io.BytesIO()
        save_kwargs = {'format': 'JPEG', 'quality': 85}
        # Only include exif if it exists
        if exif_bytes is not None:
            save_kwargs['exif'] = exif_bytes
        img.save(img_byte_arr, **save_kwargs)
        img_bytes = img_byte_arr.getvalue()
        
        logger.info(f"pdf to img_bytes completed")
        return upload_processed_files(img_bytes, objectKey, ".jpeg")
    else:
        raise Exception(f"Error reading PDF file: {objectKey}")

@tracer.capture_method  
def convertTiffToImage(objectKey):
    logger.info("converting tiff to image")
    response = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=objectKey)
    
    if 'Body' in response and response['Body'] is not None:
        tiffFile = response['Body'].read()
        # Open TIFF using PIL
        tiff_image = Image.open(io.BytesIO(tiffFile))
        
        # Log original TIFF dimensions
        width, height = tiff_image.size
        logger.info(f"Original TIFF dimensions - Width: {width}, Height: {height}")
        
        # Get TIFF metadata and EXIF
        try:
            exif = tiff_image.getexif()
            if exif is not None:
                orientation = exif.get(274)
                logger.info(f"Original TIFF orientation: {orientation}")
                logger.info(f"Original TIFF exif: {exif}")
        except Exception as e:
            logger.warning(f"Error reading TIFF EXIF: {str(e)}")
        
        # Convert to bytes maintaining orientation
        img_byte_arr = io.BytesIO()
        save_kwargs = {'format': 'JPEG', 'quality': 85}
        exif_data = tiff_image.info.get('exif')
        if exif_data is not None:
            save_kwargs['exif'] = exif_data
        tiff_image.save(img_byte_arr, **save_kwargs)
        img_bytes = img_byte_arr.getvalue()
        
        logger.info(f"tiff to jpeg conversion completed")
        return upload_processed_files(img_bytes, objectKey, ".jpeg")        
    else:
        raise Exception(f"Error reading TIFF file: {objectKey}")

@tracer.capture_method   
def upload_processed_files(data, objectKey, extension):
    # Log dimensions and orientation of the converted image before uploading
    try:
        image = Image.open(io.BytesIO(data))
        width, height = image.size
        logger.info(f"Converted image dimensions before S3 upload - Width: {width}, Height: {height}")
        
        exif = image.getexif()
        if exif is not None:
            orientation = exif.get(274)
            logger.info(f"Converted image orientation before S3 upload: {orientation}")
    except Exception as e:
        logger.warning(f"Failed to get converted image details: {str(e)}")
    
    filePaths = objectKey.split("/")
    fileName = filePaths[-1]
    baseFileName = os.path.splitext(fileName)[0]
    newFileName = baseFileName + extension
    filePaths[-1] = newFileName
    newKey = '/'.join(filePaths)
    newKey = newKey.replace(INPUT_FOLDER, OUTPUT_FOLDER)
    logger.info(f"newKey: {newKey}")
    
    S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=newKey, Body=data)
    return newKey

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    logger.info(f"event: {event}")
    try:
        for record in event.get('Records'):
            body = record.get('s3')
            objectKey = body.get('object').get('key')
            objectKey = urllib.parse.unquote_plus(objectKey)
            filePaths = objectKey.split("/")
            fileName = filePaths[-1]
            documentId = filePaths[-2]
            merchantId = filePaths[-3]
            logger.info(f"fileName + documentId: {fileName}+{documentId}")
            # +"-"+filePaths[-2]+"-"+filePaths[-1]
            outputS3Path = objectKey
        
            # need to change 3/4
            if len(filePaths) != 4:
                raise Exception(f"Incorrect file path for document: {objectKey}") 
            elif not objectKey.startswith(INPUT_FOLDER):
                raise Exception(f"document is not inside {INPUT_FOLDER} folder: {objectKey}") 
            elif (checkFileType(fileName) == 'UNKNOWN'):
                raise Exception(f"Unsupported file type: {fileName}")
            
            logger.info(f"fileName: {fileName}")
            
            fileType = checkFileType(fileName)
            logger.info(f"fileType: {fileType}")
            
            if fileType == 'PDF':
                outputS3Path = createImageFromPdf(objectKey)
                createNewDdbEntry(
                    documentId,
                    fileName, 
                    None, 
                    None,
                    merchantId,
                    objectKey,
                ) 
                return "documents type has been converted from pdf"
            elif fileType == 'TIFF':
                outputS3Path = convertTiffToImage(objectKey)
                createNewDdbEntry(
                    documentId, 
                    fileName, 
                    None, 
                    None,
                    merchantId,
                    objectKey,
                ) 
                return "documents type has been converted from tiff"
            elif fileType == 'IMAGE':
                can_be_used = processImage(objectKey, fileName)
                extractedDocumentPayload = createNewDdbEntry(
                    documentId, 
                    fileName, 
                    objectKey if can_be_used else None, 
                    outputS3Path if can_be_used else None,
                    merchantId,
                    objectKey if not can_be_used else None,
                ) 
                if can_be_used:
                    # Log dimensions before invoking next lambda
                    try:
                        response = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=objectKey)
                        image_data = response['Body'].read()
                        image = Image.open(io.BytesIO(image_data))
                        width, height = image.size
                        logger.info(f"Image dimensions before invoking lambda - Width: {width}, Height: {height}")
                    except Exception as e:
                        logger.warning(f"Failed to get image dimensions: {str(e)}")
                        
                    LAMBDA_CLIENT.invoke(
                        FunctionName=PROCESS_DOCUMENT_DATA_LAMBDA,
                        InvocationType='Event',
                        Payload=json.dumps(extractedDocumentPayload)
                    )
                return "documents are successfully handled"
    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return "The server encountered an unexpected condition that prevented it from fulfilling your request."

@tracer.capture_method
def createNewDdbEntry(id, fileName, inputS3Path, outputS3Path, merchantId, inputSourceS3Path=None):
    current_datetime = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    
    response = EXTRACTED_DOCUMENT_DDB_TABLE.get_item(
        Key={
            EXTRACTED_DOCUMENT_PRIMARY_KEY: id
        }
    )
    logger.info(f"doc file name: {fileName}")
        
    if 'Item' in response:
        logger.info(f"Found existing item for id: {id}")
        extractedDocumentPayload = response['Item']
        extractedDocumentPayload['updatedAt'] = current_datetime
        extractedDocumentPayload['updatedBy'] = 'System'
        extractedDocumentPayload['inputS3Path'] = inputS3Path
        extractedDocumentPayload['outputS3Path'] = outputS3Path
        
        if inputSourceS3Path is not None:
            extractedDocumentPayload['inputSourceS3Path'] = inputSourceS3Path
        logger.info(f"existing doc file name: {extractedDocumentPayload['inputFileName']} {extractedDocumentPayload['inputFileName'] is not None} ")
    else:
        logger.info(f"Creating new item for id: {id}")
        extractedDocumentPayload = {
            EXTRACTED_DOCUMENT_PRIMARY_KEY: id,
            'inputFileName': fileName,
            'inputS3Path': inputS3Path,
            'outputS3Path': outputS3Path,
            'conversionStatus': False,
            'documentStatus': 'IN_PROGRESS',
            'createdAt': current_datetime,
            'createdBy': 'System',
            'updatedAt': current_datetime,
            'updatedBy': 'System',
            'merchantId': merchantId
        }
        if inputSourceS3Path is not None:
            extractedDocumentPayload['inputSourceS3Path'] = inputSourceS3Path
    
    logger.info(f"Putting item in DDB: {extractedDocumentPayload}")
    EXTRACTED_DOCUMENT_DDB_TABLE.put_item(Item=extractedDocumentPayload)
    return extractedDocumentPayload

@tracer.capture_method
def get_file_extension(filename): 
    _, file_extension = os.path.splitext(filename)
    return file_extension

@tracer.capture_method
def checkFileType(filename):
    fileExtension = get_file_extension(filename)
    
    print(fileExtension)
    if fileExtension.lower() == '.pdf':
        return 'PDF'
    elif fileExtension.lower() in ['.jpg', '.jpeg', '.png']:
        return 'IMAGE'
    elif fileExtension.lower() in ['.tif', '.tiff']:
        return 'TIFF'
    else:
        return 'UNKNOWN'