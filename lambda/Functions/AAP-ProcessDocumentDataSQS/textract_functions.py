import boto3
from aws_lambda_powertools import Logger, Tracer

logger = Logger()
tracer = Tracer()

TEXTRACT_CLIENT = boto3.client('textract')

@tracer.capture_method
def textract_get_document_lines(bucket_name, object_key):
    response = TEXTRACT_CLIENT.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_key,
            }
        }
    )
    
    if 'Blocks' not in response:
        raise Exception("No text detected in the document")
    
    all_lines =[]
    bounding_boxes_data = []
    line_counter = 0
    
    for idx, block in enumerate(response['Blocks']):
        if block['BlockType'] == 'LINE':
            numbered_line = f"{line_counter}|{block['Text']}"
            all_lines.append(numbered_line)
            line_counter += 1

            #bounding boxes
            geometry = block['Geometry']
            bounding_box = geometry['BoundingBox']

            location_details = {
                'boundingBox': {
                    'width': bounding_box['Width'],
                    'height': bounding_box['Height'],
                    'left': bounding_box['Left'],
                    'top': bounding_box['Top']
                }
            }

            bounding_boxes_data.append(location_details)
        
    return all_lines, bounding_boxes_data
