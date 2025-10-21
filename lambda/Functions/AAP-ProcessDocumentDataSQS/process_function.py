import re
from decimal import Decimal
from aws_lambda_powertools import Logger, Tracer
import json

# TODO: standardise a document data schema later

logger = Logger()
tracer = Tracer()

COMPRESSED_DATE_PATTERN = r'^\d{8}$'
MISSING_VALUE = "UNKNOWN"

@tracer.capture_method
def post_process_document_data(document_data_list, bounding_boxes_data):
    document_data = {
        "formData": [],
        "tableData": []
    }

    for field in document_data_list:
        logger.info(f"FIELD: {field}")
        confidence_score = field.get("confidenceScore", 0)
        field['confidenceScore'] = f"{confidence_score}%"

        file_line_index = field.get('fileLineIndex', '')
        indexes = parse_string_array(file_line_index, True)
        field['locationDetails'] = bounding_box_mapping(indexes, bounding_boxes_data)

        if field.get("fieldType") == "form":
            field['fieldValue'] = process_value(field.get("fieldValue", ""))
            document_data["formData"].append(field)
        elif field.get("fieldType") == "table":
            logger.info(f"TABLE FIELDVALUE: {field.get('fieldValue')}")
            field_values = parse_string_array(field.get("fieldValue", ""))
            processed_values = [process_value(val) for val in field_values]
            field['fieldValue'] = processed_values
            logger.info(f"FORM FIELDVALUE: {field['fieldValue']}")
            document_data["tableData"].append(field)

        logger.info(f'FORM: {document_data['formData']}')
        logger.info(f'TABLE: {document_data['tableData']}')
            
    is_completed = check_document_completion(document_data)
    return document_data, is_completed


def process_value(value):
    # custom value processing logic can be added here
    if not value:
        value = MISSING_VALUE
    elif value == 'M':
        value = "MALE"
    elif value == "F":
        value = "FEMALE"
    elif value == "Y":
        value = "YES"
    elif value == "N":
        value = "NO"
    value = value.upper()
    return value

def check_document_completion(document_data):
    form_data = document_data.get("formData")
    table_data = document_data.get("tableData")
    
    for form_field in form_data:
        if not form_field.get("fieldValue") or form_field.get("fieldValue") == MISSING_VALUE:
            return False

    for table_field in table_data:
        for table_value in table_field.get("fieldValue"):
            if not table_value or table_value == MISSING_VALUE:
                return False
    
    longest_value_count = 0
    for table_field in table_data:
        longest_value_count = max(longest_value_count, len(table_field.get("fieldValue")))
    for table_field in table_data:
        if len(table_field.get("fieldValue")) != longest_value_count:
            return False
    
    return True


def parse_string_array(array_string, to_int=False):
    value = []
    
    # Handle None or empty values
    if not array_string:
        return value
        
    if isinstance(array_string, str) and array_string.startswith('[') and array_string.endswith(']'):
        try:
            cleaned_string = array_string[1:-1].strip()
            
            if not cleaned_string:
                return value
                
            value = [item.strip() for item in cleaned_string.split(',')]
            
            if to_int:
                converted_values = []
                for item in value:
                    try:
                        if item and item.strip():  # Check for empty strings
                            converted_values.append(int(item))
                    except ValueError:
                        logger.warning(f"Skipping non-integer value: '{item}'")
                value = converted_values
        except Exception as e:
            logger.warning(f"Failed to parse array string: {array_string}, error: {str(e)}")
    else:
        value.append(array_string)
    return value


def bounding_box_mapping(indexes, bounding_boxes_data):
    try:        
        location_details = []
        
        if not indexes or not bounding_boxes_data:
            return location_details
            
        for index in indexes:            
            if index is None:
                continue
                
            try:
                if index >= len(bounding_boxes_data):
                    continue
                    
                box_data = bounding_boxes_data[index]
                
                if not isinstance(box_data, dict) or 'boundingBox' not in box_data:
                    logger.warning(f"Invalid box_data structure at index {index}")
                    continue
                
                decimal_box = {
                    'boundingBox': {
                        'width': Decimal(str(box_data['boundingBox']['width'])),
                        'height': Decimal(str(box_data['boundingBox']['height'])),
                        'left': Decimal(str(box_data['boundingBox']['left'])),
                        'top': Decimal(str(box_data['boundingBox']['top']))
                    }
                }
                
                location_details.append(decimal_box)
            except (KeyError, TypeError) as e:
                logger.warning(f"Error processing box data at index {index}: {str(e)}")
                continue

        return location_details
    except Exception as e:
        logger.error(f'Error in bounding_box_mapping: {str(e)}')
        # Return empty result instead of raising exception to avoid breaking the process
        return []