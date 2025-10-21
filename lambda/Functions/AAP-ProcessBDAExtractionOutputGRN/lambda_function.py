import os
import boto3
import json
import time
import pandas as pd
from decimal import Decimal
import io
from aws_lambda_powertools import Logger, Tracer
from botocore.exceptions import NoCredentialsError, ClientError
import csv
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import uuid
from bedrock_function import promptBedrock
import re

AGENT_MAPPING_BUCKET = os.environ.get('AGENT_MAPPING_BUCKET')
DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
EXTRACTED_GRN_TABLE = os.environ.get('EXTRACTED_GRN_TABLE')
EXTRACTED_GRN_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_GRN_LINE_ITEM_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
BDA_PROCESSING_BUCKET = os.environ.get('BDA_PROCESSING_BUCKET')


S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_GRN_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_TABLE)
EXTRACTED_GRN_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_GRN_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)

logger = Logger()
tracer = Tracer()

## This function processes the extracted data from Bedrock and generates a combined CSV file
## The extracted data is stored in JSON format in S3, and the CSV file is also stored in S3

""""
Required fields from the extracted data to store in ExtractedDocument table.
1. GRN Number*
2. GRN Date
3. GRN Type (PO or non-PO GRN)
4. Supplier Name* 
5. Supplier Address
6. Buyer Name
7. Buyer Address
8. PO Number* 
9. Payment Terms
10. Currency*
11. Total GRN Amount*
12. Tax Details (SST/VAT/GST)
13. Tax Amount*
14. Line Item Details* (Item Code, Description, Unit Price, UOM, Quantity, Total Price)
15. Payment Due Date

Fields with * will be used to perform duplicate checking

Enhancement required:
1. (DONE) Perform mapping according to latest required fields. 
2. (DONE) Perform master data checking - Retrieve mapping file from S3 bucket and parse into text, fed into bedrock along side the extracted values and prompt included instructions to map. If unable to map then '-' is returned.
3. (DONE) Perform duplication checking based on required fields - Duplicate checking with ExtractedDocument table.
4. (DONE) Update relevant DynamoDB tables
- Timeline (Audit Log to inform that the grn is succcessfully extracted and processed)
- ExtractedDocument (GRN level information of the extracted document)
- DocumentUpload (File level information of the document). One DocumentUpload table record could have multiple ExtractedDocument records.
5. (TESTING) Folders for the CSV files in S3 bucket - Create folders based on merchantId/documentUploadId/date
"""

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        day = datetime.now().strftime('%Y_%m_%d')
        for record in event.get('Records', []):
            body = json.loads(record.get('body', '{}'))
            invocation_id = body.get('invocation_id')
            result_json_list = body.get('result_json_list', [])
            source_file_name = body.get('source_file_name')
            merchantId = body.get('merchant_id')
            documentUploadId = body.get('document_upload_id')
            file_path = body.get('file_path')

            updatedMappedJsonData = []
            
            if not result_json_list:
                continue
            
            if source_file_name.rsplit('_', 1)[0] != 'goods_received':
                unsupportExtractedDocumentTypePayload = {
                    "grnNumber": "-",
                    "grnDate": "-",
                    "documentType": "grn",
                    "poNumber": "-",
                    "supplierName": "-",
                    "supplierCode": "-",
                    "totalAmount": 0,
                    'boundingBoxes': "{}",
                    "status": "Exceptions",
                    "exceptionStatus": "Document Format Unrecognized",
                    "statusOfGoodsReceived": "-",
                    "createdAt": now,
                    "createdBy": "System",
                    "updatedAt": now,
                    "updatedBy": "System",
                    "sourceFile": source_file_name,
                    "confidenceScore": 0,
                    "documentUploadId": documentUploadId
                }
                createExtractedResultRecord(unsupportExtractedDocumentTypePayload, merchantId, documentUploadId, source_file_name, file_path, now)
                # updateDocumentUploadStatus(documentUploadId, [unsupportDocumentTypePayload], now)
                updatedMappedJsonData.append(unsupportExtractedDocumentTypePayload)
                updateFailedDocumentUploadStatus(documentUploadId, "Document Format Unrecognized")
                createTimelineRecord(merchantId, unsupportExtractedDocumentTypePayload, now)
                continue

            # generate_combined_csv(result_json_list, source_file_name, invocation_id, now, day)
            
            ## Perform initial mapping on the JSON file according to the required fields
            mappedJsonData = processJsonResult(result_json_list)

            for grn in mappedJsonData:
                logger.info(f'GRN: {grn}')

                ## Perform master data checking
                grn = performMasterDataChecking(grn, merchantId)
                logger.info(f'GRN AFTER MASTER DATA CHECKING: {grn}')

                ## Perform duplication checking
                grn = performDuplicateChecking(grn, merchantId)
                logger.info(f'GRN AFTER DUPLICATE CHECKING: {grn}')

                exception_details, input_tokens, output_tokens = performExceptionChecking(grn)
                grn['exceptionStatus'] = exception_details.get("exceptionStatus")
                grn['status'] = exception_details.get("status")
                logger.info(f'EXCEPTION DETAILS: {exception_details}')

                ## Create the ExtractedDocument & ExtractedDocumentLineItem table records
                grn = createExtractedResultRecord(grn, merchantId, documentUploadId, source_file_name, file_path, now)
                logger.info(f'GRN AFTER CREATE EXTRACTED RESULT: {grn}')
                
                ## Create the Timeline table records
                createTimelineRecord(merchantId, grn, now)

                updatedMappedJsonData.append(grn)
                
            ## Update the DocumentUpload table status
            logger.info(f'UPDATED MAPPED JSON: {updatedMappedJsonData}')
            updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData, now)
            generate_combined_csv(mappedJsonData, merchantId, documentUploadId, source_file_name, now, day)
            
            
        return {
            "status": True,
            "body": "Data extraction process completed."
        }
    
    except NoCredentialsError:
        return {"status": False, "body": "AWS credentials not available"}
    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        updateFailedDocumentUploadStatus(documentUploadId, "System Error")
        return {
            "status": True,
            'body': "The server encountered an unexpected condition that prevented it from fulfilling your request."
        }

@tracer.capture_method
def generate_combined_csv(mappedJsonData, merchantId, documentUploadId, source_file_name, now, day):
    file_name = source_file_name.replace(".pdf", "")
    success_file_name = f"grn_table_{file_name}_{now}.csv"
    success_csv_key = f"extracted_data/{merchantId}/{documentUploadId}/{day}/{success_file_name}"
    failed_file_name = f"grn_flagged_{file_name}_{now}.csv"
    failed_csv_key = f"extracted_data/{merchantId}/{documentUploadId}/{day}/{failed_file_name}"
    
    csv_data = []
    error_csv_data = []
    error_headers = []
    headers = [
        "Vendor_Name", 
        "GRN_Date", 
        "GRN_Number", 
        "PO_Number",
        "Item_Code",
        "Description",
        "Quantity",
        "UOM",
        "Total_Amount",
        "Source_File"
    ]
    
    # Define a mapping of CSV columns to possible JSON keys
    field_mapping = {
        "Vendor_Name": ["supplierName"],
        "GRN_Date": ["grnDate"],
        "GRN_Number": ["grnNumber"],
        "PO_Number": ["poNumber"],
        "Item_Code": ["itemCode"],
        "Description": ["description"],
        "Quantity": ["quantity"],
        "UOM": ["uom"],
        "Total_Amount": ["totalAmount"],
        "Due_Date": ["dueDate"],
    }

    for jsonData in mappedJsonData:
        row = []
        for lineItem in jsonData.get("lineItem", []):
            for column, possible_keys in field_mapping.items():
                value = ""
                for key in possible_keys:
                    if key in lineItem:
                        value = lineItem[key]
                        break
                row.append(value)
            row.append(source_file_name)
            csv_data.append(row)

        ## Generate failed report
        if len(csv_data) == 0:
            error_headers.extend(headers)
            error_headers.append("Error_Message")
            error_row = ["", "", "", "", "", "", "", "", "", "", source_file_name, "No table data extracted from the file"]
            error_csv_data.append(error_row)
    
    if len(csv_data) > 0:
        
        if os.path.exists('/tmp/' + success_file_name):
            os.remove('/tmp/' + success_file_name)
        
        with open('/tmp/' + success_file_name, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(headers)
            writer.writerows(csv_data)
        
        S3_CLIENT.upload_file('/tmp/' + success_file_name, BDA_PROCESSING_BUCKET, success_csv_key)
        os.remove('/tmp/' + success_file_name)
            
    else:
        
        if os.path.exists('/tmp/' + failed_file_name):
            os.remove('/tmp/' + failed_file_name)

        with open('/tmp/' + failed_file_name, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(error_headers)
            writer.writerows(error_csv_data)

        
        S3_CLIENT.upload_file('/tmp/' + failed_file_name, BDA_PROCESSING_BUCKET, failed_csv_key)

        
        os.remove('/tmp/' + failed_file_name)

@tracer.capture_method
def performExceptionChecking(grnData):
    prompt = f"""
TASK: Check the grn data for missing fields and apply exception rules.

INSTRUCTIONS:
1. Check for all of the following conditions (they are not mutually exclusive):
   - If 'isDuplicate' flag is True, note "duplicate grn detected" (highest priority)
   - If 'completeMapping' flag is False, note "master data mapping incomplete (<field names>)" (second priority)
   - If any fields have empty values, note "missing field values (<field names>)" (last priority)
2. Generate a clear, concise message that describes all identified issues.
3. Ensure the message is human-readable and accurately reflects all exceptions.

EXAMPLE MESSAGES:
- "Incomplete master data mapping and is missing required field values(buyerName, ...)"
- "Duplicate grn detected in the system."
- "Duplicate grn detected, master data mapping incomplete and missing field values"
- "Master data mapping incomplete (<item name>, ...)"
- "NA"

POSSIBLE EXCEPTION STATUSES:
- "MissingValues": Some fields were not extracted.
- "IsDuplicate": Record with same grn number found.
- "CompleteMapping": Not all entities were found in master files.
- "N/A": N/A

INPUT DATA:
{grnData}

OUTPUT FORMAT:
You STRICTLY only the return the descriptive message, you are NOT ALLOWED to deviate from this format. DO NOT PROVIDE ANY OTHER DETAILS:
{{
  "exceptionStatus": "Descriptive message explaining all issues found or no issues found",
  "status": [one of: "Exceptions", "Success"]
}}
"""
    
    exception_details, input_tokens, output_tokens = promptBedrock(prompt)
    exception_details = json.loads(exception_details)
    logger.info(f'DOCUMENT EXCEPTION STATUS CHECK RESULT: {exception_details}')
    return exception_details, input_tokens, output_tokens

@tracer.capture_method
def processJsonResult(result_json_list):
    mappedJsonData = []

    field_mapping = {
        "grnNumber": ["GRNNumber"],
        "grnDate": ["GRNDate"],
        "supplierName": ["SupplierName"],
        "poNumber": ["PONumber", "POnumber"],
        "paymentTerms": ["PaymentTerms"],
        "statusOfGoodsReceived": ["StatusOfGoodsReceived"],
    }

    table_field_mapping = {
        "itemCode": ["ItemCode"],
        "description": ["Description"],
        "quantity": ["Quantity"],
        "uom": ["UOM"],
        "totalAmount": ["TotalAmount"],
    }

    for file_key in result_json_list:
        try:
            logger.info(f'FILE KEY: {file_key}')
            response = S3_CLIENT.get_object(Bucket=BDA_PROCESSING_BUCKET, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            logger.info(f'DATA: {data}')

            explainability_info_list = data.get('explainability_info', [])
            
            # Initialize the row and extracted_data dictionary
            row = {}
            extracted_data = {}  # This will hold the raw data before mapping
            row['boundingBoxes'] = {}  # Initialize boundingBoxes as an empty dict
            service_table_data = None
            
            # First, extract raw data from explainability info
            for explanation_obj in explainability_info_list:
                for key, value in explanation_obj.items():
                    if key == "service_table":
                        service_table_data = value
                    else:
                        # Store the raw data for later processing
                        extracted_data[key] = value

            logger.info(f'EXTRACTED DATA: {extracted_data}')
            
            # Now process the extracted data according to field_mapping
            for column, possible_keys in field_mapping.items():
                value = ""
                row['boundingBoxes'][column] = []
                
                for key in possible_keys:
                    if key in extracted_data:
                        field_info = extracted_data[key]
                        # Extract just the value
                        value = field_info.get('value', '')
                        geometry_data = field_info.get('geometry', [])
                        
                        # Process bounding boxes
                        for geo_data in geometry_data:
                            page = geo_data.get('page', '')
                            bounding_box = geo_data.get('boundingBox', {})
                            
                            # Convert to Decimal for DynamoDB compatibility
                            decimal_box = {
                                'width': Decimal(str(bounding_box.get('width', 0))),
                                'height': Decimal(str(bounding_box.get('height', 0))),
                                'left': Decimal(str(bounding_box.get('left', 0))),
                                'top': Decimal(str(bounding_box.get('top', 0))),
                                'page': page  # Add page to each bounding box
                            }
                            
                            # Add bounding box to the appropriate field array
                            row['boundingBoxes'][column].append(decimal_box)
                        
                        break  # Break after finding the first matching key
                
                # Set the actual value for this field
                row[column] = value

            # Process line items
            row["lineItem"] = []
            if service_table_data:
                for service in service_table_data:
                    lineItem = {}
                    lineItem['boundingBoxes'] = {}
                    
                    # Process each line item field based on table_field_mapping
                    for column, possible_keys in table_field_mapping.items():

                        value = ""
                        lineItem['boundingBoxes'][column] = []
                        
                        for key in possible_keys:
                            if key in service:
                                field_info = service[key]
                                # Extract just the value
                                value = field_info.get('value', '')
                                geometry_data = field_info.get('geometry', [])
                                
                                # Process bounding boxes for line items
                                for geo_data in geometry_data:
                                    page = geo_data.get('page', '')
                                    bounding_box = geo_data.get('boundingBox', {})
                                    
                                    decimal_box = {
                                        'width': Decimal(str(bounding_box.get('width', 0))),
                                        'height': Decimal(str(bounding_box.get('height', 0))),
                                        'left': Decimal(str(bounding_box.get('left', 0))),
                                        'top': Decimal(str(bounding_box.get('top', 0))),
                                        'page': page
                                    }
                                    
                                    lineItem['boundingBoxes'][column].append(decimal_box)
                                
                                break
                        
                        # Set the actual value for this line item field
                        lineItem[column] = value
                    
                    row["lineItem"].append(lineItem)
                    
            # Extract confidence score from the matched_blueprint section
            confidence_score = 0
            if 'matched_blueprint' in data:
                confidence_score = data.get('matched_blueprint', {}).get('confidence', 0)
            elif 'blueprint_match' in data:
                confidence_score = data.get('blueprint_match', {}).get('confidence', 0)
            elif 'explainability_info' in data:
                # Try to find it in explainability_info if not at top level
                for info in data.get('explainability_info', []):
                    if isinstance(info, dict) and 'matched_blueprint' in info:
                        confidence_score = info.get('matched_blueprint', {}).get('confidence', 0)
                        break

            # Convert to float for consistency
            confidence_score = float(confidence_score)

            # Add confidence score to row data
            row["confidenceScore"] = confidence_score
            
            if row.get("supplierName").lower() == "apple malaysia sdn. bhd.":
                row["poNumber"] = format_apple_po_number(row.get("poNumber", ""))
                logger.info(f"Formatted PO Number: {row['poNumber']}")

            mappedJsonData.append(row)
        except Exception as e:
            logger.error(f"Error processing file {file_key}: {str(e)}")
            # Continue with next file if there's an error

    logger.info(f'MAPPED JSON DATA: {mappedJsonData}')
    return mappedJsonData

@tracer.capture_method
def format_apple_po_number(po_number):
    if not po_number:
        return "PO-000000"  # Default value if empty
    
    # Split at first space and only keep the first part
    po_part = po_number.split(" ")[0]
    
    # If already properly formatted, keep it
    if re.match(r'^PO-\d{6}[A-Za-z0-9]*$', po_part):
        return po_part
    
    # Extract digits and any suffix characters
    match = re.search(r'(\d+)([A-Za-z0-9]*)$', po_part)
    if match:
        digits = match.group(1)
        suffix = match.group(2)  # Any characters after the digits
        numeric_part = digits[:6].zfill(6)  # Format the 6 digits
        return f"PO-{numeric_part}{suffix}"
    
    # If no digits found
    return "PO-000000"

@tracer.capture_method
def get_merchant_mapping(merchantId):
    response = MERCHANT_DDB_TABLE.get_item(
        Key={'merchantId': merchantId},
    )
    
    merchant = response['Item']
    supplierMappingPath = merchant.get('supplierMapping')
    itemMappingPath = merchant.get('itemMapping')

    return supplierMappingPath, itemMappingPath

@tracer.capture_method
def performMasterDataChecking(grn, merchantId):

    ## Save the confidence score before processing
    original_confidence_score = grn.get('confidenceScore')

    supplierMappingPath, itemMappingPath = get_merchant_mapping(merchantId)
    supplierDatabase = parse_mappings(supplierMappingPath)
    itemDatabase = parse_mappings(itemMappingPath)

    grn, input_tokens, output_tokens = performLineItemMasterMapping(grn, itemDatabase)
    logger.info(f'GRN AFTER LINE ITEM MASTER DATA CHECKING: {grn}')
    
    grn, input_tokens, output_tokens = performMasterDataMapping(grn, supplierDatabase)
    logger.info(f'GRN AFTER MASTER DATA CHECKING: {grn}')

    ## Restore the confidence score
    if original_confidence_score is not None:
        grn['confidenceScore'] = original_confidence_score

    return grn

@tracer.capture_method
def parse_mappings(object_key) -> str:
    response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=object_key)
    csv_content = response['Body'].read().decode('utf-8')
    
    df = pd.read_csv(io.StringIO(csv_content))
    df_headers = list(df.columns)
    item_list = df.to_dict('records')

    formatted_df = f"Columns: {'|'.join(df_headers)}\n"
    
    for item in item_list:
        row_data = []
        for header in df_headers:
            row_data.append(f"{item[header]}")
        formatted_df += "\n" + "|".join(row_data)

    return formatted_df

@tracer.capture_method
def performLineItemMasterMapping(grn, database):
    formatted_item = []
    bounding_box_map = {}
    
    for index, item in enumerate(grn.get("lineItem", [])):
        item_id = f"item_{index}"
        
        bounding_box_map[item_id] = item.get("boundingBoxes", {})
        
        item_payload = {
            "item_list_id": item_id,  # Add an identifier to track this item
            "description": item.get("description"),
            "uom": item.get("uom"),
            "quantity": item.get("quantity"),
            "totalAmount": item.get("totalAmount")
        }
        formatted_item.append(item_payload)

    prompt_data = f"""
TASK:
Perform Data Mapping for the input items.
You are provided a database within the <database> tags to cross-reference the input items.
You are to map the input items to the correct items in the database and return the itemCode value in the items JSON object.
Fields with empty values should remain as "" and must not affect the completeMapping output.

SCENARIO:
1. If you are able to map the item completely, return the itemCode value, set completeMapping in the item JSON object to True.
    a. set status to "Success"
    b. set exceptionStatus to "N/A"
2. If you are able to map the description field but other fields are not mapped, return the itemCode value, set completeMapping in the item JSON object to False.
    a. set status to "Exceptions"
    b. set exceptionStatus to "Master Mapping Error (<fields>)"
3. If you are able to map all fields except for description, return "-" as the value in the itemCode field and set completeMapping in the item JSON object to False.
    a. set status to "Exceptions"
    b. set exceptionStatus to "Master Mapping Error (<fields>)"

EXAMPLE:
<database>
Columns: item code|item description|quantity|uom|total amount|last modified
1|melon|3|KG|22.00|28/3/2025
6|IPAD PRO 11 WF CL 256GB SP BLK-ITP|5|EA|2565.50|31/3/2025
10|APPLE WATCH MAG FAST CHARGER|3|EA|300.00|2/4/2025
20|NB IP Slim 3 15 IRU9 CORE5 16G 512G 11S|6|EA|12000|2/4/2025
</database>

<input>
[{{"item_list_id": "item_0", "description": "Melon",  "quantity": 4, "uom": "", "totalAmount": 40}}, {{"item_list_id": "item_1", "description": "HP 266 Mouse", "quantity": 4, "uom": "KG", "totalAmount": 56}}]
</input>

<output>
[{{"item_list_id": "item_0", "description": "Melon",  "quantity": 4, "uom": "", "totalAmount": 40.00, "itemCode": "1", "completeMapping": true, "status": "Success", "exceptionStatus": "N/A"}}, {{"item_list_id": "item_1", "description": "HP 266 Mouse", "quantity": 4, "uom": "KG", "totalAmount": 56.00, "itemCode": "-", "completeMapping": false, "status": "Exceptions", "exceptionStatus": "Master Mapping Error (description, uom)"}}]
</output>

IMPORTANT:
- Different entities can be closely related by name but they are not the same, ENSURE you are mapping the correct entities and returning the correct codes.
- DO NOT MODIFY and DO NOT CONSIDER item_list_id field in the input JSON object for the mapping process.

REAL INPUT:
<database>
{database}
</database>

<input>
{formatted_item}
</input>

OUTPUT FORMAT:
You STRICTLY only the return the response in the example JSON object below, you are NOT ALLOWED to deviate from this format. DO NOT PROVIDE ANY OTHER DETAILS:
[{{
    "item_list_id": "item_0",
    "description": "item001",
    "itemCode": "code001",
    "quantity": "2",
    "uom": "KG",
    "totalAmount": "20.00",
    "completeMapping": True or False,
    "status": "Success" or "Exceptions",
    "exceptionStatus": "N/A" or "Master Mapping Error (<fields>)",
}},
{{...}}
]
"""
    
    masterMappedItems, input_tokens, output_tokens = promptBedrock(prompt_data)
    masterMappedItems = json.loads(masterMappedItems)

    for item in masterMappedItems:
        item_id = item.pop("item_list_id", None)  # Remove the item_id field
        if item_id and item_id in bounding_box_map:
            item["boundingBoxes"] = bounding_box_map[item_id]

    logger.info(f'MASTER MAPPED ITEMS: {masterMappedItems}')
    
    grn["lineItem"] = masterMappedItems
    
    return grn, input_tokens, output_tokens

@tracer.capture_method
def performMasterDataMapping(grn, database):
    boudinging_boxes = grn.get("boundingBoxes", {})
    del grn["boundingBoxes"]

    line_item_bounding_boxes = {}
    for idx, item in enumerate(grn.get("lineItem", [])):
        if "boundingBoxes" in item:
            line_item_bounding_boxes[idx] = item["boundingBoxes"]
            del item["boundingBoxes"]

    prompt_data = f"""
TASK:
Analyze the entities from in the input Json Object and map them to the correct entities in the <database> tags.
You are provided databases within the <database> tags to cross-reference the entities.
Fields with empty values should remain as "" and must not affect the completeMapping output.

For formFields, ONLY map Supplier fields to the correct company entities in the database and set the "supplierCode" with the values from the database.
scenarios:
1. If you are able to map both the company name and address completely, set the Code as the code value from the database.
    a. set completeMapping in the formFields to True.
    b. set status in the formFields to "Success"
    c. set exceptionStatus in the formFields to "N/A"
2. If you are able to map the company name but not the address, set the code as the code value from the database.
    a. set completeMapping in the formFields to False.
    b. set status in the formFields to "Exceptions"
    c. set exceptionStatus in the formFields to "Master Mapping Error (address)"
3. If you are able to map the address but not the company name, set the code as the "-" .
    a. set completeMapping in the formFields to False.
    b. set status in the formFields to "Exceptions"
    c. set exceptionStatus in the formFields to "Master Mapping Error (name)"
4. If you are unable to map the company name and address, set the code as "-".
    a. set completeMapping in the formFields to False.
    b. set status in the formFields to "Exceptions"
    c. set exceptionStatus in the formFields to "Master Mapping Error (name, address)"

After mapping the formFields and setting status and exceptionStatus, check the lineItem array's status and exceptionStatus and update the formFields status and exceptionStatus fields accordingly.
scenarios:
1. If all line items and formFields are mapped correctlyy, set the formFields status to "Success" and exceptionStatus to "N/A".
2. If all line item is mapped correctly but formFields are not, remain formFields status and exceptionStatus.
3. If any line item is not mapped correctly but formFields are, set the formFields status to "Exceptions" and generate a new general exceptionStatus message that best describes the overall exceptionStatuses.
4. If all line items and formFields are not mapped correctly, set the formFields status to "Exceptions" and exceptionStatus to "Master Mapping Error (<fields>)".

DATABASE TEXT:
{database}

EXTRACTED GRN TEXT:
<grn>
{grn}
</grn>

OUTPUT FORMAT:
You STRICTLY only the return the response in the example JSON object below, you are NOT ALLOWED to deviate from this format. DO NOT PROVIDE ANY OTHER DETAILS:
You MUST NOT remove any fields in the JSON object, even if you are unable to map them.
You MUST NOT remove the boundingBoxes field in the JSON object.
{{
    "formField1": "formValue1",
    "formField2": "formValue2",
    ...,
    "lineItem": [
        {{
            ...,
            "completeMapping": True or False,
            "status": "Success" or "Exceptions",
            "exceptionStatus": "N/A" or "Master Mapping Error (<fields>)",
        }},
        {{...}}
    ],
    "completeMapping": True/False,
}}

SAMPLE OUTPUT:
{{
    "grnNumber": "grn001",
    "buyerName": "merchant001",
    "buyerCode": "-",
    "supplierName": "supplier001",
    "supplierCode": "supplierCode001",
    ...,
    "lineItem": [
        {{
            ...,
            "completeMapping": True,
            "status": "Success",
            "exceptionStatus": "N/A",
        }},
        {{
            ...,
            "completeMapping": False,
            "status": "Exceptions",
            "exceptionStatus": "Master Mapping Error (description, unitPrice, ...)",
        }},
        {{...}}
    ],
    "completeMapping": False
    "status": "Exceptions",
    "exceptionStatus": "Master Mapping Error for buyer and line items (description, unitPrice, ...) ",
}}

IMPORTANT:
- Different entities can be closely related by name but they are not the same, ENSURE you are mapping the correct entities and returning the correct codes.
- If you are unsure about a mapping, please return "-" as the value.
"""

    grn, input_tokens, output_tokens = promptBedrock(prompt_data)
    grn = json.loads(grn)
    grn["boundingBoxes"] = boudinging_boxes

    for idx, item in enumerate(grn.get("lineItem", [])):
        if idx in line_item_bounding_boxes:
            item["boundingBoxes"] = line_item_bounding_boxes[idx]

    return grn, input_tokens, output_tokens

@tracer.capture_method
def performDuplicateChecking(grn, merchantId):
    # for mappedJson in mappedJsonData:
    grnResp = EXTRACTED_GRN_DDB_TABLE.query(
        IndexName='gsi-merchantId-grnNumber',
        KeyConditionExpression=Key('merchantId').eq(merchantId)&Key('grnNumber').eq(grn.get('grnNumber')),
        FilterExpression=Attr('documentType').eq("grn")
    ).get('Items', [])

    if grnResp:
        grn["isDuplicate"] = True
    else:
        grn["isDuplicate"] = False
    
    return grn

@tracer.capture_method
def createExtractedResultRecord(grnData, merchantId, documentUploadId, source_file_name, file_path, now):
    extractedGrnId = str(uuid.uuid4())
    for item in grnData.get("lineItem", []):
        extractedDocumentLineItemPayload = {
            "extractedGrnLineItemsId": str(uuid.uuid4()),
            'grnNumber': grnData.get("grnNumber"),
            "itemCode": item.get("itemCode"),
            "description": item.get("description"),
            "quantity": item.get("quantity"),
            "itemUom": item.get("uom"),
            "totalAmount": grnData.get("totalAmount", 0),
            "merchantId": merchantId,
            "extractedGrnId": extractedGrnId,
            "documentUploadId": documentUploadId,
            'boundingBoxes': item.get('boundingBoxes'),
            "exceptionStatus": item.get('exceptionStatus'),
            'status': item.get('status'),
            "supplierCode": grnData.get("supplierCode"),
            "supplierName": grnData.get("supplierName"),
            "createdAt": now,
            "createdBy": "System",
            "updatedAt": now,
            "updatedBy": "System"
        }

        extractedDocumentLineItemPayload = convert_floats_to_decimals(extractedDocumentLineItemPayload)
        EXTRACTED_GRN_LINE_ITEM_DDB_TABLE.put_item(Item=extractedDocumentLineItemPayload)  

    extractedDocumentPayload = {
        "extractedGrnId": extractedGrnId,
        "merchantId": merchantId,
        "grnNumber": grnData.get("grnNumber"),
        "grnDate": grnData.get("grnDate"),
        "documentType": grnData.get("documentType", "grn"),
        "purchaseOrderNo": grnData.get("poNumber"),
        "supplierName": grnData.get("supplierName"),
        "supplierCode": grnData.get("supplierCode"),
        "statusOfGoodsReceived": grnData.get("statusOfGoodsReceived"),
        "documentStatus": grnData.get("status"),
        'boundingBoxes': grnData.get('boundingBoxes'),
        "exceptionStatus": grnData.get('exceptionStatus'),
        "filePath": file_path,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "approvedAt": "",
        "approvedBy": "",
        "remarks": "",
        "sourceFile": source_file_name,
        "confidenceScore": round(grnData.get("confidenceScore", 0)*100),
        "documentUploadId": documentUploadId
    }

    extractedDocumentPayload = convert_floats_to_decimals(extractedDocumentPayload)
    EXTRACTED_GRN_DDB_TABLE.put_item(Item=extractedDocumentPayload)
    grnData["extractedGrnId"] = extractedGrnId

    return grnData

@tracer.capture_method
def createTimelineRecord(merchantId, grnData, now):
    if 'approvalStatus' in grnData:
        if grnData['approvalStatus'] == "APPROVED":
            title = "approved"
            description = "GRN approved"
        else:
            title = "rejected"
            description = grnData.get('rejectionReason', "GRN rejected")
    elif grnData['status'] == "Success":
        title = "Document Processed"
        description = "Document extracted successfully"
    else:
        title = "Document Processing Failed"
        description = grnData.get('exceptionStatus')
    
    timelinePayload = {
        "timelineId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "timelineForId": grnData.get("extractedGrnId"),
        "title": title,
        "type": grnData.get("documentType", "grn"),
        "description": description,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "grnNumber": grnData.get("grnNumber", "-"),
        "supplierName": grnData.get("supplierName", "-")
    }
    TIMELINE_DDB_TABLE.put_item(Item=timelinePayload)

@tracer.capture_method
def documentUploadStatusCheck(document_upload_id):
    # query all exceptionStatus from extractedDocuments with documentUploadId
    # and return the exceptionStatus and status based on the extractedDocuments' exceptionStatus and status

    time.sleep(3) # wait for 3 seconds to ensure all records are inserted

    all_extracted_documents = EXTRACTED_GRN_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id)
    ).get('Items', [])

    all_statuses = [extracted_document.get('exceptionStatus') for extracted_document in all_extracted_documents]

    prompt = f"""
TASK: Categorize a list of document exception statuses into a single exception status and provide a high-level status based on priority ranking.

INPUT:
A list of exception statuses from multiple documents in the following format:
["exception status 1", "exception status 2", "exception status 3", ...]

CATEGORIZATION RULES WITH PRIORITY RANKING:
1. Exception Status Determination (in order of priority):
   a. If ANY statuses contain mention of duplicate grns, set exception status to "Duplicate Error" (HIGHEST PRIORITY)
   b. If ANY statuses indicate master data mapping failures, set exception status to "Master Mapping Error" (SECOND PRIORITY)
   c. If ANY statuses indicate missing fields or data errors, set exception status to "Missing Field Error" (THIRD PRIORITY)
   d. If ALL statuses are "None" or indicate no issues, set exception status to "N/A" (LOWEST PRIORITY)

2. High-Level Status Determination:
   a. Set status to "Fail" if exception status is "Duplicate Error" or "Master Mapping Error"
   b. Set status to "Pending Review" if exception status is "Missing Field Error"
   c. Set status to "Success" if exception status is "N/A"

EXAMPLES:
- Input: ["Duplicate GRN Number Found", "Missing required field values"]
  Output: {{"exceptionStatus": "Duplicate Error", "status": "Fail"}}

- Input: ["Missing required field values", "Master Data Mapping Failed"]
  Output: {{"exceptionStatus": "Master Mapping Error", "status": "Fail"}}
  
- Input: ["Missing required field values", "N/A"]
  Output: {{"exceptionStatus": "Missing Field Error", "status": "Pending Review"}}
  
- Input: ["None", "N/A"]
  Output: {{"exceptionStatus": "N/A", "status": "Success"}}

EXPECTED OUTPUT:
Return ONLY a valid JSON object with the following structure, with no formatting, markdown, or additional text:
{{
  "exceptionStatus": [one of: "Duplicate Error", "Master Mapping Error", "Missing Field Error", "NA"],
  "status": [one of: "Fail", "Pending Review", "Success"]
}}

INPUT:
{all_statuses}
"""

    exception_status, input_tokens, output_tokens = promptBedrock(prompt)
    exception_status = json.loads(exception_status)
    logger.info(f'DOCUMENT EXCEPTION STATUS CHECK RESULT: {exception_status}')
    return exception_status, input_tokens, output_tokens

    
@tracer.capture_method
def updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData, now):
    exception_details, input_tokens, output_tokens = documentUploadStatusCheck(documentUploadId)
    exception_status = str(exception_details.get("exceptionStatus"))
    status = str(exception_details.get("status"))


    # Collect valid confidence scores (ensure they're not 0 or None)
    confidence_scores = []
    for mappedJson in updatedMappedJsonData:
        score = mappedJson.get("confidenceScore", 0)
        confidence_scores.append(float(score))

    # Calculate average (avoid division by zero)
    if confidence_scores and any(confidence_scores):
        avg_confidence_score = round(sum(confidence_scores) / len(confidence_scores)) * 100
    else:
        avg_confidence_score = 0 
    avg_confidence_score_decimal = convert_floats_to_decimals(avg_confidence_score)

    # Convert for DynamoDB
    confidence_scores_decimal = [convert_floats_to_decimals(score) for score in confidence_scores]

    update_values = {
        ':status': status,
        ':exceptionStatus': exception_status,
        ':updatedAt': now,
        ':updatedBy': "System",
        ':avgConfidenceScore': avg_confidence_score_decimal,
        ':confidenceScoreList': confidence_scores_decimal
    }

    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={
            'documentUploadId': documentUploadId,
        },
        UpdateExpression="set #status_attr = :status, exceptionStatus = :exceptionStatus, updatedAt = :updatedAt, updatedBy = :updatedBy, avgConfidenceScore = :avgConfidenceScore, confidenceScoreList = :confidenceScoreList",
        ExpressionAttributeNames={
            '#status_attr': 'status'
        },
        ExpressionAttributeValues={
            ':status': status,
            ':exceptionStatus': exception_status,
            ':updatedAt': now,
            ':updatedBy': "System",
            ':avgConfidenceScore': avg_confidence_score_decimal,
            ':confidenceScoreList': confidence_scores_decimal 
        }
    )

@tracer.capture_method
def updateFailedDocumentUploadStatus(documentUploadId, exceptionStatus):
    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={
            'documentUploadId': documentUploadId,
        },
        UpdateExpression="set #status_attr = :status, exceptionStatus = :exceptionStatus",
        ExpressionAttributeNames={
            '#status_attr': 'status'
        },
        ExpressionAttributeValues={
            ':status': "Failed",
            ':exceptionStatus': exceptionStatus
        }
    )

def convert_floats_to_decimals(obj):
    """
    Recursively convert all float values in a nested structure to Decimal
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimals(item) for item in obj]
    else:
        return obj