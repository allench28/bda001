#Process BDA Extracted Output
import os
import boto3
import json
import time
import pandas as pd
from decimal import ROUND_HALF_UP, Decimal
import io
from aws_lambda_powertools import Logger, Tracer
from botocore.exceptions import NoCredentialsError, ClientError
import csv
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import uuid
from bedrock_function import promptBedrock

SMARTEYE_DOCUMENTS_BUCKET = os.environ.get('SMARTEYE_DOCUMENTS_BUCKET')
AGENT_MAPPING_BUCKET = os.environ.get('AGENT_MAPPING_BUCKET')
DOCUMENT_UPLOAD_TABLE = os.environ.get('DOCUMENT_UPLOAD_TABLE')
EXTRACTED_DOCUMENT_TABLE = os.environ.get('EXTRACTED_PO_TABLE')
EXTRACTED_DOCUMENT_LINE_ITEM_TABLE = os.environ.get('EXTRACTED_PO_LINE_ITEM_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
# AGENT_CONFIGURATION_TABLE = os.environ.get('AGENT_CONFIGURATION_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
BDA_PROCESSING_BUCKET = os.environ.get('BDA_PROCESSING_BUCKET')


S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_UPLOAD_TABLE)
EXTRACTED_DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_TABLE)
EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_DOCUMENT_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)
# AGENT_CONFIGURATION_DDB_TABLE = DDB_RESOURCE.Table(AGENT_CONFIGURATION_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)

logger = Logger()
tracer = Tracer()

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

            if source_file_name.rsplit('_',1)[0] != 'purchase_order':
                unsupportExtractedDocumentTypePayload = {
                    "poNumber": "-",
                    "poDate": "-",
                    "documentType": "Not Po",
                    "supplierName": "-",
                    "supplierAddress": "-",
                    "deliveryAddress": "-",
                    "currency": "-",
                    "requestDeliveryDate": "-",
                    "paymentTerms": "-",
                    "totalTaxAmount": 0,
                    "totalAmountWithTax": 0,
                    'boundingBoxes': "{}",
                    "confidenceScore": 0,
                    "status": "Exceptions",
                    "exceptionStatus": "Document Format Unrecognized",
                    "documentUploadId": documentUploadId,
                    "merchantId": merchantId,
                    "createdAt": now,
                    "createdBy": "System",
                    "updatedAt": now,
                    "updatedBy": "System",
                    "sourceFile": source_file_name,
                }
                createExtractedResultRecord(unsupportExtractedDocumentTypePayload, merchantId, documentUploadId, source_file_name, file_path, now)
                # updateDocumentUploadStatus(documentUploadId, [unsupportDocumentTypePayload], now)
                updatedMappedJsonData.append(unsupportExtractedDocumentTypePayload)
                updateFailedDocumentUploadStatus(documentUploadId, "Document Format Unrecognized")
                createTimelineRecord(merchantId, unsupportExtractedDocumentTypePayload, now)
                continue

            ## Perform initial mapping on the JSON file according to the required fields
            mappedJsonData = processJsonResult(result_json_list)

            for po in mappedJsonData:
                ## Perform master data checking
                po = performMasterDataChecking(po, merchantId)
                logger.info(f'PO AFTER MASTER DATA CHECKING: {po}')

                ## Perform duplication checking
                po = performDuplicateChecking(po, merchantId)
                logger.info(f'PO AFTER DUPLICATE CHECKING: {po}')

                exception_details, input_tokens, output_tokens = performExceptionChecking(po)
                po['exceptionStatus'] = exception_details.get("exceptionStatus")
                po['status'] = exception_details.get("status")
                logger.info(f'EXCEPTION DETAILS: {exception_details}')

                ## Create the ExtractedDocument & ExtractedDocumentLineItem table records
                po = createExtractedResultRecord(po, merchantId, documentUploadId, source_file_name, file_path, now)
                logger.info(f'PO AFTER CREATE EXTRACTED RESULT: {po}')
                
                ## Create the Timeline table records
                createTimelineRecord(merchantId, po, now)

                updatedMappedJsonData.append(po)
                
            ## Update the DocumentUpload table status
            logger.info(f'UPDATED MAPPED JSON: {updatedMappedJsonData}')
            updateDocumentUploadStatus(documentUploadId, updatedMappedJsonData, now)
            generate_combined_csv(mappedJsonData, merchantId, documentUploadId, source_file_name, now, day)
            generate_so_csv(updatedMappedJsonData, merchantId, documentUploadId, source_file_name, now, day)
            
            
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
    success_file_name = f"po_table_{file_name}_{now}.csv"
    success_csv_key = f"extracted_data/{merchantId}/{documentUploadId}/{day}/{success_file_name}"
    failed_file_name = f"po_flagged_{file_name}_{now}.csv"
    failed_csv_key = f"extracted_data/{merchantId}/{documentUploadId}/{day}/{failed_file_name}" 
    csv_data = []
    error_csv_data = []
    error_headers = []
    headers = [
        "Customer_Po_Number",
        "Customer_Name", 
        "Customer_Code",
        "Currency",
        "Billing_Address",
        "Delivery_Address",
        "Requested_Delivery_Date",
        "Payment_Terms",
        "Item_Code",
        "Description",
        "Unit_Price",
        "UOM",
        "Quantity",
        "Total_Amount",
        "Source_File"
    ]
    
    # Define a mapping of CSV columns to possible JSON keys
    field_mapping = {
        "Customer_Po_Number": ["poNumber"],
        "Customer_Po_Date": ["poDate"],
        "Customer_Name": ["buyerName"],
        "Customer_Code": ["buyerCode"],
        "Currency": ["currency"],
        "Billing_Address": ["supplierAddress"],
        "Delivery_Address": ["deliveryAddress"],
        "Requested_Delivery_Date": ["requestDeliveryDate"],
        "Payment_Terms": ["paymentTerms"],
        "Item_Code": ["itemCode"],
        "Description": ["description"],
        "Unit_Price": ["unitPrice"],
        "UOM": ["itemUom"],
        "Quantity": ["quantity"],
        "Total_Amount": ["totalAmountWithTax"],
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
        
        S3_CLIENT.upload_file('/tmp/' + success_file_name, SMARTEYE_DOCUMENTS_BUCKET, success_csv_key)
        os.remove('/tmp/' + success_file_name)
            
    else:
        
        if os.path.exists('/tmp/' + failed_file_name):
            os.remove('/tmp/' + failed_file_name)

        with open('/tmp/' + failed_file_name, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(error_headers)
            writer.writerows(error_csv_data)

        
        S3_CLIENT.upload_file('/tmp/' + failed_file_name, SMARTEYE_DOCUMENTS_BUCKET, failed_csv_key)

        
        os.remove('/tmp/' + failed_file_name)

# Generate Sales Order (SO) CSVs based on extracted PO data.
@tracer.capture_method
def generate_so_csv(mappedJsonData, merchantId, documentUploadId, source_file_name, now, day):
    file_name = source_file_name.replace(".pdf", "")
    so_document_file_name = f"so_document_{file_name}_{now}.csv"
    so_line_item_file_name = f"so_line_item_{file_name}_{now}.csv"
    so_flagged_file_name = f"so_flagged_{file_name}_{now}.csv"

    so_document_csv_key = f"sales_orders/{merchantId}/{documentUploadId}/{day}/{so_document_file_name}"
    so_line_item_csv_key = f"sales_orders/{merchantId}/{documentUploadId}/{day}/{so_line_item_file_name}"
    so_flagged_csv_key = f"sales_orders/{merchantId}/{documentUploadId}/{day}/{so_flagged_file_name}"

    document_headers = [
        "SO_Number", "Customer_Code", "Customer_Name", "SO_Date", "Currency",
        "Total_Amount", "Tax_Amount", "Grand_Total", "Billing_Address", "Delivery_Address", "Source_File"
    ]
    line_item_headers = [
        "SO_Number", "Item_Code", "Description", "Quantity", "UOM", "Currency", "Tax_Amount", "Unit_Price", "Total_Price", "Customer_Code", "Customer_Name", "Total_Tax_Amount", "Total_PO_Amount", "Delivery_Address"
    ]
    flagged_headers = [
        "Reason", "PO_Data", "Source_File"
    ]

    document_csv_data = []
    line_item_csv_data = []
    flagged_data = []

    for po in mappedJsonData:
        if not po.get("poNumber"): 
            flagged_data.append([
                "Missing poNumber",
                json.dumps(po),
                source_file_name
            ])
            continue
        
        # Use buyerName if deliveryAddress is empty or None
        delivery_address = po.get("deliveryAddress")
        if delivery_address == "-":
            delivery_address = po.get("buyerAddress")
        so_number = f"SO-{po.get('poNumber')}"


        document_csv_data.append([
            so_number,
            po.get("buyerCode"),
            po.get("buyerName"),
            now.split("T")[0],  # SO Date
            po.get("currency"),
            po.get("totalAmountWithoutTax"),
            po.get("totalTaxAmount"),
            po.get("totalAmountWithTax"),
            po.get("supplierAddress"),
            delivery_address,
            source_file_name
        ])

        for item in po["lineItem"]:
            line_item_csv_data.append([
                so_number,
                item.get("itemCode"),
                item.get("description"),
                item.get("quantity"),
                item.get("itemUom"),
                po.get("currency"),
                item.get("taxAmount"),
                item.get("unitPrice"),
                item.get("totalPrice"),
                po.get("buyerCode"),
                po.get("buyerName"),
                po.get("totalTaxAmount"),
                po.get("totalAmountWithTax"),
                delivery_address               
            ])

    # Write and upload document-level CSV
    if document_csv_data:
        with open(f"/tmp/{so_document_file_name}", "w", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(document_headers)
            writer.writerows(document_csv_data)
        S3_CLIENT.upload_file(f"/tmp/{so_document_file_name}", SMARTEYE_DOCUMENTS_BUCKET, so_document_csv_key)
        os.remove(f"/tmp/{so_document_file_name}")

    # Write and upload line-item-level CSV
    if line_item_csv_data:
        with open(f"/tmp/{so_line_item_file_name}", "w", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(line_item_headers)
            writer.writerows(line_item_csv_data)
        S3_CLIENT.upload_file(f"/tmp/{so_line_item_file_name}", SMARTEYE_DOCUMENTS_BUCKET, so_line_item_csv_key)
        os.remove(f"/tmp/{so_line_item_file_name}")

    # Write and upload flagged/failed CSV if any
    if flagged_data:
        with open(f"/tmp/{so_flagged_file_name}", "w", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(flagged_headers)
            writer.writerows(flagged_data)
        S3_CLIENT.upload_file(f"/tmp/{so_flagged_file_name}", SMARTEYE_DOCUMENTS_BUCKET, so_flagged_csv_key)
        os.remove(f"/tmp/{so_flagged_file_name}")

    logger.info(f"SO CSVs generated (document: {len(document_csv_data)}, line-items: {len(line_item_csv_data)}, flagged: {len(flagged_data)}) for documentUploadId: {documentUploadId}")


@tracer.capture_method
def performExceptionChecking(poData):
    prompt = f"""
TASK: Check the po data for missing fields and apply exception rules.

INSTRUCTIONS:
1. Check for all of the following conditions (they are not mutually exclusive):
   - If 'isDuplicate' flag is True, note "duplicate po detected" (Highest priority)
   - If 'completeMapping' flag is False, note "master data mapping incomplete (<field names>)" (second priority) 
   - If any fields except for fields <taxType>, <deliveryAddress>, and <paymentTerms> have empty values , note them as 'missing field values (<field names>)," (last priority)
2. Generate a clear, concise message that describes all identified issues.
3. Ensure the message is human-readable and accurately reflects all exceptions.

EXAMPLE MESSAGES:
- "Incomplete master data mapping and is missing required field values(customerName, ...)"
- "Duplicate po detected in the system."
- "Duplicate po detected, master data mapping incomplete for line item 2 and missing field values"
- "Vendor name and address was not found in the master files."
- "Duplicate buyer code in the master file."
- "Buyer code is missing in the master file."
- "N/A"


INPUT DATA:
{poData}

OUTPUT FORMAT:
You STRICTLY only the return the descriptive message, you are NOT ALLOWED to deviate from this format. DO NOT PROVIDE ANY OTHER DETAILS:
{{
  "exceptionStatus": "Descriptive message explaining all issues found or no issues found",
  "status": [one of: "Exceptions", "Success"]
}}
"""
    
    exception_details, input_tokens, output_tokens = promptBedrock(prompt)
    exception_details = json.loads(exception_details)
    return exception_details, input_tokens, output_tokens


@tracer.capture_method
def processJsonResult(result_json_list):
    mappedJsonData = []
    field_mapping = {
        "poNumber": ["poNumber"],
        "poDate": ["poDate"],
        "supplierName": ["supplierName"],
        "buyerName": ["buyerName"],
        "buyerAddress": ["buyerAddress"],
        "currency": ["currency"],
        "supplierAddress": ["supplierAddress"],
        "deliveryAddress": ["deliveryAddress"],
        "requestDeliveryDate": ["requestDeliveryDate"],
        "paymentTerms": ["paymentTerms"],
        "taxType": ["taxType"],
        "taxRate": ["taxRate"],
        "totalTaxAmount": ["totalTaxAmount"],
        "totalAmountWithTax": ["totalAmountWithTax"],
        "totalAmountWithoutTax": ["totalAmountWithoutTax"]
        
    }

    table_field_mapping = {
        "itemCode": ["itemCode"],
        "description": ["description"],
        "unitPrice": ["unitPrice"],
        "itemUom": ["itemUom"],
        "quantity": ["quantity"],
        "totalPrice": ["totalPrice"],
        "taxAmount": ["taxAmount"],

        
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
                        if key == "deliveryAddress":
                                    value = field_info.get('value') if field_info.get('value') != "" else field_info.get('buyerAddress')
                        else:
                            # Extract just the value
                            value = field_info.get('value', '')
                            if isinstance(value, str):
                                value = "-" if not value else value
                            else:
                                value = 0 if value is None else value
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
                        if column == 'amountExclTax':
                            column = 'totalPrice'  # Adjust for the final field name

                        value = ""
                        lineItem['boundingBoxes'][column] = []
                        
                        for key in possible_keys:
                            if key in service:
                                field_info = service[key]
                                # Extract just the value
                                if key == "itemUom":
                                    value = field_info.get('value') if field_info.get('value') != "" else "EA"
                                    
                                else:
                                    value = field_info.get('value', '')
                                    if isinstance(value, str):
                                        value = "-" if not value else value
                                    else:
                                        value = 0 if value is None else value
                                
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
            
            mappedJsonData.append(row)
        except Exception as e:
            logger.error(f"Error processing file {file_key}: {str(e)}")
            # Continue with next file if there's an error

    logger.info(f'MAPPED JSON DATA: {mappedJsonData}')
    return mappedJsonData

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
def performMasterDataChecking(po, merchantId):

    ## Save the confidence score before processing
    original_confidence_score = po.get('confidenceScore')

    supplierMappingPath, itemMappingPath = get_merchant_mapping(merchantId)
    supplierDatabase = parse_mappings(supplierMappingPath)  
    itemDatabase = parse_mappings(itemMappingPath)

    po, input_tokens, output_tokens = performLineItemMasterMapping(po, itemDatabase)
    logger.info(f'PO AFTER LINE ITEM MASTER DATA CHECKING: {po}')
    
    po, input_tokens, output_tokens = performMasterDataMapping(po, supplierDatabase)
    logger.info(f'PO AFTER MASTER DATA CHECKING: {po}')

    ## Restore the confidence score
    if original_confidence_score is not None:
        po['confidenceScore'] = original_confidence_score

    return po

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
def performLineItemMasterMapping(po, database):
    formatted_item = []
    bounding_box_map = {}
    
    for index, item in enumerate(po.get("lineItem", [])):
        item_id = f"item_{index}"
        
        bounding_box_map[item_id] = item.get("boundingBoxes", {})
        
        item_payload = {
            "item_list_id": item_id,  # Add an identifier to track this item
            "description": item.get("description"),
            "quantity": item.get("quantity"),
            "itemUom": item.get("itemUom"),
            "taxAmount": item.get("taxAmount"),
            "unitPrice": item.get("unitPrice"),
            "totalPrice": item.get("totalPrice"),
        }
        formatted_item.append(item_payload)

    logger.info(f"LINE ITEM DATABASE: {database}")

    prompt_data = f"""
TASK:
Perform Data Mapping for the input items.
You are provided a database within the <database> tags to cross-reference the input items.
You are to map the input items to the correct items in the database only on the description, itemUom and unit price column and return the corresponding itemCode value found in database.
DO NOT edit any other fields in the items JSON object besides the itemCode.

SCENARIO:
1. If you are able to map the item completely (even with minor differences in description like hypens, capitalization and other very minor differences), return the itemCode value, set completeMapping in the item JSON object to True.
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
Columns: item code|item description|itemUom|item category|currency|unit price|item status|last modified
1|melon|KG|fruit|RM|3.5|active|28/3/2025
6|Ipad Pro 11 WF CL 256GB\nSP BLK ITP|EA|003-COMPUTER, SMARTPHONE OR TABLET|RM|4913|active|31/3/2025
10|APPLE WATCH MAG FAST CHARGER\nUSBC 1M-ITS|EA|003-\nCOMPUTER,\nSMARTPHONE\nOR TABLET|RM|122|active|2/4/2025
11|THUNDERBOLT 3 TO THUNDERBOLT 2\nADPTR-ITP|EA|003-\nCOMPUTER,\nSMARTPHONE\nOR TABLET|RM|217|active|2/4/2025
12|HP 285 Silent WRLS Mouse A/P|EA|003-\nCOMPUTER,\nSMARTPHONE\nOR TABLET|RM|500|active|2/4/2025
</database>

<input>
[{{"item_list_id": "item_0", "description": "IPAD PRO 11 WF CL 256GB SP BLK-ITP", "unitPrice": 4913, "itemUom": "EA", "quantity": 4, "totalPrice": 19652, "taxAmount": -}}, 
{{"item_list_id": "item_1", "description": "HP 266 Mouse", "unitPrice": 230, "itemUom": "KG", "quantity": 4, "totalPrice": 920, "taxAmount": 0}},
{{"item_list_id": "item_2", "description": "HP 284 Silent WRLS Mouse A/P", "unitPrice": 500, "uoitemUom": "EA", "quantity": 4, "totalPrice": 2000, "taxAmount": -}}]
</input>

<output>
[{{"item_list_id": "item_0", "description": "Ipad Pro 11 WF CL 256GB SP BLK ITP", "unitPrice": 4913, "itemUom": "EA", "quantity": 4, "totalPrice": 19652, "taxAmount": 0, "itemCode": "1", "completeMapping": true, "status": "Success", "exceptionStatus": "N/A"}}, 
{{"item_list_id": "item_1", "description": "HP 266 Mouse", "unitPrice": 230, "itemUom": "KG", "quantity": 4, "totalPrice": 920, "taxAmount": 0, "itemCode": "-", "completeMapping": false, "status": "Exceptions", "exceptionStatus": "Item with description, unit price and uom does not exists in master files"}},
{{"item_list_id": "item_1", "description": "HP 284 Silent WRLS Mouse A/P", "unitPrice": 200, "itemUom": "EA", "quantity": 4, "totalPrice": 2000, "taxAmount": 0, "itemCode": "-", "completeMapping": false, "status": "Exceptions", "exceptionStatus": "Item description does not exists in master files"}}]
</output>

IMPORTANT NOTES ON MAPPING:
- Only standardize the item description with the database version when there is a legitimate match.
- When mapping descriptions, be lenient with the following differences:
  * Capitalization (e.g., "IPAD" vs "iPad")
  * Spaces vs. hyphens (e.g., "SP BLK-ITP" vs "SP BLK ITP")
  * Extra spaces or line breaks
  * Special characters like "\n" or trailing/leading spaces

- However, be strict with:
  * Product model numbers (e.g., "HP 284" vs "HP 285" should NOT match)
  * Product sizes or capacities (e.g., "256GB" vs "512GB" should NOT match)
  * Color variations (e.g., "BLK" vs "WHT" should NOT match)
  * Different product lines (e.g., "Pro" vs "Air" should NOT match)

- When comparing descriptions, parse and compare each meaningful component:
  * For electronics, match the exact model number
  * For consumables, match the exact product specifications
  * For general items, ensure core identifying characteristics match

- DO NOT MODIFY and DO NOT CONSIDER item_list_id field in the input JSON object for the mapping process.
- BUT if "taxAmount" is presented as '-', set it to 0

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
    "unitPrice": "10.00",
    "itemUom": "KG",
    "quantity": "2",
    "totalPrice": "20.00",
    "taxAmount": "2.00",
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
    
    po["lineItem"] = masterMappedItems
    
    return po, input_tokens, output_tokens

@tracer.capture_method
def performMasterDataMapping(po, database):
    input_item = {
        "supplierName": po.get("buyerName"),
        "supplierAddress": po.get("buyerAddress"),
        "poDate": po.get("poDate"),
        "paymentTerms": po.get("paymentTerms"),
        "requestDeliveryDate": po.get("requestDeliveryDate"),
        "currency": po.get("currency"),
        "buyerName": po.get("supplierName"),
        "buyerAddress": po.get("supplierAddress"),
        "deliveryAddress": po.get("deliveryAddress") or po.get("buyerAddress")
    }

    prompt_data = f"""
TASK:
Analyze the supplier in the input Json Object and map them to the correct entities in the <database> tags.
You are provided databases within the <database> tags to cross-reference the entities.
Standardize the supplier name to match the database format.
Standardize the dueDate to that of YYYY-MM-DD format.
If paymentTerms are clear, Standardize the paymentTerms to that of "NET n" format.
If paymentTerms are complex like "1.5% cd 2d/0.75%cd 14d/30d net", return as it is

IMPORTANT:
- Different entities can be closely related by name but they are not the same, ENSURE you are mapping the correct entities and returning the correct codes.
- If you are unsure about a mapping, please return "-" as the value.

For formFields, map the Buyer name and address to the correct company entities in the database and set the "buyerCode" with the values from the database.
scenarios:
1. If you can map both company name and address completely:
   a. Set supplierCode to the code value from the database
   b. Set completeMapping to true
   c. Set status in formFields to "Success"
   d. Set exceptionStatus in formFields to "N/A"

2. If you can map company name but not address:
   a. Set supplierCode to the code value from the database
   b. Set completeMapping to false
   c. Set status in formFields to "Exceptions"
   d. Set exceptionStatus to "Address did not match the one tied to the vendor name"

3. If you can map address but not company name:
   a. Set supplierCode to "-"
   b. Set completeMapping to false
   c. Set status in formFields to "Exceptions"
   d. Set exceptionStatus to "Vendor with name not found in the database"

4. If you cannot map either company name or address:
   a. Set supplierCode to "-"
   b. Set completeMapping to false
   c. Set status in formFields to "Exceptions"
   d. Set exceptionStatus to "Vendor with name and address not found in the database"

5. If you can map both company name and address completely but the vendor code is duplicated:
    a. Set supplierCode to the code value from the database
    b. Set completeMapping to false
    c. Set status in formFields to "Exceptions"
    d. Set exceptionStatus to "Duplicate buyer code found in the database"

6. If you can map both company name and address completely but the vendor code is "-":
    a. Set supplierCode to "-"
    b. Set completeMapping to false
    c. Set status in formFields to "Exceptions"
    d. Set exceptionStatus to "Missing buyer code in the database"

After mapping the formFields and setting status and exceptionStatus, check the lineItem array's status and exceptionStatus and update the formFields status and exceptionStatus fields accordingly.
scenarios:
1. If all line items AND formFields are mapped correctly:
   - formFields status remains "Success" and exceptionStatus remains "N/A"

2. If formFields have mapping exceptions (status is "Exceptions"):
   - formFields status MUST remain "Exceptions" regardless of line item status
   - Keep existing formFields exceptionStatus

3. If formFields are mapped correctly but ANY line item has mapping exceptions:
   - Change formFields status to "Exceptions"
   - Generate a new exceptionStatus message that describes the line item issues

4. If both formFields AND any line items have mapping exceptions:
   - formFields status remains "Exceptions"
   - Update exceptionStatus to include both vendor and line item issues

DATABASE TEXT:
{database}

EXTRACTED PO TEXT:
<input>
{input_item}
</input>

OUTPUT FORMAT:
You STRICTLY only the return the response in the example JSON object below, you are NOT ALLOWED to deviate from this format. DO NOT PROVIDE ANY OTHER DETAILS:
You MUST NOT remove any fields in the JSON object, even if you are unable to map them.
You MUST NOT remove the boundingBoxes field in the JSON object.
{{
    "supplierName": "supplier001",
    "supplierAddress": "supplierAddress",
    "supplierCode": "supplierCode001",
    "buyerName": "buyer001",
    "buyerAddress": "buyerAddress",
    "deliveryAddress": "deliveryAddress",
    "poDate": "yyyy-mm-dd",
    "paymentTerms": "NET n",
    "requestDeliveryDate": "yyyy-mm-dd",
    "currency": "MYR",
    "status": "Success"/"Exceptions",
    "exceptionStatus": "N/A"/error message,
}}

EXAMPLE:
<database>
Columns: code|name|address|status|last modified\n\nc1|Company A Sdn Bhd|Subang Jaya|active|28/3/2025\nc2|Mickey LowYat Co.|Kuala Lumpur|active|31/3/2025
</database>

INPUT:
{{
    "supplierName": "MICKEY LOWYAT CO",
    "supplierAddress": "Kuala Lumpur",
    "buyerName": "JANAKUASA SDN BHD",
    "buyerAddress": "Kuala Lumpur",
    "poDate": "1-2-2025,
    "requestDeliveryDate": "15-2-2025",
    "deliveryAddress": "KOTA KINABALU",	
    "currency": "RM",
    "paymentTerms": "30 Days",
}}

OUTPUT:
{{
    "supplierName": "Mickey LowYat Co.",
    "supplierAddress": "Kuala Lumpur",
    "supplierCode": "c2",
    "buyerName": "Janakuasa Sdn Bhd",
    "buyerAddress": "Kuala Lumpur",
    "poDate": "01-02-2025",	
    "paymentTerms": "NET 30",
    "requestDeliveryDate": "15-02-2025",	
    "deliveryAddress": "Kota Kinabalu",
    "currency": "MYR",
    "status": "Success",
    "exceptionStatus": "N/A",
}}

IMPORTANT:
- Different entities can be closely related by name but they are not the same, ENSURE you are mapping the correct entities and returning the correct codes.
- If you are unsure about a mapping, please return "-" as the value.
- If you are unable to map the supplier code, please return "-" as the value.
- If there is no payment terms provided, return "-" as the value.
- If the currency is "RM", return "MYR" as the value.
- If supplierName, buyerName, supplierAddress, buyerAddress or deliveryAddress are written in all caps, convert them to title case (e.g., "MICKEY LOWYAT CO" -> "Mickey LowYat Co.").

- Standardize the following address:

    Input Address:
    Concept Computer Hub No 71 & 73, 
    Jalan Intan 4, Bandar Baru 36000 Teluk Intan, Perak. 016-5511515 (Kok Yong)


Standardize it to:
- Title Case for names and street names
- Single line format (no line breaks)
- Remove trailing or duplicate commas
- Keep postal codes and state names as is

    Output:
    "Concept Computer Hub No 71 & 73, Jalan Intan 4, Bandar Baru, 36000 Teluk Intan, Perak"
"""

    po_supplier_mapping, input_tokens, output_tokens = promptBedrock(prompt_data)
    json_po_supplier_mapping = json.loads(po_supplier_mapping)

    po['buyerName'] = json_po_supplier_mapping.get("supplierName")
    po['buyerAddress'] = json_po_supplier_mapping.get("supplierAddress")
    po['buyerCode'] = json_po_supplier_mapping.get("supplierCode")
    po['supplierName'] = json_po_supplier_mapping.get("buyerName")
    po['supplierAddress'] = json_po_supplier_mapping.get("buyerAddress")
    po['poDate'] = json_po_supplier_mapping.get("poDate")
    po['paymentTerms'] = json_po_supplier_mapping.get("paymentTerms")
    po['requestDeliveryDate'] = json_po_supplier_mapping.get("requestDeliveryDate")
    po['deliveryAddress'] = json_po_supplier_mapping.get("deliveryAddress")
    po['currency'] = json_po_supplier_mapping.get("currency")  
    po['exceptionStatus'] = json_po_supplier_mapping.get("exceptionStatus")

    return po, input_tokens, output_tokens

@tracer.capture_method
def performDuplicateChecking(po, merchantId):
    # for mappedJson in mappedJsonData:
    poResp = EXTRACTED_DOCUMENT_DDB_TABLE.query(
        IndexName='gsi-merchantId-poNumber',
        KeyConditionExpression=Key('merchantId').eq(merchantId)&Key('poNumber').eq(po.get('poNumber')),
        FilterExpression=Attr('documentType').eq("po")
    ).get('Items', [])

    if poResp:
        po["isDuplicate"] = True
    else:
        po["isDuplicate"] = False
    
    return po

@tracer.capture_method
def createExtractedResultRecord(poData, merchantId, documentUploadId, source_file_name, file_path, now):
    extractedPoId = str(uuid.uuid4())
    deliveryAddress = poData.get("deliveryAddress") or poData.get("buyerAddress")
    
    for item in poData.get("lineItem", []):
        unitPrice = Decimal(str(item.get("unitPrice") or 0))
        quantity = Decimal(str(item.get("quantity") or 0))
        rawTaxRate = Decimal(str(poData.get("taxRate") or 0))

        # convert taxAmount into percentage
        taxRate = (rawTaxRate / Decimal('100'))

        # Calculate and round tax amount
        taxAmount = (taxRate * unitPrice * quantity).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        extractedDocumentLineItemPayload = {
            "extractedPoLineItemsId": str(uuid.uuid4()),
            "poNumber": poData.get("poNumber"),
            "itemCode": item.get("itemCode"),
            "description": item.get("description"),
            "unitPrice": unitPrice,
            "itemUom": item.get("itemUom"),
            "quantity": quantity,
            "totalPrice": item.get("totalPrice"), 
            "merchantId": merchantId,
            "extractedPoId": extractedPoId,
            "documentUploadId": documentUploadId,
            'boundingBoxes': item.get('boundingBoxes'),
            "exceptionStatus": item.get('exceptionStatus'),
            "taxAmount": taxAmount,
            "status": item.get('status'),
            "supplierName": poData.get("supplierName"),
            "buyerCode": poData.get("buyerCode"),
            "buyerName": poData.get("buyerName"),
            "currency": poData.get("currency"),
            "taxRate": rawTaxRate,
            "docStatus": poData.get("status"),
            "source": "email",
            "createdAt": now,
            "createdBy": "System",
            "updatedAt": now,
            "updatedBy": "System"
        }

        extractedDocumentLineItemPayload = convert_floats_to_decimals(extractedDocumentLineItemPayload)
        EXTRACTED_DOCUMENT_LINE_ITEM_DDB_TABLE.put_item(Item=extractedDocumentLineItemPayload)  

    extractedDocumentPayload = {
        "extractedPoId": extractedPoId,
        "merchantId": merchantId,
        "poNumber": poData.get("poNumber"),
        "poDate": poData.get("poDate"),
        "documentType": poData.get("documentType", "po"),
        "supplierName": poData.get("supplierName"),
        "buyerName": poData.get("buyerName"),
        "buyerCode": poData.get("buyerCode"),
        "buyerAddress": poData.get("buyerAddress"),
        "supplierAddress": poData.get("supplierAddress"),
        "deliveryAddress": deliveryAddress,
        "requestDeliveryDate": poData.get("requestDeliveryDate"),
        "paymentTerms": poData.get("paymentTerms"),
        "currency": poData.get("currency"),
        "totalAmountWithTax": poData.get("totalAmountWithTax", 0),
        "totalAmountWithoutTax": poData.get("totalAmountWithoutTax", 0),
        "taxType": poData.get("taxType"),
        "taxRate": poData.get("taxRate"),
        "totalTaxAmount": poData.get("totalTaxAmount", 0),
        "documentStatus": poData.get("status"),
        'boundingBoxes': poData.get('boundingBoxes'),
        "exceptionStatus": poData.get('exceptionStatus'),
        "source": "email",
        "filePath": file_path,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "sourceFile": source_file_name,
        "confidenceScore": round(poData.get("confidenceScore", 0)*100),
        "documentUploadId": documentUploadId
    }

    extractedDocumentPayload = convert_floats_to_decimals(extractedDocumentPayload)
    EXTRACTED_DOCUMENT_DDB_TABLE.put_item(Item=extractedDocumentPayload)
    poData["extractedPoId"] = extractedPoId

    return poData

@tracer.capture_method
def createTimelineRecord(merchantId, poData, now):
    if 'approvalStatus' in poData:
        if poData['approvalStatus'] == "APPROVED":
            title = "approved"
            description = "PO approved"
        else:
            title = "rejected"
            description = poData.get('rejectionReason', "PO rejected")
    elif poData['status'] == "Success":
        title = "Document Processed"
        description = "Document extracted successfully"
    else:
        title = "Document Processing Failed"
        description = poData.get('exceptionStatus')
    
    timelinePayload = {
        "timelineId": str(uuid.uuid4()),
        "merchantId": merchantId,
        "timelineForId": poData.get("extractedPoId"),
        "title": title,
        "type": poData.get("documentType", "po"),
        "description": description,
        "createdAt": now,
        "createdBy": "System",
        "updatedAt": now,
        "updatedBy": "System",
        "poNumber": poData.get("poNumber", "-"),
        "buyerName": poData.get("buyerName", "-")
    }
    TIMELINE_DDB_TABLE.put_item(Item=timelinePayload)

@tracer.capture_method
def documentUploadStatusCheck(document_upload_id):
    # query all exceptionStatus from extractedDocuments with documentUploadId
    # and return the exceptionStatus and status based on the extractedDocuments' exceptionStatus and status

    time.sleep(3) # wait for 3 seconds to ensure all records are inserted

    all_extracted_documents = EXTRACTED_DOCUMENT_DDB_TABLE.query(
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
   a. If ANY statuses contain mention of duplicate POs, set exception status to "Duplicate Error" (HIGHEST PRIORITY)
   b. If ANY statuses indicate master data mapping failures, set exception status to "Master Mapping Error" (SECOND PRIORITY)
   c. If ANY statuses indicate missing fields or data errors, set exception status to "Missing Field Error" (THIRD PRIORITY)
   d. If ALL statuses are "None" or indicate no issues, set exception status to "N/A" (LOWEST PRIORITY)

2. High-Level Status Determination:
   a. Set status to "Fail" if exception status is "Duplicate Error" or "Master Mapping Error"
   b. Set status to "Pending Review" if exception status is "Missing Field Error"
   c. Set status to "Success" if exception status is "N/A"

EXAMPLES:
- Input: ["Duplicate PO Number Found", "Missing required field values"]
  Output: {{"exceptionStatus": "Duplicate Error", "status": "Fail"}}

- Input: ["Master Data Mapping Failed"]
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
    

