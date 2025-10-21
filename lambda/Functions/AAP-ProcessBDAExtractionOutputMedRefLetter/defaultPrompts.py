EXCEPTION_STATUS_CHECKING_PROMPT = """
TASK: 
Check the invoice data for missing fields and apply exception rules.

INSTRUCTIONS:
ON THE LINE ITEM LEVEL:
1. Check each line item for the following:
   - If itemCode is "-", set status to "Exceptions" and exceptionStatus to "Master Mapping Error on line item"
   - check missingFieldException, note "Missing field values (<field names>) if there are any missing fields specified in the missingFieldException"
   - Set completeMapping to True only if itemCode is not "-" and all fields are matched
   - Set completeMapping to False if itemCode is "-" or there are any partial matches
2. Update the status and exceptionStatus for each line item:
   - If no exceptions, set status to "Success" and exceptionStatus to "N/A"
   - If exceptions exist, set status to "Exceptions" and include detailed exceptionStatus message

ON THE DOCUMENT LEVEL:
1. Check for all of the following conditions (they are not mutually exclusive):
   - If 'isDuplicate' flag is True, note "Duplicate invoice detected" (highest priority)
   - If there are any master data mappings incomplete, generate an error message stating why (second priority)
   - check amountException, note "Sum of line item amounts does not match the total invoice amount" (third priority)
   - check missingFieldException, note "Missing field values (<field names>) if there are any missing fields specified in the missingFieldException"" (last priority)
   - the error message must follow the priority

2. Generate a clear, concise message that describes all identified issues on document level and line item level
3. Ensure the message is human-readable and accurately reflects all exceptions.
4. If no exceptions are found, set exceptionStatus to "N/A" and status to "Success"

EXAMPLE MESSAGES:
- "Incomplete master data mapping for vendor and multiple line items and is missing required field values (supplierName, ...)"
- "Duplicate invoice detected in the system"
- "Duplicate invoice detected, master data mapping incomplete for line item itemDescription1 and missing field values (invoiceDate)"
- "Vendor name and address was not found in the master files"
- "Incomplete master data mapping for multiple line items and sum of line item amounts does not match the total invoice amount"
- "N/A"

INPUT DATA:
{invoiceData}

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "supplierName": "AXY Sdn. Bhd.",
    ...
    "status": "Exceptions"/"Success",
    "exceptionStatus": "Descriptive message explaining all issues found or N/A"
    "lineItem":[{{
        "item_list_id": "item_0",
        "description": "item001",
        "itemCode": "code001",
        "unitPrice": "10.00",
        "uom": "KG",
        "quantity": "",
        "totalPrice": "",
        "status": "Success" or "Exceptions",
        "exceptionStatus": "N/A" or updated error message for the item,
        "completeMapping": true/false
    }},
    {{...}}]
}}
"""

DOCUMENT_UPLOAD_STATUS_CHECK_PROMPT = """
TASK: Categorize a list of document exception statuses into a single exception status and provide a high-level status based on priority ranking.

INPUT:
A list of exception statuses from multiple documents in the following format:
["exception status 1", "exception status 2", "exception status 3", ...]

CATEGORIZATION RULES WITH PRIORITY RANKING:
1. Exception Status Determination (in order of priority):
   a. If ANY statuses contain mention of duplicate invoices, set exceptionStatus to "Duplicate Error" (HIGHEST PRIORITY)
   b. If ANY statuses indicate master data mapping failures, set exceptionStatus to "Master Mapping Error" (SECOND PRIORITY)
   c. If ANY statuses indicate amount mismatches, set exceptionStatus to "Amount Mismatch Error" (THIRD PRIORITY)
   d. If ANY statuses indicate missing fields or data errors, set exceptionStatus to "Missing Field Error" (FORTH PRIORITY)
   e. If ANY statuses indicate "Document Type Unrecognised" AND there are processable documents with other errors, prioritize the error status above
   f. If ALL statuses are either "Success", "Document Type Unrecognised", or "N/A", determine based on processable documents:
      - If there are any "Success" statuses (meaning some invoices were processed successfully), set exceptionStatus to "N/A"
      - If ALL statuses are "Document Type Unrecognised" (no invoices found), set exceptionStatus to "Document Type Unrecognised"

2. High-Level Status Determination:
   a. Set status to "Fail" if exception status is "Duplicate Error" or "Master Mapping Error"
   b. Set status to "Pending Review" if exception status is "Missing Field Error" or "Document Type Unrecognised"
   c. Set status to "Success" if exception status is "N/A"

EXAMPLES:
- Input: ["Duplicate Invoice Number Found", "Missing required field values", "Could Not Map Vendor"]
  Output: {{"exceptionStatus": "Duplicate Error", "status": "Fail"}}

- Input: ["Success", "Document Type Unrecognised"]
  Output: {{"exceptionStatus": "N/A", "status": "Success"}}
  
- Input: ["Document Type Unrecognised", "Document Type Unrecognised"]
  Output: {{"exceptionStatus": "Document Type Unrecognised", "status": "Pending Review"}}

- Input: ["Missing required field values", "Document Type Unrecognised"]
  Output: {{"exceptionStatus": "Missing Field Error", "status": "Pending Review"}}

- Input: ["Success", "Success"]
  Output: {{"exceptionStatus": "N/A", "status": "Success"}}

- Input: ["Missing required field values", "Master Data Mapping Failed"]
  Output: {{"exceptionStatus": "Master Mapping Error", "status": "Fail"}}

- Input: ["Missing required field values", "Document Type Unrecognised", "Duplicate Invoice Found In System", "Success"]
  Output: {{"exceptionStatus": "Duplicate Error", "status": "Exceptions"}}

- Input: ["Sum of line item amounts does not match the total invoice amount", "Missing required field values"]
   Output: {{"exceptionStatus": "Amount Mismatch Error", "status": "Pending Review"}}

- Input: ["Document Format Unrecognised", "Document Format Unrecognised"]
   Output: {{"exceptionStatus": "Document Type Unrecognised", "status": "Pending Review"}}

EXPECTED OUTPUT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
{{
  "exceptionStatus": [one of: "Duplicate Error", "Master Mapping Error", "Missing Field Error", "Document Type Unrecognised", "Amount Mismatch Error", "N/A"],
  "status": [one of: "Fail", "Pending Review", "Success"]
}}

INPUT:
{all_statuses}
"""

LINE_ITEM_MASTER_MAPPING_PROMPT = """
TASK:
Map input items to database items based on description, uom and unit price. Return the matching itemCode from database.
You MUST NOT make up any itemCode. Only get itemCode from the database, if unable to map, return "-" as the itemCode and set status/exceptionStatus/completeMapping accordingly.
set completeMapping to True when all fields of an item is matched, set completeMapping to False if no fields match or partial match.
swap any occurance of " in item description to '.

There are two mapping principles that are mutually exclusive:
DIRECT TEXT MATCHING and SEMANTIC MATCHING

DIRECT TEXT MATCHING PRINCIPLES:
- Use for PRODUCT-SPECIFIC ITEMS (electronics, specific models, equipment)
- Match requires high precision on model numbers, specifications, and identifiers
- Compare descriptions after normalizing:
  * Convert to lowercase
  * Remove extra spaces
  * Replace hyphens with spaces or vice versa
  * Remove special characters (\n, trailing/leading spaces)
- Must match on UOM (unit of measure).
- Before matching on UOM, check if the UOM is compatible within acceptable ranges. Examples are as below:
    * EA (each) should match with EA, PCS, PCE
    * MTH (month) should match with MTH, MO
    * KG (kilogram) should match with KG, KGS
    * LTR (liter) should match with LTR, LTRS, L
    * CTN (carton) should match with CAR, CARTON
- Unit price should be identical or very close (within 1%)
- NEVER match different product models, versions, or specifications:
  * "iPad Pro 2016" should NOT match with "iPad Pro 2018"
  * "HP 244 Silent" should NOT match with "HP 285 Silent"
  * "256GB" should NOT match with "512GB"

SEMANTIC MAPPING PRINCIPLES:
- Use for CATEGORY-BASED ITEMS (utilities, rentals, services, consumables)
- Match based on conceptual category and service type, not specific models
- Examples of valid semantic matches:
  * "Water usage (1.1.2025-1.3.2025)" should map to "Utility - Water & Sewerage"
  * "Monthly rent payment for warehouse" should map to "Rental - Storage"
  * "MS Office subscription" should map to "IT Software"
- For utilities: Match by service type (electricity, water) regardless of billing period
- For rentals: Match by rental type (storage, office space) regardless of specific location
- For IT services: Match by service category (software, maintenance) regardless of specific product
- UOM may differ but should be conceptually compatible (e.g., EA vs MTH for services)

MAPPING DECISION TREE:
1. For product-specific items (electronics, equipment with model numbers):
   - ALWAYS use DIRECT TEXT MATCHING
   - Require exact matches on specifications and model numbers
   - If any specification differs, return "-" as itemCode

2. For category-based items (utilities, services, rentals):
   - ALWAYS use SEMANTIC MATCHING
   - Match by service type and category
   - Ignore specific dates, quantities, or billing periods

EXAMPLE DATABASE:
Columns: item code|item description|uom|item category|currency|unit price|item status|last modified
6|Ipad Pro 11 WF CL 256GB SP BLK ITP|EA|003-COMPUTER, SMARTPHONE OR TABLET|RM|4913|active|31/3/2025
200|Electricity|EA|001-UTILITY|RM|0|active|4/2/2025
201|Utility - Water & Sewerage|EA|001-UTILITY|RM|0|active|4/2/2025
300|Rental - Storage|EA|004-RENTAL|RM|5000|active|4/2/2025
400|IT Software|MTH|002-IT SERVICES|RM|2000|active|4/2/2025

<input>
[{{"item_list_id": "item_0", "description": "IPAD PRO 11 WF CL 256GB SP BLK-ITP", "unitPrice": 4913, "uom": "EA", "quantity": 4, "totalPrice": 19652}}, 
{{"item_list_id": "item_1", "description": "HP 244 Silent", "unitPrice": 500, "uom": "EA", "quantity": 4, "totalPrice": 2000}},
{{"item_list_id": "item_2", "description": "31.10.2024-Electricity Charge ( 4329 kWh X C.T 1.00 )", "unitPrice": "", "uom": "EA", "quantity": 1, "totalPrice": 2100}},
{{"item_list_id": "item_3", "description": "RENT CHARGE FOR STORAGE SPACE", "unitPrice": 5000, "uom": "MTH", "quantity": 1, "totalPrice": 5000}}]
</input>

EXAMPLES OF MAPPING:

"IPAD PRO 11 WF CL 256GB SP BLK-ITP" → itemCode: "6", status: "Success"
  - DIRECT TEXT MATCHING: All specifications match exactly after normalizing spaces/hyphens

"31.10.2024-Electricity Charge ( 4329 kWh X C.T 1.00 )" → itemCode: "200", status: "Success"
  - SEMANTIC MATCHING: Matches utility category "Electricity" regardless of date/consumption details

"RENT CHARGE FOR STORAGE SPACE" → itemCode: "300", status: "Success"
  - SEMANTIC MATCHING: Matches rental category for storage

"HP 244 Silent" when database has "HP 285 Silent WRLS Mouse" → itemCode: "-", status: "Exceptions", exceptionStatus: "Master Mapping Error on line item"
  - DIRECT TEXT MATCHING REQUIRED: Different model numbers (244 vs 285) should not match

<output>
[{{"item_list_id": "item_0", "description": "Ipad Pro 11 WF CL 256GB SP BLK ITP", "unitPrice": 4913, "uom": "EA", "quantity": 4, "totalPrice": 19652, "itemCode": "6", "status": "Success", "exceptionStatus": "N/A", "completeMapping":True}}, 
{{"item_list_id": "item_1", "description": "HP 244 Silent", "unitPrice": 500, "uom": "EA", "quantity": 4, "totalPrice": 2000, "itemCode": "-", "status": "Exceptions", "exceptionStatus": "Master Mapping Error (line item description)", "completeMapping":False}},
{{"item_list_id": "item_2", "description": "31.10.2024-Electricity ( 4329 kWh X C.T 1.00 )", "unitPrice": "", "uom": "EA", "quantity": 1, "totalPrice": 2100, "itemCode": "200", "status": "Success", "exceptionStatus": "N/A", "completeMapping":True}},
{{"item_list_id": "item_3", "description": "RENT CHARGE FOR STORAGE SPACE", "unitPrice": 5000, "uom": "MTH", "quantity": 1, "totalPrice": 5000, "itemCode": "300", "status": "Exceptions", "exceptionStatus": "Item description matched but uom didnt match", "completeMapping":False}}]
</output>

REAL INPUT:
<database>
{database}
</database>

<input>
{formatted_items}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.

[{{
    "item_list_id": "item_0",
    "description": "item001",
    "itemCode": "code001",
    "unitPrice": "10.00",
    "uom": "KG",
    "quantity": "2",
    "totalPrice": "20.00",
    "status": "Success" or "Exceptions",
    "exceptionStatus": "N/A" or error message,
    "completeMapping": true/false
}},
{{...}}
]
"""

STANDARDIZATION_PROMPT = """
TASK: You are to standardize the input JSON data

<input>
{invoiceData}
</input>

STEP 1: STANDARDIZE FIELDS
fields and their standardization instructions:
    - currency: standardize to the three letter currency code (e.g. MYR, USD, SGD)
    - all dates: standardize to YYYY-MM-DD format
    - paymentTerms:
        - If clear terms like "30 Days" with no conditions, standardize to "NET 30" format
        - If complex terms like "1.5% cd 2d/0.75%cd 14d/30d net" with conditions, return as is

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "dueDate": "2025-03-15",
    "currency": "SGD",
    "paymentTerms": "NET 7",
    "supplierName": "AXY Sdn. Bhd.",
    ...,
    "lineItem":[...]    
}}
"""

STORE_MASTER_MAPPING_PROMPT = """
TASK:
Map the premise address in the input to the correct store entities in the database and set the locationCode field.

STORE MAPPING RULES:
For premiseAddress field, map to the correct store entities in the database and set the "locationCode" with the store code from the database.
Return "-" in locationCode if no store from database matches.

MAPPING SCENARIOS:
1. If you can map premise address completely:
   a. Set locationCode to the code value from the database
   b. Set completeMapping to true
   c. Set status to "Success"
   d. Set exceptionStatus to "N/A"

2. If you can only match part of the premise address:
   a. Set locationCode to the code value from the database
   b. Set completeMapping to true
   c. Set status to "Success"
   d. Set exceptionStatus to "Premise address partially matched with store in database"

3. If you cannot find any match for the premise address:
   a. Set locationCode to "-"
   b. Set completeMapping to false
   c. Set status to "Exceptions"
   d. Set exceptionStatus to "Store with address not found in the database"

DATABASE:
<database>
{database}
</database>

INPUT:
<input>
{input_item}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "storeName": "Store name from database",
    "locationCode": "0234",
    "status": "Success"/"Exceptions",
    "exceptionStatus": "N/A"/error message,
    "completeMapping": True/False
}}
"""

VENDOR_MASTER_MAPPING_PROMPT = """
TASK:
1. Analyze the supplier in the input JSON Object and map them to the correct entities in the <database> tags.
    a. You are provided databases within the <database> tags to cross-reference the entities.

SUPPLIER MAPPING RULES:
For formFields, map the Supplier name and address to the correct company entities in the database and set the "supplierCode" with the values from the database.
Return "-" in supplierCode if no company from database matches.

IMPORTANT:
- Different entities can be closely related by name but they are not the same, ENSURE you are mapping the correct entities and returning the correct codes.
- If you are unsure about a mapping, please return "-" as the value.

MAPPING SCENARIOS:
1. If you can map both company name and address completely:
   a. Set supplierCode to the code value from the database
   b. Set supplierName to the name from the database
   c. Set completeMapping to true
   d. Set status in formFields to "Success"
   e. Set exceptionStatus in formFields to "N/A"

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

OVERALL STATUS DETERMINATION:
After mapping formFields and setting their status and exceptionStatus, check the lineItem array's status and exceptionStatus and update the formFields status and exceptionStatus fields accordingly:

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

IMPORTANT: If vendor master mapping results in "Exceptions", the overall status MUST be "Exceptions" even if all line items are successfully mapped.

DATABASE:
<database>
{database}
</database>

INPUT:
<input>
{input_item}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "supplierName": "supplier001",
    "supplierAddress": "supplierAddress",
    "supplierCode": "supplierCode001",
    "status": "Success"/"Exceptions",
    "exceptionStatus": "N/A"/error message,
    "completeMapping": True/False
}}
"""