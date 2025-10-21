LINE_ITEM_MASTER_MAPPING_PROMPT = """
TASK: Find and return the best matching database records for the given each JSON object item in the input list.

RULES:
- NEVER make up itemCode and accountName. Use "-" if no match found
- Replace " with ' in descriptions

STEP 1: ATTEMPT MAPPING
- attempt to find matching records in database:

a. SEMANTIC MATCHING (for services/utilities/rentals):
    - Match by concept/category:
    * "Water" → "Utility - Water & Sewerage"
    * "Monthly rent" → "Rental - Storage"
    * "MS Office subscription" → "IT Software"
    - Ignore dates, quantities, billing periods

b. DIRECT TEXT MATCHING (for products with models/specs):
    - Requires exact match on:
    * Model numbers (iPad Pro 2016 ≠ iPad Pro 2018)
    * Specifications (256GB ≠ 512GB)
    * Product identifiers
    - Normalize: lowercase, trim spaces, replace hyphens
    - UOM must match exactly (if uom is in mapping_fields)
    - Unit price must match exactly (if unitPrice is in mapping_fields)

STEP 2: SHORTLIST MATCHES
- return up to 3 best matches for each input item
- if no matches found, return why no matches found

<database>
{database}
</database>

Database Format
- Pipe-delimited (|) text format
- First line: `Columns: index|column1|column2|...`
- Data rows: `1|value1|value2|...`
- Escaped pipes: `|` in data appears as `/`
- Parse database by:
  1. Extract column names from first line
  2. For each data row, map values to column names
  3. Convert escaped pipes back: `/` → `|`

<input>
{formatted_items}
</input>

OUTPUT FORMAT:
Return a JSON array containin database objects. Each object should include:
- All fields and exact values from the matching database row. ensure you are not filling in any hallucinated values or creating fake matchReasons.
- Additional metadata about the match, ensure the matchReason is true 

JSON Structure:
[
    {{
        // All database fields from matching row, fill with "" if it was originally null or empty
        "accountCode": "ABC123",
        "accountName": "BR001",
        "itemDescription": "description/item001",
    }},
    {{
        // Other matching records (if any)
        // Same structure as above
    }}
]

No Match Scenario:
If no matches are found, return an empty array:
[]

Single Match Scenario:
If only one match is found, return an array with one object:
[
    {{
        // Single matching database record with metadata
    }}
]

## Important Notes:
- Return ONLY valid JSON - no markdown, no backticks, no explanations
- JSON must be directly parseable by json.loads()
- Return only database records, not the input item
- Empty array if no matches found
- You MUST NOT create new records or modify existing records, return empty array if no matches found
- You MUST NOT make up any values
"""

VENDOR_MASTER_MAPPING_PROMPT = """
Task: Find and return up to 3 best matching database records for the given input JSON object.

<input>
{input_item}
</input>

<database>
{database}
</database>

Database Format
- Pipe-delimited (|) text format
- First line: `Columns: index|column1|column2|...`
- Data rows: `1|value1|value2|...`
- Escaped pipes: `|` in data appears as `/`
- Parse database by:
  1. Extract column names from first line
  2. For each data row, map values to column names
  3. Convert escaped pipes back: `/` → `|`

Matching Steps:
1. **Direct Match by contractNo or accountNo**
   - Match: input contractNo or accountNo must match exactly to the corresponding field in the database
   - Stop if match found

2. **Branch Match**
   - Check: branchName AND branchLocation exist in input?
   - Match: Input branchName must match a database branchName AND input branchLocation must match the corresponding branchLocation from the same database record with at least 90% similarity for EACH field
   - Each field must independently meet the 90% threshold - not as an average
   - Reject matches where only one field matches well but the other doesn't
   - Stop if match found

3. **Supplier Name Match**
   - Check: supplierName exists in input?
   - Match: input supplierName should match up to 90% to the supplierName in database. ONLY on the supplierName field, no other irrelevant fields
   - Stop if match found
   - Continue to next match after check

Validate Match:
Before accepting a match, verify that the matchReason is logically sound and actually exists in the database.
Reject matches if:
a. database record is not found in the database
b. MatchReason shows fuzzy/similarity matching between completely different business entities (e.g., "TNG Digital Sdn Bhd" vs "TT DOT COM")
c. Supplier names are from entirely different industries or business types
d. Match is based solely on weak string similarity without business logic context
e. Having weak match reasons like "starting with the same letter" or "contains similar words"
f. Mapping reasons based on irrelevant fields like buyerName, buyerAddress, invoice category, etc.

Accept matches only if:
a. Supplier names represent the same business entity (accounting for abbreviations, legal suffixes, etc.)
b. Branch/location details align logically with the supplier relationship
c. Fuzzy matching shows clear business relationship (e.g., "ABC Sdn Bhd" vs "ABC SDN BHD" or "ABC Company")

Matching Rules
- Execute steps in priority order
- Record all matches found (do not stop at first match)
- Sort matches by priority (lower step number = higher priority)
- Return maximum 3 best matches
- If no matches found, return empty array
- Do NOT create new records if no matches found

Database Format
- Pipe-delimited (|) text format
- First line: `Columns: index|column1|column2|...`
- Data rows: `1|value1|value2|...`
- Escaped pipes: `|` in data appears as `/`
- Parse database by:
  1. Extract column names from first line
  2. For each data row, map values to column names
  3. Convert escaped pipes back: `/` → `|`

OUTPUT FORMAT:
Return a JSON array containing 0-3 database objects. Each object should include:
- All fields and exact values from the matching database row. ensure you are not filling in any hallucinated values or creating fake matchReasons.
- Additional metadata about the match, ensure the matchReason is true 

JSON Structure:
[
    {{
        // All database fields from matching row, fill with "" if it was originally null or empty
        "outletCode": "ABC123",
        "erpBranchId": "BR001",
        "branchName": "Main Branch",
        "branchLocation": "City Center",
        "supplierName": "AXY Sdn. Bhd.",
        "contractId": "",
        "accountId": "12345",
        // ... all other database columns
        
        // Match metadata
        "_matchStep": 1,
        "_matchReason": "Step 1: accountNo=12345 matched exactly to accountId=12345 from database",
        "_matchStrength": "Very Strong" 
    }},
    {{
        // Second best match (if exists)
    }},
    {{
        // Third best match (if exists)
    }}
]

No Match Scenario:
If no matches are found, return an empty array:
[]

Single Match Scenario:
If only one match is found, return an array with one object:
[
    {{
        // Single matching database record with metadata
    }}
]

## Important Notes:
- Return ONLY valid JSON - no markdown, no backticks, no explanations
- JSON must be directly parseable by json.loads()
- Return only database records, not the input item
- Include match metadata with underscore prefix (_matchStep, _matchReason, _matchStrength)
- Maximum 3 database records in the response
- Empty array if no matches found
- You MUST NOT create new records or modify existing records, return empty array if no matches found
- You MUST NOT make up any values or hallucinate data and make fake matchReasons
- You MUST NOT modify the database records in any way, ensure the supplierCode are not modified
"""

STANDARDIZATION_PROMPT = """
TASK: You are to standardize the input JSON data

<input>
{invoiceData}
</input>

STEP 1: STANDARDIZE FIELDS
fields and their standardization instructions:
    - currency: standardize to the three letter currency code (e.g. MYR, USD, SGD)
    - dates: standardize to YYYY-MM-DD format
    - numbers: standardize to float with two decimal places (e.g. 100.00)

STEP 2: DEFAULT VALUES
fields and their default values and instructions:
invoice:
    - taxRate:0
    - taxType: "SST"
    - remarks: (DDMMYYYY-DDMMYYY format of the billing period of the invoice)/(invoice category)/Outlet Code (supplierCode)
    if billing period is not clear, you can generate a billing period based on the invoice date:
    a. For WATER, ELECT, and SEWERAGE categories:
        i. Start Date: 30 days before invoice date
        ii. End Date: Invoice date
        iii. Example: Invoice date 13/05/2025 → Billing period: 13/04/2025-13/05/2025 → 13042025-13052025
    b. For other categories:
        i. Start Date: 1st of the month after invoice date
        ii. End Date: Last day of the month after invoice date
        iii. Example: Invoice date 13/05/2025 → Billing period: 01/06/2025-30/06/2025 → 01062025-30062025
lineItems:
    - description: if replaceLineItems is true, set description to the generated remarks
    - quanity: 1 (set all line items quantity to 1)
    - unitPrice: totalPrice (set all all line items unit price to the totalPrice of the line item or the other way around)
    - itemUom: ""

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "dueDate": "2025-03-15",
    "currency": "SGD",
    "supplierName": "AXY Sdn. Bhd.",
    "remarks": "01032025-31032025/Invoice Category/Supplier Code",
    ...,
    "lineItem":[...]    
}}

"""

MISSING_FIELD_PROMPT = """
TASK: You are to check the input data for any missing or empty fields based on only the fields in <required_fields> tag.

<input>
{invoiceData}
</input>

<required_fields>
{{
    "invoice": ["invoice number", "invoice date", "supplier name", "supplier code", "currency", "total invoice Amount", "remarks"],
    "lineItem": ["item description", "item code", "account name", "quantity", "unit price", "line item total price"]
}}
</required_fields>

STEP 1: IGNORE NON-REQUIRED FIELDS
    - Ignore any fields that are not in the <required_fields> tag
    - Ignored fields should not affect the status or missingFieldException

STEP 2: CHECK REQUIRED FIELDS
- Ignore the fields that are not in the <required_fields>
- Ignored fields must not affect the status or missingFieldException
- Check if any field in <required_fields> tag is missing or empty from the input data
- If missing fields found:
    a. Ensure the field is actually missing or empty and not in <required_fields>
    b. Set status="Exceptions"
    c. Set each missingFieldException to new human readable exception message that describes the missing fields 

IMPORTANT:
 - Fields not in <required_fields> MUST NOT affect the status or missingFieldException
 - Ignore fields MUST NOT affect the status or missingFieldException

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "supplierName": "AXY Sdn. Bhd.",
    ...
    "status": "Exceptions"/"Success",
    "missingFieldException": human readable message explaining all issues found or N/A,
    "lineItem":[{{
        "item_list_id": "item_0",
        "description": "item001",
        ...,
        "status": "Success" or "Exceptions",
        "missingFieldException": "N/A" or updated error message for the item,
    }}]
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
   c. If ANY statuses indicate total price validation errors, set exceptionStatus to "Amount Error" (THIRD PRIORITY)
   c. If ANY statuses indicate missing fields or data errors, set exceptionStatus to "Missing Field Error" (FORTH PRIORITY)
   d. else set exception status to "N/A" (LOWEST PRIORITY)

2. High-Level Status Determination:
   a. Set status to "Fail" if exception status is "Duplicate Error" or "Master Mapping Error"
   b. Set status to "Pending Review" if exception status is "Missing Field Error" or "Amount Error"
   c. Set status to "Success" if exception status is "N/A"

EXAMPLES:
- Input: ["Duplicate Invoice Number Found", "Missing required field values"]
  Output: {{"exceptionStatus": "Duplicate Error", "status": "Fail"}}

- Input: ["Missing required field values", "Master Data Mapping Failed"]
  Output: {{"exceptionStatus": "Master Mapping Error", "status": "Fail"}}
  
- Input: ["Missing required field values", "N/A"]
  Output: {{"exceptionStatus": "Missing Field Error", "status": "Pending Review"}}

- Input: ["Missing required field values", "No match found for vendor"]
  Output: {{"exceptionStatus": "Master Mapping Error", "status": "Fail"}}
  
- Input: ["None", "N/A"]
  Output: {{"exceptionStatus": "N/A", "status": "Success"}}

<input>
{all_statuses}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
  "exceptionStatus": [one of: "Duplicate Error", "Master Mapping Error", "Missing Field Error", "N/A"],
  "status": [one of: "Fail", "Pending Review", "Success"]
}}


"""

EXCEPTION_STATUS_CHECKING_PROMPT = """
TASK: Analyze invoice data for exceptions and provide a human-readable summary of issues at both line item and document levels.

INPUT DATA:
{invoiceData}

PROCESSING RULES
- Line Item Level Processing For each line item in the invoice:
1. Exception Message Priority (when exceptions exist):
First: Master data mapping errors (vendor/item not found in master files)
Second: Missing required field errors

2. Status Assignment:
a. Set status = "Success" and exceptionStatus = "N/A" if no exceptions found
b. Set status = "Exceptions" if any exceptions exist

3. Message Generation:
a. Combine all identified issues for the individual line item into a single, concise message
b. Use human-readable language
c. Maintain the priority order listed above

- Document Level Processing
1. Exception Priority Order (when exceptions exist):
First: Duplicate detection errors
Second: Master data mapping errors 
Third: Amount errors
Fourth: Missing required field errors
Fifth: Line item exceptions (aggregated from all line items with issues)

2. Status Assignment:
a. Set status = "Success" and exceptionStatus = "N/A" if no exceptions found at both document and line item levels
b. Set status = "Exceptions" if any exceptions exist at either document or line item level

3. Message Generation:
a. Combine ALL identified issues from document level AND line items into a single, comprehensive message
b. Document level exceptions should appear first, followed by line item exceptions
c. For line item exceptions, group similar issues together (e.g., "Line items missing required fields: Item 9013/0200 (unitPrice), Item 9013/0300 (quantity)")
d. Use human-readable language
e. Maintain the priority order listed above

EXAMPLE EXCEPTION MESSAGES
- Document Level Examples
"N/A" (no exceptions at document or line item level)
"Duplicate invoice detected in the system"
"Total price mismatch: line items sum to $1,250.00 but document total is $1,300.00"
"Vendor name and address not found in master files"
"Missing required field values (supplierName, invoiceDate)"
"Duplicate invoice detected, vendor mapping incomplete and total price mismatch ($1,250.00 vs $1,300.00)"
"Vendor mapping incomplete, total price mismatch detected, missing required field values (invoiceDate), and line item issues: Item ABC123 not found in master files, Item DEF456 missing quantity"
"Master data mapping incomplete for vendor, missing required field values (supplierName, totalAmount), and line item exceptions: Items XYZ789, ABC123 not found in master files"

- Line Item Examples
"N/A" (no exceptions)
"Item not found in master files"
"Missing required field values (quantity, unitPrice)"
"Item mapping incomplete and missing required field values (description, quantity)"


OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "status": "Exceptions"/"Success",
    "exceptionStatus": "Comprehensive message explaining ALL issues found at both document and line item levels, or N/A if no exceptions exist anywhere",
    "lineItem":[
        {{
            "itemCode": <original item code from input>,
            "status": "Exceptions"/"Success",
            "exceptionStatus": "Descriptive message explaining all issues found for this specific line item or N/A",
        }}
    ]
}}
"""

INVOICE_CATEGORY_CLASSIFICATION_PROMPT = """
TASK:
You are a expert in invoice classification. Your task is to classify the invoice into one of the following categories:
1. GTO - Gross Turnover: This category is for invoices related to sales commissions, revenue sharing, or percentage-based fees calculated on gross sales amounts. Examples include:
    a. Sales commission charges
    b. Franchise fees based on revenue
    c. Royalty payments calculated as a percentage of sales
    d. Marketing or distribution fees based on turnover
2. RENTAL - This category covers all invoices related to space or equipment rental, including:
    a. Office space or retail space rental
    b. Warehouse or storage facility charges
    c. Equipment rentals
    d. Vehicle leasing
    e. Property management fees
3. ELECT - This refers to electricity charges and related services:
    a. Regular electricity consumption bills
4. WATER - This category includes all water utility related invoices:
    a. Water consumption charges
5. TELEPHONE - This covers telecommunications and related services: 
    a. Fixed line telephone services
    b. Mobile phone charges
    c. Internet services
    d. Voice over IP services
6. SEWERAGE - This includes waste water and sewage management services:
    a. Sewage disposal charges
    b. Waste water treatment
7. LATE PAYMENT INTEREST - This category is for invoices related to late payment fees or interest charges:
    a. Late payment penalties
    b. Interest charges on overdue invoices
    c. Interest advices

8. UNKNOWN - This category is for invoices that do not fit into any of the above categories or are ambiguous in nature

INVOICE:
{invoiceData}

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "supplierName": "AXY Sdn. Bhd.",
    ...,
    "invoiceCategory": classification type,
    "lineItem":[{{
        "item_list_id": "item_0",
        "description": "item001",
        "itemCode": "code001",
        "unitPrice": "10.00",
        ...
    }},
    {{...}}]
}}
"""

FINAL_VENDOR_MAPPING_PROMPT = """
TASK: You are to map the input JSON data to the best matching database record.

1. Find Best Match
Search through the database records to find the best match using this priority order:
Priority 1: Match by contractNo or accountNo
Priority 2: Match by combination of supplierName, branchName, and branchLocation

2. Validate Match
Reject matches if:
a. MatchReason shows fuzzy/similarity matching between completely different business entities (e.g., "SimDarby SK Sdn Bhd" vs "SD COM")
b. Supplier names are from entirely different industries or business types
c. Match is based solely on weak string similarity without business logic context
d. Having stupid match reasons like "starting with the same letter" 

Accept matches only if:
a. Exact contractNo or accountNo match
b. Supplier names represent the same business entity (accounting for abbreviations, legal suffixes, etc.)
c. Branch/location details align logically with the supplier relationship
d. Fuzzy matching shows clear business relationship (e.g., "ABC Sdn Bhd" vs "ABC SDN BHD" or "ABC Company")
e. matchStrength is "Strong" or "Very Strong" with a valid and verifiable matchReason

3. Determine ReplaceLineItems Setting
If a match is found, compare the supplierName against the list of vendors else SKIP this step.
a. Compare the matched record's supplierName with these vendors:
- TENAGA NASIONAL BERHAD
- Air Selangor
- Indah Water Konsortium
- Syarikat Air
- Aliran Tunas Sdn Bhd
- TT DOT COM
- TELEKOM MALAYSIA BERHAD (UNIFI)

b. If the supplierName is the exact same as the vendors stated above:
- Set replaceLineItems = true
- Set replaceLinteItemsReason = why it was set to true
c. check if replaceLineItemsReason is a valid reason and ensure that it meets the requirement of 90% similarity else
- Set replaceLineItems = false

Example: 
matched supplierName is TELEKOM MALAYSIA BERHAD (UNIFI) 
we set replaceLineItems to false because there is no EXACT match in the vendor list


When a valid match is found, set the following from the database record:
- Set supplierCode from database
- Set supplierName from database
- Set analyticAccountCode from database
- Set completeMapping = true
- Set mappingException = "N/A"
- Set status = "Success"
- Set mappingPoint = the matched database record
- Set replaceLineItems = true/false based on step 3

if no match is found:
- do not change supplierName
- Set supplierCode = "-"
- Set analyticAccountCode = "-"
- Set completeMapping = false
- Set mappingException = short descriptive error message about not being able to find a match
- Set status = "Exceptions"
- Set mappingPoint = "N/A"
- Set replaceLineItems = false

<database>
{database}
</database>

<input>
{invoice}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
    "invoiceNumber": "1234",
    "invoiceDate": "2025-03-01",
    "supplierName": "AXY Sdn. Bhd.",
    "supplierCode": "outletCode001",
    "analyticAccountCode": "erpBranchId001",
    ...
    "status": "Exceptions"/"Success",
    "mappingException": "Descriptive message explaining on why no match was found"/"N/A",
    "mappingPoint": {{
        ...,
        'branchCode': 'outletCode001', 
        'erpBranchId': 'erpBranchId001', 
        'supplierCode': 'outletCode001', 
        'supplierName': 'AXY Sdn. Bhd.',
        ...
    }},
    "completeMapping": true/false,
    "replaceLineItems": true/false,
}}

"""

FINAL_ITEM_MAPPING_PROMPT = """
TASK: for each item in input JSON data, try and match to any records in the  database.

INSTRUCTIONS:
1. go through the list of records and find the best matching record
2. set the itemCode and accountName to the best matching record

when a match is found:
- Set itemCode to accountCode from database
- Set accountName from database
- Set completeMapping = true
- Set mappingException = "N/A"
- Set status = "Success"

if no match is found:
- Set itemCode = "-"
- Set accountName = "-"
- Set completeMapping = false
- Set mappingException = short descriptive error message about not being able to find a match
- Set status = "Exceptions"

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
    "accountName": "accountName001",
    "itemCode": "code001",
    "unitPrice": "10.00",
    "uom": "KG",
    "quantity": "2",
    "totalPrice": "20.00",
    "status": "Success" or "Exceptions",
    "mappingException": "N/A" or error message,
    "completeMapping": True/False
}},
...
]

"""
