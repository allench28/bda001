# Mapping from our extraction field names to BDA-compatible field names
INVOICE_FIELD_MAPPING = {
    'InvoiceNumber': 'InvoiceNumber',
    'InvoiceDate': 'InvoiceDate', 
    'Recipient': 'Recipient',
    'RecipientAddress': 'RecipientAddress',
    'PremiseAddress': 'PremiseAddress',
    'CurrentCharge': 'CurrentCharge',
    'TotalBill': 'TotalBill',
    'TaxAmount': 'TaxAmount',
    'TaxRate': 'TaxRate',
    'TaxType': 'TaxType',
    'OutstandingAmount': 'OutstandingAmount',
    'Currency': 'Currency',
    'PaymentTerms': 'PaymentTerms',
    'Vendor': 'Vendor',
    'VendorAddress': 'VendorAddress',
    'StoreCode': 'StoreCode',
    
    # Line item fields
    'itemCode': 'itemCode',
    'Description': 'Description',
    'UOM': 'UOM',
    'Quantity': 'Quantity',
    'UnitPrice': 'UnitPrice',
    'AmountWithoutTax': 'AmountWithoutTax',
    'TaxRate': 'TaxRate',
    'TaxAmount': 'TaxAmount',
    'TotalAmountWithTax': 'TotalAmountWithTax',
    'LineDiscountAmount': 'LineDiscountAmount'
}

# BDA-compatible field names that should be included in inference_result
BDA_COMPATIBLE_FIELDS = {
    'InvoiceNumber', 'InvoiceDate', 'Recipient', 'RecipientAddress', 'PremiseAddress',
    'CurrentCharge', 'TotalBill', 'TaxAmount', 'TaxRate', 'TaxType', 'OutstandingAmount',
    'Currency', 'PaymentTerms', 'Vendor', 'VendorAddress', 'StoreCode'
}

# Expected entity names for validation
EXPECTED_INVOICE_ENTITIES = [
    'InvoiceNumber', 'InvoiceDate', 'Recipient', 'RecipientAddress', 'PremiseAddress',
    'CurrentCharge', 'TotalBill', 'TaxAmount', 'TaxRate', 'TaxType', 'OutstandingAmount', 
    'Currency', 'PaymentTerms', 'Vendor', 'VendorAddress',
    'itemCode', 'Description', 'UOM', 'Quantity', 'UnitPrice', 'AmountWithoutTax',
    'TaxRate', 'TaxAmount', 'TotalAmountWithTax', 'LineDiscountAmount', 'StoreCode'
]

# Line item field names for processing
LINE_ITEM_FIELDS = {
    'itemCode', 'Description', 'UOM', 'Quantity', 'UnitPrice', 'AmountWithoutTax',
    'TaxRate', 'TaxAmount', 'TotalAmountWithTax', 'LineDiscountAmount'
}

# Invoice level field names
INVOICE_LEVEL_FIELDS = {
    'InvoiceNumber', 'InvoiceDate', 'Recipient', 'RecipientAddress', 'PremiseAddress',
    'CurrentCharge', 'TotalBill', 'TaxAmount', 'TaxRate', 'TaxType', 'OutstandingAmount',
    'Currency', 'PaymentTerms', 'Vendor', 'VendorAddress', 'StoreCode'
}

# Fields that should be treated as numeric
NUMERIC_FIELDS = {
    'CurrentCharge', 'TotalBill', 'TaxAmount', 'TaxRate', 'OutstandingAmount',
    'Quantity', 'UnitPrice', 'AmountWithoutTax', 'TaxAmount', 'TotalAmountWithTax', 
    'LineDiscountAmount', 'StoreCode'
}

# Fields that should be treated as dates
DATE_FIELDS = {
    'InvoiceDate'
}

# FamilyMart store detection patterns
FAMILYMART_PATTERNS = [
    'FamilyMart',
    'Family Mart', 
    'FM-',
    'Fm-',
    'fm-',
    'FM ',
    'Fm ',
    'fm '
]

# Confidence thresholds
MIN_CONFIDENCE_THRESHOLD = 30
HIGH_CONFIDENCE_THRESHOLD = 80

# Document processing settings
DEFAULT_PDF_DPI = 300
MAX_PAGES_PER_DOCUMENT = 10
EXTRACTION_TIMEOUT_SECONDS = 300

# Logging constants for easier management
LOG_PREFIX_CONVERSEFM = "ConverseFM:"
LOG_PREFIX_PBEOFM = "PBEOFM:"
LOG_PREFIX_BEDROCK = "Bedrock:"

# Processing status constants
PROCESSING_STATUS_SUCCESS = "success"
PROCESSING_STATUS_FAILED = "failed"
PROCESSING_STATUS_PROCESSING = "processing"

# Data source identifiers
DATA_SOURCE_CONVERSE = "converse"
DATA_SOURCE_BDA = "bda"

# Simplified BDA output structure template for ConverseFM
SIMPLIFIED_BDA_TEMPLATE = {
    "matched_blueprint": {
        "arn": "",
        "name": "",
        "confidence": 0
    },
    "document_class": {
        "type": "Invoice"
    },
    "split_document": {
        "page_indices": []
    },
    "inference_result": {},
    "explainability_info": []
}

INVOICE_PROMPTS = {
    'INVOICE': {
        'extraction': """
You are an AI system designed to extract specific data from invoice documents, with a focus on invoices related to FamilyMart stores. Your task is to analyze the following invoice and extract key information accurately.

Document Information:
<actual_page_count>{actualPageCount}</actual_page_count>
<file_name>{fileName}</file_name>

Important: The document has {actualPageCount} pages. Page numbering starts at 1 for the first image provided, regardless of any printed page numbers on the document.

Your task is to:
1. Extract specific invoice fields and ALL line item data
2. Provide confidence scores (0-100) for each extracted field
3. Identify which page each field was found on

Please follow these steps carefully:

1. Vendor Extraction and Verification:
   - First, extract the Vendor name from the invoice.
   - Check if the extracted vendor name exactly matches or contains "SANDEN INTERCOOL (MALAYSIA) SDN BHD".
   - This step is crucial for determining whether to extract the Store Code.

2. Store Code Extraction (ONLY if vendor matches):
   - If the vendor matches, look for a store code near the premise address.
   - Valid store codes must be 4 digits or less, not exceed 0550, and be in the same line as the premise address.
   - If no valid store code is found or the vendor doesn't match, set StoreCode to an empty string.
   - Store codes can also be found within clear indications of either "Store", "Outlet", or "Site" (case insensitive).

3. Extract all other fields:
   - Invoice Level Fields: InvoiceNumber, InvoiceDate, Recipient, RecipientAddress, PremiseAddress, CurrentCharge, TotalBill, TaxAmount, TaxRate, TaxType, OutstandingAmount, Currency, PaymentTerms, Vendor, VendorAddress, StoreCode
   - Line Item Fields (extract ALL line items, up to 20): itemCode, Description, UOM, Quantity, UnitPrice, AmountWithoutTax, TaxRate, TaxAmount, TotalAmountWithTax, LineDiscountAmount

Special Instructions for PremiseAddress:
- Look for FamilyMart store patterns throughout the document
- Patterns: "FamilyMart", "Family Mart", "FM", "Fm", "fm" followed by store location
- Examples: "FM-Mid Valley", "FamilyMart - Mid Valley", "fm-kulim", "FamilyMart PJ Sek. 14"
- Check BOTH header sections AND within line item descriptions
- If multiple store references found, choose the most complete one
- If no FamilyMart pattern found, extract any store/location reference

Extraction rule for Quantity
- If quantity column has "LS" as quantity, extract it as it is.
- If quantity for the item does not have any quantity, return "0" as quantity.

Extraction Rules:
- Return an empty string if a field cannot be found.
- Extract values exactly as they appear in the document.
- Use DD-MM-YYYY format for dates when possible.
- Include decimal values for amounts (e.g., "100.50" not "100").
- Extract ALL line items found in the document.
- Include service items (e.g., "Labour Charges", "Transport Charges") as separate line items.
- Number line items sequentially starting from 1.
- Confidence scores: 100 = completely certain, 50 = some uncertainty, 0 = not found.

Output Format:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
{{
  
  "entityName": "field_name",
  "entityValue": "extracted_value",
  "confidence": confidence_score_0_to_100,
  "pageNumber": page_where_found
}}

Include ALL invoice-level fields and ALL line item fields for ALL line items found, even if some have empty values.

Example (generic) output structure:
[
  {{
    "entityName": "Vendor",
    "entityValue": "extracted vendor name",
    "confidence": 95,
    "pageNumber": 1
  }},
  {{
    "entityName": "StoreCode",
    "entityValue": "0123",
    "confidence": 90,
    "pageNumber": 1
  }},
  {{
    "entityName": "InvoiceNumber",
    "entityValue": "INV-12345",
    "confidence": 100,
    "pageNumber": 1
  }},
  {{
    "entityName": "lineItem1_Description",
    "entityValue": "Product A",
    "confidence": 95,
    "pageNumber": 2
  }}
]

Include ALL invoice-level fields and ALL line item fields for ALL line items found, even if some have empty values.
Finally, output only the valid JSON array.
""",
        'bounding_box': """
## Task Summary:
Locate specific text entities in the invoice document and provide precise bounding box coordinates.

## Context Information:
- Document type: Invoice
- Total pages to analyze: {actualPageCount}
- Coordinate system: Use normalized 0-1000 scale where (0,0) is top-left, (1000,1000) is bottom-right

## PAGE INSTRUCTIONS:
- Page 1 = First image provided
- Page 2 = Second image provided  
- Continue sequentially regardless of document page numbers
- Each page has independent coordinates from 0-1000

## VALIDATION BEFORE FINAL RESPONSE:
- Before providing for your final response, make sure you follow the rules stated in <store_code_extraction_rules>.

## BOUNDING BOX FORMAT:
For each entity, provide [x1, y1, x2, y2] coordinates where:
- x1, y1 = top-left corner of the text
- x2, y2 = bottom-right corner of the text
- Use precise pixel-level coordinates
- If text not found, use [0, 0, 0, 0]

## TARGET ENTITIES:
{extractedEntities}

## OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object.
[
  {{
    "entityName": "InvoiceNumber",
    "pageNumber": page_number_where_found,
    "boundingBox": [x1, y1, x2, y2]
  }}
]
"""
    }
}