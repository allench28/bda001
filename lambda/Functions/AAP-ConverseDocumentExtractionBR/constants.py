# Mapping from our extraction field names to BDA-compatible field names
INVOICE_FIELD_MAPPING = {
    'InvoiceNumber': 'InvoiceNumber',
    'InvoiceDate': 'InvoiceDate', 
    'Recipient': 'Recipient',
    'RecipientAddress': 'RecipientAddress',
    'PremiseAddress': 'PremiseAddress',
    'TotalBill': 'TotalBill',
    'TaxAmount': 'TaxAmount',
    'TaxRate': 'TaxRate',
    'TaxType': 'TaxType',
    'OutstandingAmount': 'OutstandingAmount',
    'Currency': 'Currency',
    'PaymentTerms': 'PaymentTerms',
    'Vendor': 'Vendor',
    'VendorAddress': 'VendorAddress',
    'BillingPeriod': 'BillingPeriod',
    'AccountNumber': 'AccountNumber',
    'ContractNo': 'ContractNo',
    'DueDate': 'DueDate',
    'LotNo': 'LotNo',
    'MallName': 'MallName',
    'RoundingAmount': 'RoundingAmount',
    'TotalCharge': 'TotalCharge',
    'TotalDiscountAmount': 'TotalDiscountAmount',
    'InvoiceCategory': 'InvoiceCategory',

    # Line item fields
    'Description': 'Description',
    'UOM': 'UOM',
    'Quantity': 'Quantity',
    'UnitPrice': 'UnitPrice',
    'AmountWithoutTax': 'AmountWithoutTax'
}

# BDA-compatible field names that should be included in inference_result
BDA_COMPATIBLE_FIELDS = {
    'InvoiceNumber', 'InvoiceDate', 'Recipient', 'RecipientAddress', 'PremiseAddress',
    'TotalBill', 'TaxAmount', 'TaxRate', 'TaxType', 'OutstandingAmount',
    'Currency', 'PaymentTerms', 'Vendor', 'VendorAddress', 'BillingPeriod',
    'AccountNumber', 'ContractNo', 'DueDate', 'LotNo', 'MallName',
    'RoundingAmount', 'TotalCharge', 'TotalDiscountAmount', 'InvoiceCategory'
}

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
<Task Summary>:
You are extracting data from invoice documents to help with automated processing. You must:
A) Extract specific invoice fields and ALL line item data
B) Provide confidence scores (0-100) for each extracted field
C) Identify which page each field was found on
</Task Summary>

<Context Information>:
You are analyzing {actualPageCount} pages of an invoice document.
Document type: Invoice  
Document language: English/Malay
File name: {fileName}
</Context Information>

<page numbering>:
Page 1 = First image provided
Page 2 = Second image provided  
Page 3 = Third image provided
Use sequential numbering (1, 2, 3...) regardless of any page numbers printed on the document
</page numbering>

<Extraction Instructions>:
**Invoice Level Fields:**
1. AccountNumber: The customer's account number associated with the invoice
2. BillingPeriod: The billing period covered by this invoice (e.g., "01-01-2023 to 31-01-2023")
3. ContractNo: The contract number related to this invoice
4. Currency: Currency code (MYR, USD, etc.)
5. DueDate: The date by which payment is due
6. InvoiceNumber: Invoice number/ID
7. InvoiceDate: the date the invoice was issued
8. LotNo: The lot number associated with the invoice
9. MallName: the name of the mall of which the store is part of (e.g., "Sunway Pyramid")
10. OutstandingAmount: Any outstanding amount from previous bills
11. PaymentTerms: Payment terms specified
12. PremiseAddress: Full address of invoiced location. This would include mall name/address that invoiced store/company is located, along with unit number/lot number
13. Recipient: The full name of the customer/company billed to
14. RecipientAddress: Full address of customer/company billed to
15. RoundingAmount: The rounding amount applied to the subtotal amount of the invoice. If there is no rounding indicated, do not return any value in this field
16. TaxAmount: Total tax amount
17. TaxRate: Tax rate percentage  
18. TaxType: Type of tax (Service Tax[SST]/GST/VAT), KWTBB and ICPT are not tax types
19. TotalCharge: The final total of entire invoice for the current billing period only without any outstanding amount, this amount is the sum of current month's line item amounts, tax amount, rounding amount, and deduction of discount amount ONLY.
20. TotalDiscountAmount: The total of invoice discount amount, if indicated in the invoice. If there is no discount indicated, do not return any value in this field
21. Vendor: Full name of the company/individual that issued the invoice or is payable to
22. VendorAddress: Full address of company/individual that issued the invoice
23. InvoiceCategory: The category of the invoice [GTO/RENTAL/ELECT/WATER/SEWERAGE/TELEPHONE/LATE PAYMENT INTEREST/UNKNOWN]
  23a. GTO - Gross Turnover: This category is for invoices related to sales commissions, revenue sharing, or percentage-based fees calculated on gross sales amounts, distribution fees based on turnover
  23b. RENTAL - This category covers all invoices related to space or equipment rental, including: Equipment rental, storage rental, and Property management fees.
  23c. ELECT - This refers to electricity charges and related services, you can refer to the unit of measure (UOM) kwh to determine if it is electricity
  23d. WATER - This refers to water supply and related services, you can refer to the unit of measure (UOM) m^3 to determine if it is water
  23e. SEWERAGE - This includes waste water and sewage management services and charges
  23f. TELEPHONE - This covers telecommunications and related services, including: Internet services, Mobile phone services and subscription fees
  23g. LATE PAYMENT INTEREST - This refers to interest charges applied for late payments
  23h. UNKNOWN - This category is used when the invoice does not clearly fit into

**Line Item Fields:**
For each line item found in the invoice table/itemized section, extract these fields with numbered prefixes:

**Line Item 1:** lineItem1_Description, lineItem1_UOM, lineItem1_Quantity, lineItem1_UnitPrice, lineItem1_AmountWithoutTax

**Line Item n:** lineItemn_Description, lineItemn_UOM, lineItemn_Quantity, lineItemn_UnitPrice, lineItemn_AmountWithoutTax 

**Line Item Field Descriptions:**
1. Description: Item description or service name  
2. UOM: Unit of measure (pieces, kg, hours, etc.)
3. Quantity: Quantity ordered/delivered
4. UnitPrice: Price per unit
5. AmountWithoutTax: Line item amount excluding tax, assume line item amount is excluding tax if not explicitly mentioned

**line item notes:**

1. Each line item for electricity amount must add ICPT and KWTBB amounts as electricity charges if exists because ICPT and KWTBB are not standalone entities.
2. Electricity charges should be extracted as one line item with the sum of total amounts of all electricity charges after ICPT and KWTBB are added.
2. Extract only line item amount from amount column.
3. A line item row contains text in line item description and number in line item amounts on the same text level.
4. If no separate amount is listed for text from a line item description, it is a continuation of the previous line item description so combine this text with the previous text from line item description to extract a single, complete description string with a single amount.
5. The number of line item descriptions extracted should be equal to the number of line item amounts listed, otherwise it is an incorrect splitting of a line item into two.

</Extraction Instructions>

<Extraction Rules>:
Return "" (empty string) if a field cannot be found
Extract values exactly as they appear in the document
For amounts, include decimal values (e.g., "100.50" not "100")
Extract ALL line items you can find in the document (not just the first one)
If a line item field is not available, use empty string for that specific field
Number line items sequentially starting from 1
Confidence: 100 
= completely certain, 50 = some uncertainty, 0 = not found
</Extraction Rules>

Include ALL invoice-level fields and ALL line item fields for ALL line items found, even if some have empty values.
<Output Format>
ONLY output the JSON array with the following structure, you MUST NOT include any other texts:
[
  {{
    "entityName": "InvoiceNumber",
    "entityValue": "extracted value or empty string",
    "confidence": confidence_score_0_to_100,
    "pageNumber": page_where_found
  }},
// other invoice fields
  {{
    "entityName": "lineItem1_Description", 
    "entityValue": "extracted line item 1 description",
    "confidence": confidence_score_0_to_100,
    "pageNumber": page_where_found
  }},
  {{
    "entityName": "lineItem1_TotalAmountWithTax", 
    "entityValue": "extracted line item 1 total amount",
    "confidence": confidence_score_0_to_100,
    "pageNumber": page_where_found
  }},
// other line items fields
]
</Output Format>
""",
        'bounding_box': """
## Task Summary:
Locate specific text entities in the invoice document and provide precise bounding box coordinates.

## Context Information:
- Document type: Invoice
- Total pages to analyze: {actualPageCount}

## PAGE INSTRUCTIONS:
- Page 1 = First image provided
- Page 2 = Second image provided  
- Continue sequentially regardless of document page numbers

## BOUNDING BOX FORMAT:
For each entity, provide [x1, y1, x2, y2] coordinates where:
- x1, y1 = top-left corner of the text
- x2, y2 = bottom-right corner of the text
- Use precise pixel-level coordinates
- If text not found, use [0, 0, 0, 0]

## TARGET ENTITIES:
{extractedEntities}

## OUTPUT FORMAT:
Return ONLY a valid JSON array:
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