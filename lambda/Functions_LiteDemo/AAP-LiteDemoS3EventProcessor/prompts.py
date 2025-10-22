PROMPTS = {
    'invoice': """

<markdown_content>
{markdown_content}
</markdown_content>

<Task Summary>:
You are extracting data from invoice documents to help with automated processing. You must:
A) Extract specific invoice fields and ALL line item data from <markdown_content>
B) Provide confidence scores (0-100) for each extracted field
</Task Summary>

<extraction_instructions>:
**Invoice Level Fields:**
1. AccountNumber: The customer's account number associated with the invoice
2. BillingPeriod: The billing period covered by this invoice (e.g., "01-01-2023 to 31-01-2023")
3. ContractNo: The contract number related to this invoice
4. Currency: Currency code (MYR, USD, etc.)
5. DueDate: The date by which payment is due
6. InvoiceNumber: Invoice number/ID
7. InvoiceDate: the date the invoice was issued
8. TaxAmount: Total tax amount
9. TaxRate: Tax rate percentage  
10. TaxType: Type of tax (Service Tax[SST]/GST/VAT), KWTBB and ICPT are not tax types
11. TotalCharge: The final total of entire invoice for the current billing period only without any outstanding amount, this amount is the sum of current month's line item amounts, tax amount, rounding amount, and deduction of discount amount ONLY.
12. Vendor: Full name of the company/individual that issued the invoice or is payable to
13. VendorAddress: Full address of company/individual that issued the invoice

**Line Item Fields:**
For each line item found in the invoice table/itemized section, extract these fields with numbered prefixes:

**Line Item 1:** lineItem1_Description, lineItem1_UOM, lineItem1_Quantity, lineItem1_UnitPrice, lineItem1_TotalAmount

**Line Item n:** lineItemn_Description, lineItemn_UOM, lineItemn_Quantity, lineItemn_UnitPrice, lineItemn_TotalAmount

**Line Item Field Descriptions:**
1. Description: Item description or service name  
2. UOM: Unit of measure (pieces, kg, hours, etc.)
3. Quantity: Quantity ordered/delivered
4. UnitPrice: Price per unit
5. AmountWithoutTax: Line item total amount.

</extraction_instructions>

<extraction_rules>:
Return "" (empty string) if a field cannot be found
Extract values exactly as they appear in the document
For amounts, include decimal values (e.g., "100.50" not "100")
Extract ALL line items you can find in the document (not just the first one)
If a line item field is not available, use empty string for that specific field
Number line items sequentially starting from 1
Confidence: 100 = completely certain, 50 = some uncertainty, 0 = not found
</extraction_rules>

Include ALL invoice-level fields and ALL line item fields for ALL line items found, even if some have empty values.
<output_format>
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
</output_format>
""",
    'po': """hi2"""
}