COLUMN_HEADER_MAPPING = {
    'salesPurchaseOrderDate': 'Date of SPA',
    'salesPurchaseOrderPrice': 'SPA Price',
    'propertyType': 'Property Type',
    'propertyAddress': "Property address",
    'rhbBankName': 'Check RHB Address',
    'bankReferenceNumber': 'Bank Ref No (AA)',
    'projectName': 'Project Name',
    'unitNo': 'Parcel /Unit No',
    'customerName': 'Borrower/Customer Name',
    'customerIdentificationNumber': 'Borrower/Customer NRIC/ passport',
    'registeredOwnerName': 'Purchaser/Registered Owner Name',
    'registeredOwnerIdentificationNumber': 'Purchaser/Registered Owner NRIC/ passport',
    'developerName': 'Developer Name',
    'developerIdentificationNumber': 'Developer ID (BRN)',
    'proprietorName': 'Proprietor Name (Land owner name)',
    'proprietorIdentificationNumber': 'Proprietor ID (NRIC/BRN)',
    'vendorName': 'Vendor Name',
    'vendorIdentificationNumber': 'Vendor NRIC',
    'titleDetailAndNumber': 'Title Detail & Title No (H.S(D/M) No',
    'lotDetailsAndNumber': 'Lot No Details & Lot No PT (D/M) No',
    'typeOfLandLocation': 'Bandar/Pekan/Mukim',
    'landLocation': 'Tempat',
    'landDistrict': 'District',
    'landArea': 'Land area',
    'landUse': 'Land use',
    'builtUpArea': 'Built up area',
    'actualCondition': 'Actual Condition (Syarat Nyata)',
    'restrictionInInterest': 'Restriction In Interest',
    'chargeeBankName': 'Chargee Bank Name',
    'hdaBeneficiaryBank': 'HDA Beneficiary Bank',
    'hdaAccountNumber': 'HDA Account',
    'facilityAmount': 'Facility Amount',
    'billingStage': 'Billing_Stage',
    'billingPercentage': 'Billing_percentage',
    'billingAmount': 'Billing_Amount',
    'architectCertStage': 'ArchitectCert_Stage',
    'architectCertPercentage': 'ArchitectCert_percentage',
    'architectCertCompletionStatus': 'ArchitectCert status completion',
    'beneficiaryName': 'Beneficiary Name',
    'beneficiaryBank': 'Beneficiary Bank',
    'beneficiaryAccountNumber': 'Beneficiary account no',
    'chargeeBankEndorsement': 'Tanggungan & Endosan'
}

FILENAME_LISTS = {
    'SPA': {
        'documentType': "SPA",
        'filenames': [
            'spa',
            "SPA",
            'spa document',
            'spa_document',
            'sale and purchase agreement',
            'sales purchase agreement',
            'spa agreement',
            'spa_agreement'
        ]
    },
    'BILLING': {
        'documentType': "BILLING",
        'filenames': [
            'billing',
            'billing statement',
            'billing_statement',
            'bill',
            'invoice',
            'billing report',
            'billing_report'
        ]
    },
    'LAND_SEARCH': {
        'documentType': "LAND SEARCH",
        'filenames': [
            'land search',
            'land_search',
            'land search report',
            'land_search_report',
            'land search result',
            'land_search_result',
            'ls',
            'ls report',
            'ls_report'
        ]
    },
    'LU_DEVELOPER': {
        'documentType': "LU DEV",
        'filenames': [
            'lu developer',
            'lu_developer',
            'lu dev',
            'lu_dev',
            'letter undertaking',
            'letter_undertaking',
            'letter undertaking developer',
            'letter_undertaking_developer',
            'lu developer form',
            'lu_developer_form',
            'dev_lu',
            'dev lu'
        ]
    }
}

DOCUMENT_ENTITIES = {
    'SPA': [
        'salesPurchaseOrderDate',
        'salesPurchaseOrderPrice',
        'propertyType',
        'propertyAddress',
        'projectName',
        'unitNo',
        'registeredOwnerName',
        'registeredOwnerIdentificationNumber',
        'developerName',
        'developerIdentificationNumber',
        'proprietorName',
        'proprietorIdentificationNumber',
        'titleDetailAndNumber',
        'lotDetailsAndNumber',
        'typeOfLandLocation',
        'landLocation',
        'landDistrict',
        'landArea',
        'landUse',
        'builtUpArea',
        'chargeeBankName',
        'hdaBeneficiaryBank',
        'hdaAccountNumber'
    ],
    'BILLING': [
        'salesPurchaseOrderPrice',
        'propertyType',
        'rhbBankName',
        'bankReferenceNumber',
        'projectName',
        'unitNo',
        'registeredOwnerName',
        'developerName',
        'developerIdentificationNumber',
        'titleDetailAndNumber',
        'lotDetailsAndNumber',
        'typeOfLandLocation',
        'landLocation',
        'landDistrict',
        'hdaBeneficiaryBank',
        'hdaAccountNumber',
        'facilityAmount',
        'billingStage',
        'billingPercentage',
        'billingAmount',
        'architectCertStage',
        'architectCertPercentage',
        'architectCertCompletionStatus'
    ],
    'LAND SEARCH': [
        'proprietorName',
        'proprietorIdentificationNumber',
        'registeredOwnerName',
        'registeredOwnerIdentificationNumber',
        'titleDetailAndNumber',
        'lotDetailsAndNumber',
        'typeOfLandLocation',
        'landLocation',
        'landDistrict',
        'landArea',
        'landUse',
        'actualCondition',
        'restrictionInInterest',
        'chargeeBankName'
    ],
    'LU DEV': [
        'salesPurchaseOrderPrice',
        'propertyType',
        'rhbBankName',
        'bankReferenceNumber',
        'projectName',
        'unitNo',
        'customerName',
        'customerIdentificationNumber',
        'registeredOwnerName',
        'registeredOwnerIdentificationNumber',
        'developerName',
        'developerIdentificationNumber',
        'proprietorName',
        'proprietorIdentificationNumber',
        'titleDetailAndNumber',
        'lotDetailsAndNumber',
        'typeOfLandLocation',
        'landLocation',
        'landDistrict',
        'chargeeBankName',
        'hdaBeneficiaryBank',
        'facilityAmount',
        'hdaAccountNumber',
        'beneficiaryName',
        'beneficiaryBank',
        'beneficiaryAccountNumber'
    ]
}

FIELD_ORDER = [
    'salesPurchaseOrderDate',
    'salesPurchaseOrderPrice',
    'propertyType',
    'propertyAddress',
    'rhbBankName',
    'bankReferenceNumber',
    'projectName',
    'unitNo',
    'customerName',
    'customerIdentificationNumber',
    'registeredOwnerName',
    'registeredOwnerIdentificationNumber',
    'developerName',
    'developerIdentificationNumber',
    'proprietorName',
    'proprietorIdentificationNumber',
    'vendorName',
    'vendorIdentificationNumber',
    'titleDetailAndNumber',
    'lotDetailsAndNumber',
    'typeOfLandLocation',
    'landLocation',
    'landDistrict',
    'landArea',
    'landUse',
    'builtUpArea',
    'actualCondition',
    'restrictionInInterest',
    'chargeeBankName',
    'hdaBeneficiaryBank',
    'hdaAccountNumber',
    'facilityAmount',
    'billingStage',
    'billingPercentage',
    'billingAmount',
    'architectCertStage',
    'architectCertPercentage',
    'architectCertCompletionStatus',
    'beneficiaryName',
    'beneficiaryBank',
    'beneficiaryAccountNumber',
    'chargeeBankEndorsement'
]

# Document-specific prompts with everything pre-formatted
DOCUMENT_PROMPTS = {
    'SPA': """
## Task Summary:
You have to do 3 important tasks:  
A) Find and extract a list of entities from the document (list and examples will be given to you).
B) Provide a confidence score number for each of the entities you extract.  
C) Identify which page each entity was found on.

## Context Information:
The following are scanned document pages used as part of a loan application in a bank in Malaysia.
The document type is a Sales & Purchase Agreement (SPA).
The document language is English/Malay.
Sentence extraction is {sentence_extraction}.
You will be analyzing multiple pages of the same document - please review all pages to extract entities.

## IMPORTANT PAGE NUMBERING INSTRUCTIONS:
You are analyzing {actual_page_count} pages total. The document may have internal page labels that DO NOT match the actual page sequence.
- ALWAYS use sequential page numbers starting from 1 for your response
- Page 1 = First image provided
- Page 2 = Second image provided  
- Page 3 = Third image provided
- etc.
- IGNORE any page numbers printed on the document itself
- Only use the sequential order of images provided to you

## Model Instructions:
Instructions about entity extraction (A): 
- Find and extract ALL 23 entities from the following list.
- Look across ALL pages provided to find these entities.
- Don't change anything or make up any information other than what is inside the document.  
- Some entities may not be present across all pages. If you cannot find some of the entities output "UNKNOWN" for that entity.  
- If an entity appears on multiple pages, choose the most complete/clear instance.
- ONLY extract characters that are clearly visible and readable.
- IMPORTANT: Extract text as clean, readable text without Unicode escape sequences (\\u codes)
- If text contains special characters, represent them as the actual characters, not as escape sequences
- Avoid any backslash escape sequences in the extracted text values

List of entities to be extracted, along with their expected type and an example:  
1. salesPurchaseOrderDate: Date of SPA
  Description: The date when the Sales & Purchase Agreement was signed or executed, be sure to extract the full date
  Possible Malay Words: Tarikh SPA
  Example: 06 Aug 2024

2. salesPurchaseOrderPrice: SPA Price
  Description: The total purchase price stated in the Sales & Purchase Agreement, not to be confused with the loan amount
  Example: RM 500,000.00

3. propertyType: Property Type
  Description: The type of property being purchased (e.g., condominium, terrace house, apartment), ensure youre not mistaken with the project name
  Possible Malay Words: Jenis Hartanah
  Example: Condominium

4. propertyAddress: Property address
  Description: The full complete address of the property ("the said land") being purchased, typically found in the "PREAMBLE" section. Extract starting from "held under" onwards and dont include "held under" until before area measurement and dont include area measurement.  
  Example: No. of Title HSD 3247, Lot No/L.O No PT9 Section NIL in the Bandar Damansara, District of Petaling, State of Selangor.

5. projectName: Project Name
  Description: The full name of the development project
  Possible Malay Words: Nama Projek
  Example: Bangsar Heights

6. unitNo: Parcel /Unit No
  Description: The specific unit number or parcel number of the property
  Possible Malay Words: Nombor Syit Piawi, Nombor Unit
  Example: Unit 12-3A

7. registeredOwnerName: Purchaser/Registered Owner Name
  Description: The name of the purchaser(s) who will be the registered owner(s) of the property, extract from "the Purchaser" in the document. You must capture all the purchaser(s) names if there are multiple.
  Possible Malay Words: Nama Pembeli, Nama Pemilik Berdaftar
  Example: Siti binti Hassan & George Lim

8. registeredOwnerIdentificationNumber: Purchaser/Registered Owner NRIC/ passport
  Description: The alphanumerical NRIC or passport number of the purchaser(s). Must be the alphanumerical value, not the name.
  Possible Malay Words: NRIC Pembeli, Nombor Kad Pengenalan
  Example: 987654-32-1098

9. developerName: Developer Name
  Description: The name of the property developer company, extract from "the Developer" label in the document
  Example: ABC Development Sdn Bhd

10. developerIdentificationNumber: Developer ID (BRN)
  Description: The alphanumerical Business Registration Number (BRN) or company number (Co No.) of the developer
  Possible Malay Words: Nombor Pendaftaran Syarikat, BRN Pemaju
  Example: 123456-A

11. proprietorName: Proprietor Name (Land owner name)
  Description: The name of the land owner or proprietor, extract from "the Proprietor" label in the document.
  Example: ABC Holdings Sdn Bhd

12. proprietorIdentificationNumber: Proprietor ID (NRIC/BRN)
  Description: The alphanumerical NRIC or Business Registration Number of the proprietor/land owner.
  Example: 567890-B

13. titleDetailAndNumber: Title Detail & Title No (H.S(D/M) No
  Description: The title type and number (e.g., H.S.(D) or H.S.(M) or GRN) from the land title, you MUST capture the number, and follow the format H.S.(D) 12345 or H.S.(M) 67890 or GRN 1234
  Possible Malay Words: Jenis Hak Milik & Nombor Hak Milik
  Example: H.S.(D) 12345

14. lotDetailsAndNumber: Lot No Details & Lot No PT (D/M) No
  Description: The lot number and details (P.T. number) from the land title, you MUST capture the number, and follow the format PTD 12345
  Possible Malay Words: Nombor Lot, PT
  Example: PT 12345 Section 2

15. typeOfLandLocation: Bandar/Pekan/Mukim
  Description: DO NOT extract struck through values. The administrative area type (Bandar/Pekan/Mukim/Town/Village) where the land is located. for example "Town/Mukim(crossed)/Pekan(crossed) Sungai Lalang" then you must extract "Town Sungai Lalang".,
  Possible Malay Words: Bandar, Pekan, Mukim
  Example: "Bandar Kuala Lumpur" or "Pekan Sungai Besi" or "Town Sungai Lalang" or "Mukim Batu"
  
16. landLocation: Tempat
  Description: Extract only from "Tempat" label. return "UNKNOWN" if not found.
  Possible Malay Words: Tempat, Kawasan
  Example: Bangsar

17. landDistrict: District
  Description: The district where the land is located, extract district from "[district], District of [not district]" in the property address.
  Possible Malay Words: Daerah
  Example: Kuala Lumpur

18. landArea: Land area
  Description: Dont extract the unit of measurement that is struck through. The total area of the land parcel, ensure to take the right unit of measurement (e.g., meter square, acres). 
  Example: 1000 sq ft

19. landUse: Land use
  Description: The approved use of the land (e.g., housing development), look for the section that says "the Developer has the absolute right to use the land for" or "the land is approved for" or "the land is used for".
  Example: housing development

20. builtUpArea: Built up area
  Description: The total built-up area of the property or building, which may be different from the land area
  Example: 800 sq ft

21. chargeeBankName: Chargee Bank Name
  Description: The name of the bank that holds the charge or mortgage over the property.
  Example: RHB Bank Berhad

22. hdaBeneficiaryBank: HDA Beneficiary Bank
  Description: The bank name for the Housing Development Account (HDA), ensure its a bank name and not a person or company
  Example: RHB Bank Berhad

23. hdaAccountNumber: HDA Account
  Description: The Housing Development Account number for fund disbursement
  Example: 98765432101

Instructions about the confidence score (B): 
- For each extracted entity include a confidence score number between 0-100 
- 100 indicates totally certain (i.e., clearly identifiable text and no ambiguity among other candidate entities) 
- 50 indicates some kind of uncertainty (i.e. not clearly identifiable text or multiple candidate values for an entity, making difficult to select the right one). 
- 0 indicates totally uncertainty and implies that you should give "UNKNOWN" for that entity.  
- Any other situations should be treated accordingly within those confidence score.  

Instructions about page identification (C):
- For each entity, specify which page number it was found on using SEQUENTIAL NUMBERING (1, 2, 3, etc.)
- Use the order of images provided, NOT the page numbers printed on the document
- If an entity is "UNKNOWN", set page_number to 0

## OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT include any additional text, explanations, or formatting.
The JSON must be in the following format:
[{{ 
   "entity_name": "entity key name as given in the above entity list (e.g., "salesPurchaseOrderDate")", 
   "entity_value": "entity value as extracted from document",
   "confidence": confidence score as number between 0-100,
   "page_number": page number where entity was found,
   "sentence": the short sentence that you extacted the entity from [only if sentence extraction is enabled]
}}]

IMPORTANT: 
- The "entity_name" field should contain the exact entity key (e.g., "salesPurchaseOrderDate", "propertyAddress") not the display name
- The "entity_value" field should contain the actual extracted text from the document
- Include ALL 23 entities in your response, even if some are "UNKNOWN"
- THE OUTPUT MUST FOLLOW THE OUTPUT JSON STRUCTURE
- Review all pages before providing your final response
- Each entity should appear only once in your response (choose the best instance if found on multiple pages)
- Make sure the format can be directly parsed by json.loads() without any additional text or formatting
- DO NOT use backslash escape sequences like \\u2021, \\u2020, etc. in entity values
- REMEMBER: Use sequential page numbering (1 to {actual_page_count}) based on image order, not document labels
    """,
    
    'BILLING': """
You are a specialized Entity Extraction Expert for Billing documents with expertise in processing multi-page invoices and certificates. You are to analyze the document in <document></document> tags. Only respond based on information within <document></document>

## CRITICAL PAGE RESTRICTIONS
STOP: Before extracting ANY entities, acknowledge these absolute restrictions:

Entity #10 (titleDetailAndNumber): INVOICE pages ONLY
Entity #11 (lotDetailsAndNumber): INVOICE pages ONLY  
Entity #12 (typeOfLandLocation): INVOICE pages ONLY
Entity #13 (landLocation): INVOICE pages ONLY
Entity #14 (landDistrict): INVOICE pages ONLY

RULE: If you find these entities on CERTIFICATE pages, you MUST ignore them completely.
RULE: Finding information on wrong page type = treat as if information doesn't exist.

## ENTITY EXTRACTION INSTRUCTIONS

Step 1 - Page classification:
MANDATORY: Create page type inventory BEFORE any entity extraction:
- Scan ALL pages simultaneously and create this mapping:
- INVOICE pages: [list all page numbers that are invoices]  
- CERTIFICATE pages: [list all page numbers that are certificates]

Example output: "INVOICE pages: 3,5 | CERTIFICATE pages: 1,2,4"

RULE: Complete this mapping before extracting ANY entities
RULE: Page position in sequence is irrelevant - only page type matters

Step 2 - Entity extraction rules:
- Process ALL 23 entities using cross-page simultaneous scanning
- For entities 10-14: scan only INVOICE pages (ignore page order within INVOICE pages)
- For entities 1-9,15-23: scan all pages (ignore page order completely)
- Choose best instance based on completeness/clarity, NOT page position
- Output "UNKNOWN" if entity cannot be found after scanning all allowed pages

Step 3 - Text extraction standards:
- MANDATORY: Use [?] for ANY character that could be confused with another
- When in doubt between two similar characters, always use [?]
- Extract only characters you are 100 percent certain about
- For names and critical identifiers, be extra cautious with character recognition

## QUALITY CHECK INSTRUCTIONS
MANDATORY: After initial extraction, perform these checks:

1. UNKNOWN entity verification:
   - For each "UNKNOWN" entity, re-scan ALL allowed pages
   - Look specifically for that entity's expected labels
   - Only keep "UNKNOWN" if truly not found after re-scanning

2. Label verification:
   - If clearly labeled field exists, it must be extracted
   - Don't skip obvious labeled fields

3. Order independence verification:
   - Confirm extraction would be identical if pages were in reverse order
   - Verify page position didn't influence entity selection
   - Only page type restrictions and content quality should determine extraction

## TARGET ENTITIES

1. salesPurchaseOrderPrice: The total purchase or selling price stated in the Sales & Purchase Agreement, not to be confused with the loan amount
   Example: "RM 500,000.00"

2. propertyType: The type of property being purchased (e.g., condominium, terrace house, apartment), not to be confused with the project name
   Example: "Condominium"

3. rhbBankName: The specific RHB Bank branch name for verification, either 'RHB Bank Berhad', 'RHB Islamic Bank Berhad' or 'UNKNOWN' if not found
   Example: "RHB Bank Berhad"

4. bankReferenceNumber: Extract ONLY from 'Your Ref' or 'Financier Ref' - this is the BANK's reference, NOT 'Our Ref' which is the developer's reference. Look for labels like 'Your Ref:', 'Financier Ref:', or similar bank-issued reference numbers.
   Example: "23KLM00123"

5. projectName: The name of the development project
   Example: "Bangsar Heights"

6. unitNo: The specific unit number or parcel number of the property
   Possible Malay Words: Nombor Syit Piawi, Nombor Unit
   Example: "Unit 12-3A"

7. registeredOwnerName: The name of the purchaser(s) who will be the registered owner(s) of the property
   Example: "Siti binti Hassan"

8. developerName: The name of the property development company submitting the billing
   Example: "ABC Development Sdn Bhd"

9. developerIdentificationNumber: The Business Registration Number (BRN) or company number of the developer
   Example: "123456-A"

10. titleDetailAndNumber: PAGE RESTRICTION: INVOICE PAGES ONLY - IGNORE IF FOUND ON CERTIFICATES. Extract the value found after any of these labels: 'Title No:', 'Hakmilik No:', 'Jenis Hak Milik:', 'H.S.(D)', 'H.S.(M)', 'GRN', 'PN', or similar label identifiers. The value may be in formats like 'H.S.(D) 12345', 'H.S.(M) 67890', 'GRN 123', 'PN 456', or simply a number like '649'. Extract the complete value including any prefixes or suffixes. If multiple title references exist, prioritize the most complete format, otherwise extract any available title number.
    Possible Malay Words: Nombor Hak Milik, Hakmilik No.
    Example: "H.S.(D) 12345"

11. lotDetailsAndNumber: PAGE RESTRICTION: INVOICE PAGES ONLY - IGNORE IF FOUND ON CERTIFICATES. Find values after labels: 'P.T.(D/M) No:', 'Lot No:', 'Lot / PT No:' or similar identifiers. Extract complete values including prefixes/suffixes.
    Possible Malay Words: Nombor Lot, PT
    Example: "PT 12345 Section 2"

12. typeOfLandLocation: PAGE RESTRICTION: INVOICE PAGES ONLY - IGNORE IF FOUND ON CERTIFICATES. Extract from the value found after any of these labels: 'Master Title' or 'Land Title' label in the INVOICE pages. If multiple title references exist, prioritize the most complete format, otherwise extract any available administrative area type (Bandar/Pekan/Mukim/Town/Village) where the land is located in.
    Possible Malay Words: Bandar, Pekan, Mukim
    Example: "Bandar Kuala Lumpur" or "Pekan Sungai Besi" or "Town Sungai Lalang" or "Mukim Batu"

13. landLocation: PAGE RESTRICTION: INVOICE PAGES ONLY - IGNORE IF FOUND ON CERTIFICATES. Extract only from 'Tempat' label. return 'UNKNOWN' if not found.
    Possible Malay Words: Tempat, Kawasan
    Example: "Bangsar"

14. landDistrict:
- **PAGE RESTRICTION** : INVOICE PAGES ONLY â€“ IGNORE IF FOUND ON CERTIFICATES OR COMPANY/ADDRESS SECTIONS (e.g., RHB address, developer address, etc.).
- **Goal**: Extract only from explicit labels such as 'Daerah', 'Master Title', or 'Land Title' that clearly indicate the district where the land is located.
- Valid patterns include:
i) "Daerah [district name]"
ii) "[district name], District of [name]"
- If the word 'Daerah' or other district indicators appear only as part of an address or are not explicitly linked to land title information, return "N/A".
- **Source Pages**: Must be found on **INVOICE pages only**.
- Possible Malay Words: Daerah
- Example: "Kuala Lumpur"
- Example of invalid extraction (should return N/A): "Daerah Kuala Lumpur" found in the company address or RHB address section.

15. hdaBeneficiaryBank: The bank name associated with Housing Development Account (HDA) for fund disbursement. Look for bank names in these contexts: (1) Explicitly near 'HDA Account' labels, (2) Bank names mentioned in the same section/paragraph as HDA account information, (3) Banks listed in fund settlement contexts. May appear as standalone bank names near HDA details or within parentheses in payment instructions.
    Example: "RHB Bank Berhad"

16. hdaAccountNumber: The Housing Development Account number for the receiving bank. Look for account numbers that appear in conjunction with fund disbursement or receiving contexts. May be explicitly labeled as 'HDA Account' or appear as numeric/alphanumeric identifiers near the beneficiary bank name.
    Example: "98765432101"

17. facilityAmount: The total loan or facility amount approved by the bank
    Possible Malay Words: Jumlah Kemudahan
    Example: "RM 1,000,000.00"

### Billing Entities (INVOICE PAGES ONLY)

18. billingStage: ALL the construction stages for which the billing(s) is made. if there is multiple bills/invoices, compile all the stages into one list. You must attach the corresponding stage in the format '(stage) [description]'
    Possible Malay Words: Peringkat Bil, Tahap Pembinaan
    Example: "(2a) Foundation Work, (2b) The drains serving the said building"

19. billingPercentage:
- **Goal**: Extract ALL the corresponding percentages for the stages listed in billingStage from the INVOICE pages, including cases where multiple percentages (for the same stage) appear across different pages or property units.
- **Source Pages**: Must be found on **INVOICE pages only**.
- **Aggregation**: If multiple stages/percentages exist across different INVOICE pages, **compile all of them into one single list**. Do not omit additional percentages if they belong to the same stage.
- **Format**: Each extracted percentage must be in the format `(stage) [percentage]`. Combine all extracted values into a single string separated by commas. Ensure the stage number matches the one used in `billingStage`.
- **Labels/Context**: Look for percentages near stage descriptions or amounts. Include all relevant percentages associated with the same billing stage, even if they appear under different property units or invoice breakdowns.
- Example: "(2a) 25%, (2g) 10%"

20. billingAmount: ADD TOGETHER the amount due from the financier from ALL INVOICE pages. Calculate: (Amount Due from Page 1) + (Amount Due from Page 2) + (Amount Due from Page 3) + etc. Use the final net amount due after deductions/additions from each individual invoice page, then sum all these final amounts across all pages.
    Possible Malay Words: Jumlah Bil
    Example: "RM 125,000.00"

### Architect Certification Entities (CERTIFICATE PAGES ONLY)

**21. architectCertStage**:
- **Goal**: Extract ALL construction stages certified as **completed or ongoing** by the architect.
- **Source Pages**: Must be found on **CERTIFICATE pages only**. **IGNORE if found on INVOICE pages.**
- **Aggregation**: Compile all certified stages across all CERTIFICATE pages into one single list.
- **Format**: Each extracted stage must be in the format `(stage) [description]`.
- **Labels/Context**: Look for stages explicitly mentioned in the Architect's Certificate of Completion, near 'Peringkat Sijil Arkitek', or sections detailing work certified.
- Example: "(1) Immediately upon the signing of this agreement, (2a) The Foundation Work"

**22. architectCertPercentage**:
- **Goal**: Extract ALL the corresponding percentages of the construction stages certified by the architect.
- **Source Pages**: Must be found on **CERTIFICATE pages only**. **IGNORE if found on INVOICE pages.**
- **Aggregation**: Compile all certified percentages across all CERTIFICATE pages into one single list.
- **Format**: Each extracted percentage must be in the format `(stage) [percentage]`. Ensure the stage number matches the one used in `architectCertStage`.
- **Labels/Context**: Look for percentages next to the certified stages in the Architect's Certificate.
- Example: "(2g) 2.5%, (3b) 50%"

**23. architectCertCompletionStatus**:
- **Goal**: Extract ALL the specific statuses or completion notes for the stages certified by the architect.
- **Source Pages**: Must be found on **CERTIFICATE pages only**. **IGNORE if found on INVOICE pages.**
- **Aggregation**: Compile all certification statuses across all CERTIFICATE pages into one single list.
- **Format**: Each extracted status must be in the format `(stage) [status]`. Ensure the stage number matches the one used in `architectCertStage`.
- **Labels/Context**: Extract the descriptive status, which may be a simple 'completed', 'yes', or a detailed note like '20-36 plots completed'. Look in the column or section detailing the status of the certified work.
- Example: "(1) yes, (2a) 20-36 plots completed"
## CONFIDENCE SCORING
- For each extracted entity include a confidence score number between 0-100
- 100 indicates totally certain (i.e., clearly identifiable text and no ambiguity among other candidate entities)
- 50 indicates some kind of uncertainty (i.e. not clearly identifiable text or multiple candidate values for an entity, making difficult to select the right one)
- 0 indicates totally uncertainty and implies that you should give "UNKNOWN" for that entity
- Any other situations should be treated accordingly within those confidence score

## PAGE IDENTIFICATION
- For each entity, specify which page number it was found on using SEQUENTIAL NUMBERING (1, 2, 3, etc.)
- Use the order of images provided, NOT the page numbers printed on the document
- If an entity is "UNKNOWN", set page_number to 0

## OUTPUT FORMAT
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT include any additional text, explanations, or formatting.
The JSON must be in the following format:
[{{
   "entity_name": "entity key name as given in the above entity list (e.g., \"salesPurchaseOrderPrice\")",
   "entity_value": "entity value as extracted from document (DO NOT USE EXAMPLE PROVIDED. CAN BE UNKNOWN)",
   "confidence": confidence score as number between 0-100,
   "page_number": page number where entity was found,
   "sentence": "the exact sentence or text fragment that contains the entity",
   "reasoning": "Think step by step here. detailed explanation of extraction logic, context analysis, and decision-making process. Do this FIRST"
}}]

IMPORTANT:
- The "entity_name" field should contain the exact entity key (e.g., "salesPurchaseOrderPrice", "titleDetailAndNumber") not the display name
- The "entity_value" field should contain the actual extracted text from the document
- Include ALL 23 entities in your response, even if some are "UNKNOWN"
- Ensure the JSON is valid and properly formatted
- Review all pages before providing your final response
- Each entity should appear only once in your response (choose the best instance if found on multiple pages)
- Make sure the format can be directly parsed by json.loads() without any additional text or formatting
- DO NOT use backslash escape sequences like \\u2021, \\u2020, etc. in entity values
- REMEMBER: Use sequential page numbering based on image order, not document labels

**Final Instruction**: Think step by step in your reasoning for each value. Analyze the provided markdown content and provide ONLY the JSON array response with comprehensive reasoning for each entity extraction decision. Begin your response directly with the opening bracket [.
    """,
    
    'LAND SEARCH': """
## Task Summary:
You have to do 3 important tasks:  
A) Find and extract a list of entities from the document (list and examples will be given to you).
B) Provide a confidence score number for each of the entities you extract.  
C) Identify which page each entity was found on.

## Context Information:
The following are scanned document pages used as part of a loan application in a bank in Malaysia.
The document type is a LAND SEARCH report from the land registry.
The document language is English/Malay.
Sentence extraction is {sentence_extraction}.
You will be analyzing multiple pages of the same document - please review all pages to extract entities.

## IMPORTANT PAGE NUMBERING INSTRUCTIONS:
You are analyzing {actual_page_count} pages total. The document may have internal page labels that DO NOT match the actual page sequence.
- ALWAYS use sequential page numbers starting from 1 for your response
- Page 1 = First image provided
- Page 2 = Second image provided  
- Page 3 = Third image provided
- etc.
- IGNORE any page numbers printed on the document itself
- Only use the sequential order of images provided to you

## Model Instructions:
Instructions about entity extraction (A): 
- Find and extract ALL 14 entities from the following list.
- Look across ALL pages provided to find these entities.
- Don't change anything or make up any information other than what is inside the document.  
- Some entities may not be present across all pages. If you cannot find some of the entities output "UNKNOWN" for that entity.  
- If an entity appears on multiple pages, choose the most complete/clear instance.
- ONLY extract characters that are clearly visible and readable.
- IMPORTANT: Extract text as clean, readable text without Unicode escape sequences (\\u codes)
- If text contains special characters, represent them as the actual characters, not as escape sequences
- Avoid any backslash escape sequences in the extracted text values

List of entities to be extracted, along with their expected type and an example:  
1. proprietorName: Proprietor Name (Previous Land Owner)
  Description: If there is "pindahmilik tanah oleh [proprietor] kepada [purchaser]" extract proprietor. If there is no "pindahmilik tanah oleh [proprietor] kepada [purchaser]" then extract the entity that is under the "REKOD KETUANPUYAAN" or "PEMILKAN DAN ALAMAT" section. 
  Possible Malay Words: Nama Pemilik Tanah, pindahmilik tanah oleh, Gadaian oleh
  Example: ABC Holdings Sdn Bhd

2. proprietorIdentificationNumber: Proprietor ID (NRIC/BRN)
  Description: The NRIC or Business Registration Number of the proprietor/previous land owner
  Possible Malay Words: NRIC/BRN Pemilik Tanah
  Example: 567890-B

3. registeredOwnerName: Purchaser/Registered Owner Name (Current Owner)
  Description: If there is "pindahmilik tanah oleh [proprietor] kepada [purchaser]" extract purchaser . If there is no "pindahmilik tanah oleh [proprietor] kepada [purchaser]", then return "UNKNOWN".
  Possible Malay Words: Nama Pembeli, Nama Pemilik Berdaftar, Pemilik
  Example: Siti binti Hassan, Kobe Sdn Bhd

4. registeredOwnerIdentificationNumber: Purchaser/Registered Owner NRIC/Passport
  Description: The NRIC or passport number of the purchaser(s)/registered owner(s) (current owner)
  Possible Malay Words: NRIC Pembeli, Nombor Kad Pengenalan
  Example: 987654-32-1098

5. titleDetailAndNumber: Title Detail & Title No (H.S.(D/M) No
  Description: The title type and number (e.g., H.S.(D) or H.S.(M) or GRN) from the land title, ensure the number is captured, should follow the format H.S.(D) 12345 or H.S.(M) 67890, Hakmilik Sementara 123 or GRN 1234
  Possible Malay Words: Jenis Hak Milik, Nombor Hak Milik & Hakmilik Sementara
  Example: H.S.(D) 12345

6. lotDetailsAndNumber: Lot No Details & Lot No PT (D/M) No
  Description: The lot number and details (P.T. number) from the land title
  Possible Malay Words: Nombor Lot, PT
  Example: PT 12345 Section 2

7. typeOfLandLocation: Bandar/Pekan/Mukim
  Description: The administrative area type (Bandar/Pekan/Mukim/Town/Village) where the land is located, take the one that is not struck through or if all are struck through, take the one that is in the title. for example "Town/Mukim(crossed)/Pekan(crossed) Sungai Lalang" then you should take "Town Sungai Lalang".,
  Possible Malay Words: Bandar, Pekan, Mukim
  Example: "Bandar Kuala Lumpur" or "Pekan Sungai Besi" or "Town Sungai Lalang" or "Mukim Batu"

8. landLocation: Tempat
  Description: Extract only from "Tempat" label. return "UNKNOWN" if not found.
  Possible Malay Words: Tempat, Kawasan
  Example: Bangsar

9. landDistrict: District
  Description: Extract from "Daerah" label, return "UNKNOWN" there is no value
  Possible Malay Words: Daerah
  Example: Kuala Lumpur

10. landArea: Land area
  Description: The total area of the land parcel with unit of measurement
  Possible Malay Words: Keluasan Tanah
  Example: 1000 sq ft

11. landUse: Land use
  Description: The approved use of the land (e.g., residential, commercial, industrial, bangunan)
  Possible Malay Words: Kegunaan Tanah
  Example: Residential

12. actualCondition: Actual Condition (Syarat Nyata)
  Description: The complete actual condition or restriction (Syarat Nyata) attached to the land. Capture all conditions listed under "Syarat Nyata" or "Actual Condition" section. 
  Possible Malay Words: Syarat Nyata
  Example: Tanah ini henadklah digunakan untul rumah kediaman sahaja, tidak boleh digunakan untuk tujuan komersial.

13. restrictionInInterest: Restriction In Interest
  Description: Any restrictions in interest or limitations on the land use
  Possible Malay Words: Sekatan Kepentingan, Sekatan
  Example: None

14. chargeeBankName: Chargee Bank Name
  Description: The name of the bank that holds the charge or mortgage over the property
  Possible Malay Words: Nama Bank Gadaian, Gadaian menjamin wang pokok
  Example: RHB Bank Berhad

Instructions about the confidence score (B): 
- For each extracted entity include a confidence score number between 0-100 
- 100 indicates totally certain (i.e., clearly identifiable text and no ambiguity among other candidate entities) 
- 50 indicates some kind of uncertainty (i.e. not clearly identifiable text or multiple candidate values for an entity, making difficult to select the right one). 
- 0 indicates totally uncertainty and implies that you should give "UNKNOWN" for that entity.  
- Any other situations should be treated accordingly within those confidence score.  

Instructions about page identification (C):
- For each entity, specify which page number it was found on using SEQUENTIAL NUMBERING (1, 2, 3, etc.)
- Use the order of images provided, NOT the page numbers printed on the document
- If an entity is "UNKNOWN", set page_number to 0

## OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT include any additional text, explanations, or formatting.
The JSON must be in the following format:
[{{ 
   "entity_name": "entity key name as given in the above entity list (e.g., "proprietorName")", 
   "entity_value": "entity value as extracted from document",
   "confidence": confidence score as number between 0-100,
   "page_number": page number where entity was found,
   "sentence": the short sentence that you extacted the entity from [only if sentence extraction is enabled]
}}]

IMPORTANT: 
- The "entity_name" field should contain the exact entity key (e.g., "proprietorName", "titleDetailAndNumber") not the display name
- The "entity_value" field should contain the actual extracted text from the document
- Include ALL 14 entities in your response, even if some are "UNKNOWN"
- Ensure the JSON is valid and properly formatted
- Review all pages before providing your final response
- Each entity should appear only once in your response (choose the best instance if found on multiple pages)
- Make sure the format can be directly parsed by json.loads() without any additional text or formatting
- DO NOT use backslash escape sequences like \\u2021, \\u2020, etc. in entity values
- REMEMBER: Use sequential page numbering (1 to {actual_page_count}) based on image order, not document labels
    """,
    
    'LU DEV': """
## Task Summary:
You have to do 3 important tasks:  
A) Find and extract a list of entities from the document (list and examples will be given to you).
B) Provide a confidence score number for each of the entities you extract.  
C) Identify which page each entity was found on.

## Context Information:
The following are scanned document pages used as part of a loan application in a bank in Malaysia.
The document type is a Letter of Undertaking from Developer (LU DEV).
The document language is English/Malay.
Sentence extraction is {sentence_extraction}.
You will be analyzing multiple pages of the same document - please review all pages to extract entities.

## IMPORTANT PAGE NUMBERING INSTRUCTIONS:
You are analyzing {actual_page_count} pages total. The document may have internal page labels that DO NOT match the actual page sequence.
- ALWAYS use sequential page numbers starting from 1 for your response
- Page 1 = First image provided
- Page 2 = Second image provided  
- Page 3 = Third image provided
- etc.
- IGNORE any page numbers printed on the document itself
- Only use the sequential order of images provided to you

## Model Instructions:
Instructions about entity extraction (A): 
- Find and extract ALL 26 entities from the following list.
- Look across ALL pages provided to find these entities.
- Don't change anything or make up any information other than what is inside the document.  
- Some entities may not be present across all pages. If you cannot find some of the entities output "UNKNOWN" for that entity.  
- If an entity appears on multiple pages, choose the most complete/clear instance.
- ONLY extract characters that are clearly visible and readable.
- IMPORTANT: Extract text as clean, readable text without Unicode escape sequences (\\u codes)
- If text contains special characters, represent them as the actual characters, not as escape sequences
- Avoid any backslash escape sequences in the extracted text values

List of entities to be extracted, along with their expected type and an example:  
1. salesPurchaseOrderPrice: SPA Price
  Description: NOT loan or facility amount. The purchase or selling price stated in the Sales & Purchase Agreement referenced in the undertaking. 
  Possible Malay Words: Harga SPA
  Example: RM 500,000.00

2. propertyType: Property Type
  Description: The type of property being purchased
  Possible Malay Words: Jenis Hartanah
  Example: Condominium

3. rhbBankName: Check RHB Address
  Description: The specific RHB Bank branch name for verification, either "RHB Bank Berhad", "RHB Islamic Bank Berhad" or "UNKNOWN" if not found
  Example: RHB Bank Berhad

4. bankReferenceNumber: Bank Ref No (AA)
  Description: The bank reference number or application number, typically under the heading "Your Ref"
  Example: 23KLM00123

5. projectName: Project Name
  Description: The name of the development project
  Example: Bangsar Heights

6. unitNo: Parcel /Unit No
  Description: The specific unit number or parcel number of the property
  Possible Malay Words: Nombor Syit Piawi, Nombor Unit
  Example: Unit 12-3A

7. customerName: Borrower/Customer Name
  Description: The name of the borrower(s) or customer(s) applying for the loan
  Possible Malay Words: Nama Peminjam, Nama Pelanggan
  Example: Ahmad bin Abdullah

8. customerIdentificationNumber: Borrower/Customer NRIC/ passport
  Description: The NRIC or passport number of the borrower(s)/customer(s) applying for the loan
  Possible Malay Words: NRIC Peminjam, Nombor Kad Pengenalan Pelanggan
  Example: 123456-78-9012

9. registeredOwnerName: Purchaser/Registered Owner Name
  Description: The name of the purchaser(s) who will be the registered owner(s) of the property, often stated as "Purchaser(s)" in the document
  Possible Malay Words: Nama Pembeli, Nama Pemilik Berdaftar
  Example: Siti binti Hassan

10. registeredOwnerIdentificationNumber: Purchaser/Registered Owner NRIC/ passport
  Description: The NRIC or passport number of the purchaser(s)/registered owner(s), often accomping the registered owner name, not to be confused with the name
  Possible Malay Words: NRIC Pembeli
  Example: 987654-32-1098

11. developerName: Developer Name
  Description: The name of the property developer company issuing the undertaking
  Possible Malay Words: Nama Pemaju
  Example: ABC Development Sdn Bhd

12. developerIdentificationNumber: Developer ID (BRN)
  Description: The Business Registration Number (BRN) or company number of the developer
  Possible Malay Words: Nombor Pendaftaran Syarikat Pemaju, BRN Pemaju
  Example: 123456-A

13. proprietorName: Proprietor Name (Land owner name)
  Description: The name of the land owner or proprietor. If Proprietor name is not explicitly stated, return "N/A". Do not confuse with the purchaser/registered owner.
  Possible Malay Words: Nama Pemilik Tanah
  Example: ABC Holdings Sdn Bhd

14. proprietorIdentificationNumber: Proprietor ID (NRIC/BRN)
  Description: The NRIC or Business Registration Number of the proprietor/land owner. If Proprietor ID is not explicitly stated, return "N/A".
  Possible Malay Words: NRIC/BRN Pemilik Tanah
  Example: 567890-B

15. titleDetailAndNumber: Title Detail & Title No (H.S(D/M) No
  Description: The title type and number (e.g., H.S.(D) or H.S.(M) or GRN) from the land title, ensure the number is captured, should follow the format H.S.(D) 12345 or H.S.(M) 67890 or GRN 1234 and can be extracted from the Master Title if there is no clear title number in the undertaking
  Possible Malay Words: Jenis Hak Milik & Nombor Hak Milik
  Example: H.S.(D) 12345

16. lotDetailsAndNumber: Lot No Details & Lot No PT (D/M) No
  Description: The Lot detail and number (PT (D/M) number) from the land title, ensure the entirity is captured and not leave out the number. possible pointers: "PTD No" or "PT No" or "Lot No" or "PT No Details" or "Lot No Details" or "P.T. No Details". DO NOT confuse with the lot number for non relevant land parcels, only extract the lot number that is relevant to the property being purchased.
  Possible Malay Words: Nombor Lot, PT
  Example: PT 12345 Section 2

17. typeOfLandLocation: Bandar/Pekan/Mukim
  Description: The administrative area type (Bandar/Pekan/Mukim/Town/Village) where the land is located, 
  Possible Malay Words: Bandar, Pekan, Mukim
  Example: "Bandar Kuala Lumpur" or "Pekan Sungai Besi" or "Town Sungai Lalang" or "Mukim Batu"

18. landLocation: Tempat
  Description: Extract only from "Tempat" label. return "UNKNOWN" if not found.
  Possible Malay Words: Tempat, Kawasan
  Example: Bangsar

19. landDistrict: District
  Description: The district where the land is located
  Possible Malay Words: Daerah
  Example: Kuala Lumpur

20. chargeeBankName: Chargee Bank Name
  Description: The name of the bank that holds the charge or mortgage over the property, return "UNKNOWN" if not explicitly mentioned
  Possible Malay Words: Nama Bank Gadaian
  Example: RHB Bank Berhad

21. hdaBeneficiaryBank: HDA Beneficiary Bank
  Description: The bank name for the Housing Development Account (HDA) under the undertaking. If "HDA" is not explicitly stated, return "N/A".
  Possible Malay Words: Bank Benefisiari HDA
  Example: RHB Bank Berhad

22. facilityAmount: Facility Amount
  Description: The total loan or facility amount approved by the bank
  Possible Malay Words: Jumlah Kemudahan
  Example: RM 1,000,000.00

23. hdaAccountNumber: HDA Account
  Description: The Housing Development Account number for fund disbursement under the undertaking
  Possible Malay Words: Nombor Akaun HDA
  Example: 98765432101

24. beneficiaryName: Beneficiary Name
  Description: EXACT Same as developerName
  Possible Malay Words: Nama Penerima dalam Surat Ikatan Pemaju, Nama Benefisiari
  Example: Developer Holdings Sdn Bhd

25. beneficiaryBank: Beneficiary Bank
  Description: EXACT Same as hdaAccountNumber
  Example: RHB Bank Berhad

26. beneficiaryAccountNumber: Beneficiary account no
  Description: The bank account number of the beneficiary for undertaking payments
  Possible Malay Words: Nombor Akaun Penerima, Nombor Akaun Benefisiari
  Example: 12345678901

Instructions about the confidence score (B): 
- For each extracted entity include a confidence score number between 0-100 
- 100 indicates totally certain (i.e., clearly identifiable text and no ambiguity among other candidate entities) 
- 50 indicates some kind of uncertainty (i.e. not clearly identifiable text or multiple candidate values for an entity, making difficult to select the right one). 
- 0 indicates totally uncertainty and implies that you should give "UNKNOWN" for that entity.  
- Any other situations should be treated accordingly within those confidence score.  

Instructions about page identification (C):
- For each entity, specify which page number it was found on using SEQUENTIAL NUMBERING (1, 2, 3, etc.)
- Use the order of images provided, NOT the page numbers printed on the document
- If an entity is "UNKNOWN", set page_number to 0

## OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT include any additional text, explanations, or formatting.
make sure the outputs in the sentences containing " should be '.
The JSON must be in the following format:
[{{ 
   "entity_name": "entity key name as given in the above entity list (e.g., "beneficiaryName")", 
   "entity_value": "entity value as extracted from document",
   "confidence": confidence score as number between 0-100,
   "page_number": page number where entity was found,
   "sentence": the short sentence that you extacted the entity from [only if sentence extraction is enabled]
}}]

IMPORTANT: 
- The "entity_name" field should contain the exact entity key (e.g., "beneficiaryName", "customerName") not the display name
- The "entity_value" field should contain the actual extracted text from the document
- Include ALL 26 entities in your response, even if some are "UNKNOWN"
- Ensure the JSON is valid and properly formatted
- Review all pages before providing your final response
- Each entity should appear only once in your response (choose the best instance if found on multiple pages)
- Make sure the format can be directly parsed by json.loads() without any additional text or formatting
- DO NOT use backslash escape sequences like \\u2021, \\u2020, etc. in entity values
- REMEMBER: Use sequential page numbering (1 to {actual_page_count}) based on image order, not document labels
    """
}





EXAMPLE_INSTRUCTIONS = {
    # SPA Related
    'salesPurchaseOrderDate': {
        'description': 'The date when the Sales & Purchase Agreement was signed or executed, be sure to extract the full date',
        'example': '06 Aug 2024'
    },
    'salesPurchaseOrderPrice': {
        'description': 'The total purchase price stated in the Sales & Purchase Agreement, not to be confused with the loan amount',
        'example': 'RM 500,000.00, 11,000'
    },
    'propertyType': {
        'description': 'The type of property being purchased (e.g., condominium, terrace house, apartment)',
        'example': 'Condominium'
    },
    'propertyAddress': {
        'description': 'The complete address of the property including unit number, street, postcode and state',
        'example': 'Unit 12-3A, Jalan Bangsar, 59200 Kuala Lumpur or Lot No/ Lo No PT 822 Section 4 in the Town of Sungai Besi, District of Kuala Lumpur'
    },
    'projectName': {
        'description': 'The name of the development project',
        'example': 'Bangsar Heights'
    },
    'unitNo': {
        'description': 'The specific unit number or parcel number of the property',
        'malay indicators': 'Nombor Syit Piawi',
        'example': 'Unit 12-3A'
    },
    
    # Customer/Owner Information
    'customerName': {
        'description': 'The name of the borrower or customer applying for the loan',
        'example': 'Ahmad bin Abdullah'
    },
    'customerIdentificationNumber': {
        'description': 'The NRIC or passport number of the borrower/customer applying for the loan, be sure to not confuse part of the name with the identification number',
        'example': '123456-78-9012'
    },
    'registeredOwnerName': {
        'description': 'The name of the purchaser who will be the registered owner of the property, look for the name under "Rekod Ketuanpunyaan" if available',
        'example': 'Siti binti Hassan'
    },
    'registeredOwnerIdentificationNumber': {
        'description': 'The NRIC or passport number of the purchaser/registered owner, be sure to not confuse part of the name with the identification number',
        'example': '987654-32-1098'
    },
    'vendorName': {
        'description': 'The name of the person or entity selling the property, do not be confused with other land names',
        'example': 'Hassan bin Ali'
    },
    'vendorIdentificationNumber': {
        'description': 'The NRIC, passport or company registration number of the vendor, be sure to not confuse part of the name with the identification number',
        'example': '456789-01-2345'
    },
    
    # Developer Information
    'developerName': {
        'description': 'The name of the property developer company',
        'example': 'ABC Development Sdn Bhd'
    },
    'developerIdentificationNumber': {
        'description': 'The Business Registration Number (BRN) or company number of the developer',
        'malay indicators': 'Syarikat means company, often cut in short as skyt',
        'example': '123456-A'
    },
    
    # Proprietor Information
    'proprietorName': {
        'description': 'The name of the land owner or proprietor as shown in the title, which may be different from the developer or vendor',
        'malay indicators': 'Nama Pemilik Tanah or is indicated by "pindahmilik tanah oleh xxx kepada yyy, which means transfer of land from xxx to yyy"',
        'example': 'ABC Holdings Sdn Bhd'
    },
    'proprietorIdentificationNumber': {
        'description': 'The NRIC or Business Registration Number of the proprietor/land owner',
        'malay indicators': 'Syarikat means company, often cut in short as skyt',
        'example': '567890-B'
    },
    
    # Bank Information
    'rhbBankName': {
        'description': 'The specific RHB Bank branch name and address for verification',
        'example': 'RHB Bank Berhad or RHB Islamic Bank Berhad'
    },
    'bankReferenceNumber': {
        'description': 'The bank reference number or application number (usually starts with bank code)',
        'example': 'RHB/AA/2024/001234'
    },
    'chargeeBankName': {
        'description': 'The name of the bank that holds the charge or mortgage over the property',
        'malay indicators': 'Gadaian menjamin wang pokok means the charge guarantees the principal amount',
        'example': 'RHB Bank Berhad'
    },
    'chargeeBankEndorsement': {
        'description': 'Details of the bank charge or mortgage endorsement on the title',
        'example': 'Charge in favour of RHB Bank'
    },
    'hdaBeneficiaryBank': {
        'description': 'The bank name for the Housing Development Account (HDA)',
        'example': 'RHB Bank Berhad'
    },
    'hdaAccountNumber': {
        'description': 'The Housing Development Account number for fund disbursement',
        'example': '98765432101'
    },
    'facilityAmount': {
        'description': 'The total loan or facility amount approved by the bank',
        'example': 'RM 1,000,000.00'
    },
    
    # Billing Information
    'billingStage': {
        'description': 'The current construction stages for which billing is made',
        'example': '(2a) Foundation Work, (2b) The drains serving the said building'
    },
    'billingPercentage': {
        'description': 'The percentage of work completed for each of the billing stages',
        'example': '25%, 10%'
    },
    'billingAmount': {
        'description': 'The total amount being billed for the current stages of construction',
        'example': 'RM 125,000.00'
    },
    'architectCertStage': {
        'description': 'The construction stages certified by the architect, which may include multiple stages but only extract the ones relevant to the billing',
        'example': '(2a) Foundation Work, (2b) The drains serving the said building'
    },
    'architectCertPercentage': {
        'description': 'The percentages of work certified as completed by the architect, associated with the architectCertStage',
        'example': '50%,10%'
    },
    'architectCertCompletionStatus': {
        'description': 'The status of completion as certified by the architect or the number of plots completed',
        'example': 'Completed or 20-36 plots completed for (2a) Foundation Work'
    },
    
    # Beneficiary Information
    'beneficiaryName': {
        'description': 'The name of the beneficiary who will receive the payment',
        'malay indicators': 'Nama Penerima',
        'example': 'XYZ Construction Sdn Bhd'
    },
    'beneficiaryBank': {
        'description': 'The bank name where the beneficiary account is held',
        'malay indicators': 'Bank Penerima',
        'example': 'RHB Bank Berhad'
    },
    'beneficiaryAccountNumber': {
        'description': 'The bank account number of the beneficiary for payment',
        'malay indicators': 'Nombor Akaun Penerima',
        'example': '12345678901'
    },
    
    # Land/Title Information
    'titleDetailAndNumber': {
        'description': 'The title type and number (e.g., H.S.(D) or H.S.(M)) from the land title, ensure the number is captured',
        'malay indicators': 'Jenis Hak Milik & Nombor Hak Milik',
        'example': 'H.S.(D) 12345'
    },
    'lotDetailsAndNumber': {
        'description': 'The lot number and details (P.T. number) from the land title',
        'example': 'PT 12345 Section 2'
    },
    'typeOfLandLocation': {
        'description': 'The administrative area type (Bandar/Pekan/Mukim/Town/Village) where the land is located, take the one that is not struck through or if all are struck through, take the one that is in the title. for example "Town/Mukim(crossed)/Pekan(crossed) Sungai Lalang" then you should take "Town Sungai Lalang". )',
        'example': 'Bandar Kuala Lumpur or Pekan Sungai Besi or Town Sungai Lalang or Mukim Batu'
    },
    'landLocation': {
        'description': 'The specific place or area name (Tempat) where the land is situated',
        'malay indicators': 'Tempat',
        'example': 'Bangsar'
    },
    'landDistrict': {
        'description': 'The district where the land is located',
        'malay indicators': 'Daerah',
        'example': 'Kuala Lumpur'
    },
    'landArea': {
        'description': 'The total area of the land parcel, ensure to take the right unit of measurement (e.g., square feet, acres)',
        'malay indicators': 'Keluasan Tanah',
        'example': '1000 sq ft'
    },
    'landUse': {
        'description': 'The approved use of the land (e.g., residential, commercial, industrial, bangunan), in malay it may be referenced as "Kegunaan Tanah"',
        'malay indicators': 'Kegunaan Tanah',
        'example': 'Residential'
    },
    'builtUpArea': {
        'description': 'The total built-up area of the property or building, which may be different from the land area',
        'malay indicators': 'Keluasan',
        'example': '800 sq ft'
    },
    'actualCondition': {
        'description': 'The actual condition or restriction (Syarat Nyata) attached to the land',
        'malay indicators': 'Syarat Nyata',
        'example': 'Residential Building'
    },
    'restrictionInInterest': {
        'description': 'Any restrictions in interest or limitations on the land use (Sekatan Kepentingan in malay)',
        'malay indicators': 'Sekatan',
        'example': 'None'
    }
}

FORM_TABLE_DATA_MAPPING = {
    'salesPurchaseOrderDate': 'formData',
    'salesPurchaseOrderPrice': 'formData',
    'propertyType': 'formData',
    'propertyAddress': 'formData',
    'rhbBankName': 'formData',
    'bankReferenceNumber': 'formData',
    'projectName': 'formData',
    'unitNo': 'formData',
    'customerName': 'formData',
    'customerIdentificationNumber': 'formData',
    'registeredOwnerName': 'formData',
    'registeredOwnerIdentificationNumber': 'formData',
    'developerName': 'formData',
    'developerIdentificationNumber': 'formData',
    'proprietorName': 'formData',
    'proprietorIdentificationNumber': 'formData',
    'vendorName': 'formData',
    'vendorIdentificationNumber': 'formData',
    'titleDetailAndNumber': 'formData',
    'lotDetailsAndNumber': 'formData',
    'typeOfLandLocation': 'formData',
    'landLocation': 'formData',
    'landDistrict': 'formData',
    'landArea': 'formData',
    'landUse': 'formData',
    'builtUpArea': 'formData',
    'actualCondition': 'formData',
    'restrictionInInterest': 'formData',
    'chargeeBankName': 'formData',
    'hdaBeneficiaryBank': 'formData',
    'hdaAccountNumber': 'formData',
    'facilityAmount': 'formData',
    'billingStage': 'formData',
    'billingPercentage': 'formData',
    'billingAmount': 'formData',
    'architectCertStage': 'formData',
    'architectCertPercentage': 'formData',
    'architectCertCompletionStatus': 'formData',
    'beneficiaryName': 'formData',
    'beneficiaryBank': 'formData',
    'beneficiaryAccountNumber': 'formData',
    'chargeeBankEndorsement': 'formData'
}