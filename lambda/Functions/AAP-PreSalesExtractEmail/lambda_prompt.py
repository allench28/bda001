# Email Body Text Analysis Prompt
ANALYZE_EMAILBODY_SUBJECT_TENDER_PROMPT = """
TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    1. Subject (The subject of the message)
    2. Tender (True or false if the email is a tender process)
    3. Email Sent Date (The date of the message started)
    4. Analysis Report (Your reason of your output for extraction)

2. You are provided:
    a. An Email Content within the <email_input> tags for analysis

3. Standardize and clean the data:
    - Standardize email content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR DETERMINING TENDER:
    - An email should be classified as a tender request if the content includes any of the following keywords or phrases with relevant context:
        - "RFQ" or "Request for Quotation"
        - "RFP" or "Request for Proposal"
        - "Tender"
        - "Tender code" or "Tender number"
        - "Closing date"
    - An email should NOT be classified as a tender request if it only contains a general term like "quote", "propose", or "quotation" without any additional context suggesting a formal tender or bidding process.

RULES FOR EXTRACTING SUBJECT:
    - Extract from {subject}
    - Exclude any forwarding or reply prefixes such as "FW:", "FWD:", "RE:", or "Re:" that appear at the beginning of the Subject.

RULES FOR EXTRACTING EMAIL SENT DATE:
    - Extract from {emailSentDate}
    - Extract ONLY ONE date 
    - Strictly follow this date time format: DD-MM-YYYYTHH:MM:SS.ZZZZZZ

Below is the example of determining tender process:
input
From: William Yap <william.yap@vendorbase.org>
Sent: Friday, 14 February, 2025 6:54 PM
To: Elvan Poon <elvanpoon@vstecs.com.my>
Subject: RFQ Dell R750
 
Dear Mr. Elvan,
  
Quote for Dell R750 (Not R750XS Or R760 Or R760XS) 2U Rack-Mount Server  
Intel Xeon Silver 4310 2.1G, 12C/24T, 10.4GT/s, 18M Cache, Turbo, HT (120W) DDR4-2666 X 1
16GB RDIMM, 3200MT/s, Dual Rank X 1     
 
Yours Truly,
 
William Yap Fook Chong

output:
Tender is true because you can find keywords such as RFQ and Quote.

If the email content in <email_input>{email_input}</email_input> ONLY consist ONE message, then return the email metadata in the following:
- subject: <email_metadata>{subject}</email_metadata>
- emailSentDate: <email_metadata>{emailSentDate}</email_metadata>

INPUT:
<email_input>
{email_input}
</email_input>

<email_metadata>
{subject}
{emailSentDate}
</email_metadata>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "subject": "Request - Alto Product",
    "emailSentDate": "DD-MM-YYYYTHH:MM:SS.microsecondsZ",
    "isTender": True or False,
    "analysisReport": "I extracted this because...."
}}

"""

# ANALYZE_EMAILBODY_PRODUCT_BRAND_SUPP_KU_PROMPT = """
# TASK:
# 1 Analyze the email content in the input JSON Object and determine the following key information:
#     a. Product Name: The name of the inquired or requested products (if mentioned)
#     b. Chinese Product Price: List all Product Name and Price of Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.) found in the given products list.
#     c. Chinese Product Support: Extract any mention of warranty or support/maintenance services for Chinese brand products.
#     d. Western Product Price: List all Product Name and Price of Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC etc.) found in the given products list.
#     e. Western Product Support: Extract any mention of warranty or support/maintenance services for Western brand products.
#     f. KU Services: Extract any mention of professional services (such as consulting, configuration, or advisory services), especially those referring to the keyword "advise".
#     e. Brand: The brand associated with the product.
#     g. Analysis Report (Your reason of your output for extraction)

# 2. You are provided:
#     a. You are provided an Email Content within the <email_input> tags for analysis
#     b. You are provided multiple brand mapping data input within the <mapping_fields> tags to cross-reference the brands and products

# 3. Standardize and clean the data
#     a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
#     b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
#     c. Use fuzzy matching to handle near-identical descriptions or naming variations.

# RULES FOR EXTRACTING PRODUCT NAMES:
#     - Based on the products mentioned in Subject Line or message body.
#     - MUST include all the different products specified, even in the tables.
#     - DO NOT extract products in message footer.
#     - For any mentioning of product swapping, extract ONLY the product that is required after the swap. DO NOT extract the product before swap.
    
# RULES FOR EXTRACTING CHINESE PRODUCT PRICE:
#     - From the "productName" list, select only those products that are Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.) according to the mapping fields.
#     - For each selected product, extract its price and include it in the "ChineseProductPrice" dictionary, where the product name is the key and the price is the value.
#     - Extract the MYR/RM currency price in the email body if it is explicitly mentioned
#     - The RM price amount usually look like the format below:
#         - MYR 100, RM 100, 100
#     - RETURN ONLY '-' if there are no prices found for the products within the "productName" list.
#     - DO NOT provide random value, if the price is not explicitly mentioned.

# RULES FOR EXTRACTING WESTERN PRODUCT PRICE:
#     - From the "productName" list, select only those products that are Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC, etc.) according to the mapping fields.
#     - For each selected product, extract its price and include it in the "WesternProductPrice" dictionary, where the product name is the key and the price is the value.
#     - Extract the product USD currency price in the email body if it is explicitly mentioned, otherwise convert the price amount to USD according to the USD/MYR rate.
#     - The USD price amount usually look like the format below:
#         - $100, USD 100, 100
#     - RETURN ONLY '-' if there are no prices found for the products within the "productName" list.
#     - DO NOT extract random value, if the price is not explicitly mentioned.

# RULES FOR EXTRACTING CHINESE PRODUCT SUPPORT:
#     - Extract any mention of warranty or support/maintenance services for Chinese brand products only.
#     - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
#         - Example: Required warranty for Huawei switch router

# RULES FOR EXTRACTING WESTERN PRODUCT SUPPORT:
#     - Extract any mention of warranty or support/maintenance services for Western brand products only.
#     - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
#         - Example: Required warranty for Dell Server

# RULES FOR EXTRACTING KU SERVICE:
#     - Return a detailed statement of which product required which professional service like the following example:
#         - Example: Required advisory service for Huawei switch router

# RULES AND INSTRUCTIONS FOR BRAND MAPPING:
#     1. For products that are generic (e.g., “Router”, “Switch”, “Firewall”) follow the example below
#         a. Search other parts of the message thread to find any **mentioned brand** 
#         b. Link the mentioned brand accordingly to the mapping fields <mapping_fields>
#         c. DO NOT make up random brands, STRICTLY follow ONLY the brand mentioned in the message thread
#     2. If the product is very specific (e.g., “Catalyst 2960”), determine the brand based on the mapping fields <mapping_fields>
#     3. Always match the product with the mapping fields <mapping_fields>
#     4. Follow the context below when determinig brands:
#         - The following is the explanation of the Teams:
#             a. Data Center: 
#                 - Handles infrastructure-related topics: server hardware, networking, storage, and data center operations.    
#             b. Software Team: 
#                 - Handles software inquiries — licensing, implementation, support, and infrastructure integration.
#             c. EN & Collabs: 
#                 - Handles network and collaboration technologies — support effective communication and ensure reliable connectivity.
#             d. Security Team: 
#                 - Handles all security products and topics - firewall, cyber threats, devices, security networks, and prevention systems.
#         - Ensure the assigned Team aligns with the Brand like the example below:
#             - Data Center: {dataCenterBrandMapping}
#             - Software Team: {softwareBrandMapping}
#             - EN & Collabs: {enCollabsBrandMapping}
#             - Security Team: {securityBrandMapping} and {ciscoSecurePortfolioBrandMapping} 
#     5. ONLY return each identified brand names ONCE in the list
#     6. DO NOT make up random brands, STRICTLY follow the brands and products in <mapping_fields>

# Determine the brand according the following example:
# The Product Name identified from <email_input> is Cisco_Webex, and it was found in the items or product lines of {enCollabsBrandMapping} where the mapping field looks like below example:
# {enCollabsBrandMapping} = {{
#     "Cisco_Collab": ["PABX", "Webex", "A-Flex", "VideoConference"],
# }}
# Hence, the Brand mapped to Cisco_Webex is Cisco_Collab. 

# Please be specific when matching the products to Brand, follow the example below:
# When Product identified is MX100-HW is a Cisco brand, you may find it in both {enCollabsBrandMapping} and {securityBrandMapping} where the mapping fields looks like below example:
# {enCollabsBrandMapping} = {{
#     "Cisco": ["Cisco_Router & Switch & Wireless", "Catalyst", "Meraki"]
# }}
# {securityBrandMapping} = {{
#     "Cisco_S&R": ["Catalyst C9K", "Catalyst 8K", "Meraki MS", "Meraki MV", "Meraki MX", "Meraki MDM", "IE Series", "IR series"],
# }}
# The Product "ACX7509" clearly indicate an ACX series, which can be found in the items or product lines of {securityBrandMapping} 
# Hence, the Brand should be "Cisco_S&R", not "Cisco".

# INPUT:
# <email_input>
# {email_input}
# </email_input>

# <mapping_fields>
# {dataCenterBrandMapping}
# {softwareBrandMapping}
# {enCollabsBrandMapping}
# {securityBrandMapping}
# {ciscoSecurePortfolioBrandMapping}
# </mapping_fields>

# IMPORTANT:
#     - Return a list if the information is found in the email or a value is determined
#     - DO NOT return list or array if the information not found in the email or unable to determine a value, set the value to '-' instead

# OUTPUT FORMAT:
# Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
# DO NOT remove any keys from the input JSON object:
# {{
#     "brand": ["Brand1", "Brand2"] or '-',
#     "productName": ["Product1", "Product2"] or '-',
#     "ChineseProductPrice": {{
#         "ProductA": "123.00",
#         "ProductB": "456.00"
#     }},
#     "chineseProductSupport": ["Huawei 3-year warranty"] or '-',
#     "WesternProductPrice": {{
#         "ProductC": "123.00",
#         "ProductD": "456.00"
#     }},
#     "westernProductSupport": ["IBM maintenance"] or '-',
#     "kuServices": ["Professional advisory service"] or '-',
#     "analysisReport": "I extracted this because...."
# }}

# """

# SPLIT PROMPT FOR PRODUCT, SUPPORT, AND BRAND ANALYSIS
ANALYZE_EMAILBODY_PRODUCT_BRAND_PROMPT = """
TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    a. Specific Products: The specific model/name of the inquired or requested products (e.g, Huawei Atlas X300, APC-5000VA)
    b. Brand: The brand associated with the product.
    c. General Products: The products shown in general terms (e.g, Server, Switch, Storage, Infrastructure), which is not a model name.
    d. Entity: The company department (Astar or Pericomp)
    e. Analysis Report (Your reason of extracting Specific and Generic Product)

2. You are provided:
    a. You are provided an Email Content within the <email_input> tags for analysis
    b. You are provided multiple brand mapping data input within the <mapping_fields> tags to cross-reference the brands and products

3. Standardize and clean the data
    a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

INSTRUCTIONS:
    1. Extract all the products found in <email_input>
    2. Go through Email Body first to find products, then go through Subject Line if there's no products in Email Body. 
    3.Analyze the extracted products to find the generic product names (e.g, Generic: Smart Server, Specific: Huawei Atlas 800)
    4. Separate the generic and specific product names into different list specificProduct" and "generalProduct"
    5. ONLY determine the brand of products in "specificProduct" list with <mapping_fields>

RULES FOR EXTRACTING ENTITY:
    - Identify the VSTECS department based on the message footer
    - Must be VSTECS footer
    - You must either return Astar or Pericomp

RULES FOR EXTRACTING SPECIFIC PRODUCTS:
    - DO NOT extract if the brand cannot be directly determined or the product is not a model,
    - Based on the products mentioned in Subject Line or message body.
    - MUST include all the different products specified, even in the tables.
    - The products extracted MUST be specified with a model or brand, such as below example:  
        - HPE DL360 Gen10
        - Cisco Nexus 93180YC-EX
    - DO NOT extract products in message footer.
    - For any mentioning of product swapping, extract ONLY the product that is required after the swap. DO NOT extract the product before swap.
    
RULES FOR EXTRACTING GENERAL PRODUCTS:
    - Analyze the products extracted
    - Extract general IT product objects ONLY if the brand cannot be directly determined with <mapping_fields>, such as:
        - Server
        - Firewall
        - Storage
        - Other generic IT hardware components
    - MUST include all the different products specified, even in the tables.
    - ONLY extract if the word appears as a standalone product name or with minimal descriptive modifiers (e.g., "Switch" or "Smart Switch").    
    - DO NOT extract terms like "Switch" when they are part of a full product model name or specification (e.g., "Cisco Switch 4000").        
    - DO NOT extract products in message footer.
    - RETURN '-' if there is no generic product.

Extract the products according the following example:
The email content consist a sentence that shows the products: 
    "We are planning to deploy new firewalls along with the Palo Alto PA-850 in our data center. Cisco router needed for branch office setup"
Hence, you should extract the products as below:
    Generic product: Firewall, Cisco router
    Specific product: Palo Alto PA-850

RULES AND INSTRUCTIONS FOR BRAND MAPPING:
    1. ONLY map for the products in PRODUCT NAMES list, DO NOT use GENERAL PRODUCTS.
    2. If the product is very specific (e.g., “Catalyst 2960”), determine the brand based on the mapping fields <mapping_fields>
    3. Always match the product with the mapping fields <mapping_fields>
    4. Follow the context below when determinig brands:
        - The following is the explanation of the Teams:
            a. Data Center: 
                - Handles infrastructure-related topics: server hardware, networking, storage, and data center operations.    
            b. Software Team: 
                - Handles software inquiries — licensing, implementation, support, and infrastructure integration.
            c. EN & Collabs: 
                - Handles network and collaboration technologies — support effective communication and ensure reliable connectivity.
            d. Security Team: 
                - Handles all security products and topics - firewall, cyber threats, devices, security networks, and prevention systems.
        - Ensure the assigned Team aligns with the Brand like the example below:
            - Data Center: {dataCenterBrandMapping}
            - Software Team: {softwareBrandMapping}
            - EN & Collabs: {enCollabsBrandMapping}
            - Security Team: {securityBrandMapping} and {ciscoSecurePortfolioBrandMapping} 
    5. ONLY return each identified brand names ONCE in the list
    6. DO NOT make up random brands, STRICTLY follow the brands and products in <mapping_fields>

Determine the brand according the following example:
The Product Name identified from <email_input> is Cisco_Webex, and it was found in the items or product lines of {enCollabsBrandMapping} where the mapping field looks like below example:
{enCollabsBrandMapping} = {{
    "Cisco_Collab": ["PABX", "Webex", "A-Flex", "VideoConference"],
}}
Hence, the Brand mapped to Cisco_Webex is Cisco_Collab. 

Please be specific when matching the products to Brand, follow the example below:
When Product identified is MX100-HW is a Cisco brand, you may find it in both {enCollabsBrandMapping} and {securityBrandMapping} where the mapping fields looks like below example:
{enCollabsBrandMapping} = {{
    "Cisco": ["Cisco_Router & Switch & Wireless", "Catalyst", "Meraki"]
}}
{securityBrandMapping} = {{
    "Cisco_S&R": ["Catalyst C9K", "Catalyst 8K", "Meraki MS", "Meraki MV", "Meraki MX", "Meraki MDM", "IE Series", "IR series"],
}}
The Product "ACX7509" clearly indicate an ACX series, which can be found in the items or product lines of {securityBrandMapping} 
Hence, the Brand should be "Cisco_S&R", not "Cisco".

INPUT:
<email_input>
{email_input}
</email_input>

<mapping_fields>
{dataCenterBrandMapping}
{softwareBrandMapping}
{enCollabsBrandMapping}
{securityBrandMapping}
{ciscoSecurePortfolioBrandMapping}
</mapping_fields>

IMPORTANT:
    - Return a list if the information is found in the email or a value is determined
    - DO NOT return list or array if the information not found in the email or unable to determine a value, set the value to '-' instead

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "brand": ["Brand1", "Brand2"] or '-',
    "specificProduct": ["Product1", "Product2"] or '-',
    "generalProduct": ["Product1", "Product2"] or '-',
    "entity": "Astar" or "Pericomp",
    "analysisReport": "I extracted this because...."
}}

"""

ANALYZE_EMAILBODY_PRODUCT_SUPP_KU_PROMPT = """
TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    a. Chinese Product Price: List of Chinese brand products (e.g., Huawei, Lenovo, xFusion, etc.) with price mentioned.
    b. Chinese Product Support: Extract any mention of warranty or support/maintenance services for Chinese brand products.
    c. Western Product Price: List of Western brand products (e.g., IBM, Dell, HPE, Cisco, Juniper, APC etc.) with price mentioned.
    d. Western Product Support: Extract any mention of warranty or support/maintenance services for Western brand products.
    e. KU Services: Extract any mention of professional services (such as consulting, configuration, or advisory services), especially those referring to the keyword "advise".
    f. Analysis Report (Your reason of your output for extraction)

2. You are provided:
    a. You are provided an Email Content within the <email_input> tags for analysis

3. Standardize and clean the data
    a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR EXTRACTING CHINESE PRODUCT PRICE:
    - From <att_input>, select only those products that are Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.).
    - For each selected product, extract its price and include it in the "ChineseProductPrice" dictionary, where the product name is the key and the price is the value.
    - Extract the MYR/RM currency price in the email attachment text ONLY IF it is explicitly mentioned
    - The RM price amount usually look like the format below:
        - MYR 100, RM 100, 100
    - If there are no prices found for the products in the <att_input>, DO NOT RETURN any value:
        - Example: You found a product 'IBM Server', but no specific prices are mentioned anywhere. Hence, DO NOT RETURN the product or any value.
    - DO NOT provide random value, if the price is not explicitly mentioned.
    - RETURN a string '-' if the list is empty:
        - Example: 'chineseProductPrice': '-'

RULES FOR EXTRACTING WESTERN PRODUCT PRICE:
    - From the <att_input>, select only those products that are Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC, etc.).
    - For each selected product, extract the price and include it in the "WesternProductPrice" dictionary, where the product name is the key and the price is the value.
    - Extract the product USD currency price in the email attachment text ONLY IF it is explicitly mentioned, otherwise convert the price amount to USD according to the USD/MYR rate.
    - The USD price amount usually look like the format below:
        - $100, USD 100, 100
    - If there are no prices found for the products in the <att_input>, DO NOT RETURN any value:
        - Example: You found a product 'IBM Server', but no specific prices are mentioned anywhere. Hence, DO NOT RETURN the product or any value.    - DO NOT extract random value, if the price is not explicitly mentioned.
    - RETURN a string '-' if the list is empty:
        - Example: 'westernProductPrice': '-'

RULES FOR EXTRACTING CHINESE PRODUCT SUPPORT:
    - Extract any mention of warranty or support/maintenance services for Chinese brand products only.
    - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
        - Example: Required warranty for Huawei switch router

RULES FOR EXTRACTING WESTERN PRODUCT SUPPORT:
    - Extract any mention of warranty or support/maintenance services for Western brand products only.
    - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
        - Example: Required warranty for Dell Server

RULES FOR EXTRACTING KU SERVICE:
    - Return a detailed statement of which product required which professional service like the following example:
        - Example: Required advisory service for Huawei switch router

INPUT:
<email_input>
{email_input}
</email_input>

IMPORTANT:
    - Return a list if the information is found in the email or a value is determined
    - DO NOT return list or array if the information not found in the email or unable to determine a value, set the value to '-' instead

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "ChineseProductPrice": {{
        "ProductA": "123.00",
        "ProductB": "456.00"
    }} or '-',
    "chineseProductSupport": ["Huawei 3-year warranty"] or '-',
    "WesternProductPrice": {{
        "ProductC": "123.00",
        "ProductD": "456.00"
    }} or '-',
    "westernProductSupport": ["IBM maintenance"] or '-',
    "kuServices": ["Professional advisory service"] or '-',
    "analysisReport": "I extracted this because...."
}}

"""

ANALYZE_EMAILBODY_RESELLER_PROMPT = """
You are VSTECS company's expert AI assistant who analyzes email thread metadata and content provided inside <email_input>. 
TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    1. Reseller (Company that requests or inquires for product or service) 
    2. Redirector Address (The email address of the VSTEC entity who redirects the external request to VSTEC internal team)
    3. Analysis Report (Your reason of your output for extraction)

2. You are provided:
    a. An Email Content within the <email_input> tags for analysis

3. Standardize and clean the data:
    - Standardize email content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES AND INSTRUCTIONS FOR IDENTIFYING RESELLER:
    1. DO NOT make your own decision, strictly follow the given rules and instructions.
    2. The Reseller is the external company that DIRECTLY communicates with VSTECS regarding the product or service request or inquiry.
    3. DO NOT confuse the reseller with the customer or end user. 
    4. First, check the message footer for the company name that requests or inquires about the product or service.
    5. If the company name is NOT explicitly mentioned in the footer, check the person's email address to determine the reseller entity.
    6. The Reseller's company name MUST appear in the email address domain of the sender or recipient who is directly communicating with VSTECS.
    7. DO NOT extract or return any company name that starts with "VSTECS". If the identified reseller starts with "VSTECS", return '-' instead.
    8. DO NOT extract or return any company name if the email is a delivery notice or announcement.
    9. If there is NO MATCH between the company name and the email address domain in the messages, then there is NO Reseller. Set the value to '-'.
    10. If no valid Reseller is found after applying all the above rules, set the value to '-'.

RULES FOR DETERMINING REDIRECTOR ADDRESS:
    1. Redirector forwards or redirects any email to VSTECS internal group, team, or personnel.
    2. Redirector Address MUST be VSTECS personnel.
    3. It acts as the point of contact between external and internal parties.
    4. DO NOT extract Reseller email address if Redirector Address is not determined.

Below is the example of extracting Reseller:
input:
Hi PAG Team

Seeking your help on below, able to configure 2 DSCs?

1. Dell R750 (Not R750XS Or R760 Or R760XS) 2U Rack-Mount Server  
Intel Xeon Silver 4310 2.1G, 12C/24T, 10.4GT/s, 18M Cache, Turbo, HT (120W) DDR4-2666 X 1
16GB RDIMM, 3200MT/s, Dual Rank X 1

Regards,
 
Lee Wei An
Senior Product Executive (Dell EMC Server & Networking)
 
VSTECS ASTAR SDN BHD (263791-K)
Lot 3, Jalan Teknologi 3/5,
Taman Sains Selangor, Kota Damansara,
47810 Petaling Jaya, Selangor, Malaysia.
DID No.:          03-6286 8287
Mobile No.:       019-2440263
Website:          www.vstecs.com.my
_______________________________
From: William Yap <william.yap@vendorbase.org>
Sent: Friday, 14 February, 2025 6:54 PM
To: Elvan Poon <elvanpoon@vstecs.com.my>
Subject: RFQ Dell R750
 
Dear Mr. Elvan,
 
Please advise the price and stock status for the following (End user : HCLTech)
 
1. Dell R750 (Not R750XS Or R760 Or R760XS) 2U Rack-Mount Server  
 
Should you require any further information, please do not hesitate to contact me (anytime), I will always ensure you my personal attention.
 
Thank you very much.           
 
Yours Truly,
 
William Yap Fook Chong
H/P : 012-3788823
Email : william.yap@vendorbase.org
Vendorcom IT Consulting  (Company No:001211107-K, GST No. 001773813760)                           
No 24-1, Jalan PUJ 3/2, Taman Puncak Jalil, 43300, Seri Kembangan, Selangor
Tel : 603-89440839 Fax : 603-89440829
Thank You For Your Support

output:
The reseller can be explicitly found in the footer of the message which is Vendorcom IT Consulting, otherwise it can be found in the email address of requester or inquirer which is "william.yap@vendorbase.org".

INPUT:
<email_input>
{email_input}
</email_input>

IMPORTANT:
    - Always provide full and complete name for Reseller
    - If the information not found in the email or unable to determine a value, set the value to '-'

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:

{{
    "reseller": "XYZ Sdn. Bhd." or "XYZ (Xi Yang Zi)",
    "redirectorAddress": "abc@email.com",
    "analysisReport": "I extracted this because...."
}}

"""

ANALYZE_EMAILBODY_EU_INDUSTRY_PROMPT = """
TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    1. End User Name (Company or a person that USES the product or service)
    2. Industry (Industry of the End User's company)

2. You are provided:
    a. An Email Content within the <email_input> tags for analysis

3. Standardize and clean the data:
    - Standardize email content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR IDENTIFYING END USER NAME:
    - DO NOT make your own decision, strictly follow the given rules
    - End user must be the entity that uses the product or service
    - Extract End User ONLY IF mentioned explicitly such as the following examples:
        a. EU: Yokogawa
        b. End User: Yokogawa
        c. End User is Yokogawa
        d. Customer: Yokogawa
        e. Customer is Yokogawa
    - DO NOT return any End User name if it is not explicitly mentioned
    - Return '-' if unable to extract End User.

RULES FOR DETERMINING INDUSTRY:
    - Check for End User company details such as company name or End User name
    - Determine industry according ONLY to the End User company name or End User name
    - DO NOT provide random industry if End User is not identified

Below is the example of extracting End User and determining the End User's Industry:
input:
Hi PAG Team

Seeking your help on below, able to configure 2 DSCs?

1. Dell R750 (Not R750XS Or R760 Or R760XS) 2U Rack-Mount Server  
Intel Xeon Silver 4310 2.1G, 12C/24T, 10.4GT/s, 18M Cache, Turbo, HT (120W) DDR4-2666 X 1
16GB RDIMM, 3200MT/s, Dual Rank X 1

Regards,
 
Lee Wei An
Senior Product Executive (Dell EMC Server & Networking)
 
VSTECS ASTAR SDN BHD (263791-K)
Lot 3, Jalan Teknologi 3/5,
Taman Sains Selangor, Kota Damansara,
47810 Petaling Jaya, Selangor, Malaysia.
DID No.:          03-6286 8287
Mobile No.:       019-2440263
Website:          www.vstecs.com.my
_______________________________
From: William Yap <william.yap@vendorbase.org>
Sent: Friday, 14 February, 2025 6:54 PM
To: Elvan Poon <elvanpoon@vstecs.com.my>
Subject: RFQ Dell R750
 
Dear Mr. Elvan,
 
Please advise the price and stock status for the following (End user : HCLTech)
 
1. Dell R750 (Not R750XS Or R760 Or R760XS) 2U Rack-Mount Server  
 
Should you require any further information, please do not hesitate to contact me (anytime), I will always ensure you my personal attention.
 
Thank you very much.           
 
Yours Truly,
 
William Yap Fook Chong
H/P : 012-3788823
Email : william.yap@vendorbase.org
Vendorcom IT Consulting  (Company No:001211107-K, GST No. 001773813760)                           
No 24-1, Jalan PUJ 3/2, Taman Puncak Jalil, 43300, Seri Kembangan, Selangor
Tel : 603-89440839 Fax : 603-89440829
Thank You For Your Support

output:
End User is explicitly mentioned which is HCLTech, and HCLTech is from an IT industry. 
The reseller can be found in the footer of the message which is Vendorcom IT Consulting, otherwise it can be found in the email address of requester or inquirer which is "william.yap@vendorbase.org"


INPUT:
<email_input>
{email_input}
</email_input>

IMPORTANT:
    - Always provide full and complete name for End User Name and Industry
    - Always make attempt on identifying and determining the key information
    - If the information not found in the email or unable to determine a value, set the value to '-'

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:

{{
    "endUserName": "ABC Sdn. Bhd." or "ABC (Aaron Bin Corri)",
    "industry": "Information Technology",
}}

"""

ANALYZE_TEAM_VENDOR_PROMPT = """
INPUT:
<email_input>
{brandInput}
{attachmentBrandInput}
</email_input>

<mapping_fields>
{team}
</mapping_fields>

TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    a. Vendor: The company that owns the brand.
    b. Team: The team associated with the brand.

2. You are provided:
    a. You are provided a list of brands in {brandInput} and {attachmentBrandInput}for analysis
    b. You are provided the Teams that are responsible for handling different brands 
    c. You are provided multiple brand mapping data input within the <mapping_fields> tags to cross-reference the brands, and products

3. Standardize and clean the data
    a. Standardize product and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES TO DETERMINE VENDOR:
    1. Determine the vendor ONLY based on the given Brand in {brandInput} and {attachmentBrandInput} tag
    2. ONLY return each identified vendor names ONCE in the list
    3. The following is the example for brand and vendor relationship:
        - The identified brand is Cisco_SEC, then the vendor should be Cisco

RULES TO DETERMINE THE TEAM WHO HANDLES THE BRAND:
    1. Determine the team who are responsible for handling all Brands in {brandInput}, DO NOT miss out any Brand.
    2. Only assign the team if the brand name in {brandInput} is an exact match to one of the brand names in {team}. 
    3. When matching a brand from {brandInput} to a team in {team}, you MUST match the brand name exactly (character by character, no partial or substring matches).
        - For example, if {brandInput} is "Huawei", only match with "Huawei" in the team mapping.
        - DO NOT match "Huawei" with "Huawei_SEC" or any other brand that only contains "Huawei" as part of its name.
    4. ONLY return each identified Team names ONCE in the list

Please be specific when matching Team with Brand such as the following example:
Scenario 1:
The Brand in {brandInput} is Cisco and Cisco_SEC. Based on {team}, Cisco is found in "EN & Collabs" and Cisco_SEC is found in "Security Team".
{team} = {{
    "EN & Collabs": ["Cisco"],
    "Security Team": ["Cisco_SEC"]
}}
Hence, the Team who handles the identified Brands must be EN & Collabs and Security Team. 

Scenario 2:
The Brand in {brandInput} is Huawei. Based on {team}, Huawei is found in "EN & Collabs".
{team} = {{
    "EN & Collabs": ["Huawei"],
    "Security Team": ["Huawei_SEC"]
}}
Even though "Security Team" has brand name containing "Huawei", but you MUST ONLY match with the EXACT Brand name, in which EN & Collabs should be the extracted Team. 


IMPORTANT:
    1. If the information not found or unable to determine a value, set the value to '-'

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT add or remove any keys from the input JSON object:
{{
    "vendor": ["vendor1", "vendor2"],
    "team": ["Team1", "Team2"],
}}

"""

# Email Attachment Analysis Prompt
ANALYZE_ATTACHMENT_EU_INDUSTRY_PROMPT = """
TASK:
1 Analyze the email attachment content in the input JSON Object and determine the following key information:
    1. End User Name (Company or a person that USES the product or service)
    2. Industry (Industry of the End User's company)

2. You are provided:
    a. An Email Attachment Content within the <att_input> tags for analysis

3. Standardize and clean the data:
    - Standardize email content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR IDENTIFYING END USER NAME:
    - DO NOT make your own decision, strictly follow the given rules
    - End user must be the entity that uses the product or service
    - Extract End User ONLY IF mentioned explicitly such as the following examples:
        a. EU: Yokogawa
        b. End User: Yokogawa
        c. End User is Yokogawa
        d. Customer: Yokogawa
        e. Customer is Yokogawa
    - DO NOT return any End User name if it is not explicitly mentioned
    - Return '-' if unable to extract End User.

RULES FOR DETERMINING INDUSTRY:
    - Check for End User company details such as company name or End User name
    - Determine industry according ONLY to the End User company name or End User name
    - DO NOT provide random industry if End User is not identified

INPUT:
<att_input>
{att_input}
</att_input>

IMPORTANT:
    - Always provide full and complete name for End User Name and Industry
    - Always make attempt on identifying and determining the key information
    - If the information not found in the email or unable to determine a value, set the value to '-'

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:

{{
    "endUserName": "ABC Sdn. Bhd." or "ABC (Aaron Bin Corri)",
    "industry": "Information Technology",
}}

"""

# ANALYZE_ATTACHMENT_PRODUCT_BRAND_SUPP_KU_PROMPT = """
# TASK:
# 1 Analyze the email attachment content in the input JSON Object and determine the following key information:
#     a. Product Name: The name of the inquired or requested products (if mentioned)
#     b. Chinese Product Price: List all Product Name and Price of Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.) found in the given products list.
#     c. Chinese Product Support: Extract any mention of warranty or support/maintenance services for Chinese brand products.
#     d. Western Product Price: List all Product Name and Price of Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC etc.) found in the given products list.
#     e. Western Product Support: Extract any mention of warranty or support/maintenance services for Western brand products.
#     f. KU Services: Extract any mention of professional services (such as consulting, configuration, or advisory services), especially those referring to the keyword "advise".
#     e. Brand: The brand associated with the product.
#     g. Analysis Report (Your reason of your output for extraction)

# 2. You are provided:
#     a. You are provided an Email Attachment Content within the <att_input> tags for analysis
#     b. You are provided multiple brand mapping data input within the <mapping_fields> tags to cross-reference the brands and products

# 3. Standardize and clean the data
#     a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
#     b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
#     c. Use fuzzy matching to handle near-identical descriptions or naming variations.

# RULES FOR EXTRACTING PRODUCT NAMES:
#     - Based on the products mentioned in Subject Line or message body.
#     - MUST include all the different products specified, even in the tables.
#     - DO NOT extract products in message footer.
#     - For any mentioning of product swapping, extract ONLY the product that is required after the swap. DO NOT extract the product before swap.
    
# RULES FOR EXTRACTING CHINESE PRODUCT PRICE:
#     - From the "productName" list, select only those products that are Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.) according to the mapping fields.
#     - For each selected product, extract its price and include it in the "ChineseProductPrice" dictionary, where the product name is the key and the price is the value.
#     - Extract the MYR/RM currency total/extended price in the attachment if it is explicitly mentioned
#     - The RM price amount usually look like the format below:
#         - MYR 100, RM 100, 100
#     - RETURN ONLY '-' if there are no prices found for the products within the "productName" list.
#     - DO NOT provide random value, if the price is not explicitly mentioned.

# RULES FOR EXTRACTING WESTERN PRODUCT PRICE:
#     - From the "productName" list, select only those products that are Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC, etc.) according to the mapping fields.
#     - For each selected product, extract its price and include it in the "WesternProductPrice" dictionary, where the product name is the key and the price is the value.
#     - Extract the product USD currency total/extended price in the attachment if it is explicitly mentioned, otherwise convert the price amount to USD according to the USD/MYR rate.
#     - The USD price amount usually look like the format below:
#         - $100, USD 100, 100
#     - RETURN ONLY '-' if there are no prices found for the products within the "productName" list.
#     - DO NOT extract random value, if the price is not explicitly mentioned.

# RULES FOR EXTRACTING CHINESE PRODUCT SUPPORT:
#     - Extract any mention of warranty or support/maintenance services for Chinese brand products only.
#     - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
#         - Example: Required warranty for Huawei switch router

# RULES FOR EXTRACTING WESTERN PRODUCT SUPPORT:
#     - Extract any mention of warranty or support/maintenance services for Western brand products only.
#     - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
#         - Example: Required warranty for Dell Server

# RULES FOR EXTRACTING KU SERVICE:
#     - Return a detailed statement of which product required which professional service like the following example:
#         - Example: Required advisory service for Huawei switch router

# RULES AND INSTRUCTIONS FOR BRAND MAPPING:
#     1. For products that are generic (e.g., “Router”, “Switch”, “Firewall”) follow the example below
#         a. Search other parts of the message thread to find any **mentioned brand** 
#         b. Link the mentioned brand accordingly to the mapping fields <mapping_fields>
#         c. DO NOT make up random brands, STRICTLY follow ONLY the brand mentioned in the message thread
#     2. If the product is very specific (e.g., “Catalyst 2960”), determine the brand based on the mapping fields <mapping_fields>
#     3. Always match the product with the mapping fields <mapping_fields>
#     4. Follow the context below when determinig brands:
#         - The following is the explanation of the Teams:
#             a. Data Center: 
#                 - Handles infrastructure-related topics: server hardware, networking, storage, and data center operations.    
#             b. Software Team: 
#                 - Handles software inquiries — licensing, implementation, support, and infrastructure integration.
#             c. EN & Collabs: 
#                 - Handles network and collaboration technologies — support effective communication and ensure reliable connectivity.
#             d. Security Team: 
#                 - Handles all security products and topics - firewall, cyber threats, devices, security networks, and prevention systems.
#         - Ensure the assigned Team aligns with the Brand like the example below:
#             - Data Center: {dataCenterBrandMapping}
#             - Software Team: {softwareBrandMapping}
#             - EN & Collabs: {enCollabsBrandMapping}
#             - Security Team: {securityBrandMapping} and {ciscoSecurePortfolioBrandMapping} 
#     5. ONLY return each identified brand names ONCE in the list
#     6. DO NOT make up random brands, STRICTLY follow the brands and products in <mapping_fields>

# Determine the brand according the following example:
# The Product Name identified from <att_input> is Cisco_Webex, and it was found in the items or product lines of {enCollabsBrandMapping} where the mapping field looks like below example:
# {enCollabsBrandMapping} = {{
#     "Cisco_Collab": ["PABX", "Webex", "A-Flex", "VideoConference"],
# }}
# Hence, the Brand mapped to Cisco_Webex is Cisco_Collab. 

# Please be specific when matching the products to Brand, follow the example below:
# When Product identified is MX100-HW is a Cisco brand, you may find it in both {enCollabsBrandMapping} and {securityBrandMapping} where the mapping fields looks like below example:
# {enCollabsBrandMapping} = {{
#     "Cisco": ["Cisco_Router & Switch & Wireless", "Catalyst", "Meraki"]
# }}
# {securityBrandMapping} = {{
#     "Cisco_S&R": ["Catalyst C9K", "Catalyst 8K", "Meraki MS", "Meraki MV", "Meraki MX", "Meraki MDM", "IE Series", "IR series"],
# }}
# The Product "ACX7509" clearly indicate an ACX series, which can be found in the items or product lines of {securityBrandMapping} 
# Hence, the Brand should be "Cisco_S&R", not "Cisco".

# INPUT:
# <att_input>
# {att_input}
# </att_input>

# <mapping_fields>
# {dataCenterBrandMapping}
# {softwareBrandMapping}
# {enCollabsBrandMapping}
# {securityBrandMapping}
# {ciscoSecurePortfolioBrandMapping}
# </mapping_fields>

# IMPORTANT:
#     - Return a list if the information is found in the email or a value is determined
#     - DO NOT return list or array if the information not found in the email or unable to determine a value, set the value to '-' instead

# OUTPUT FORMAT:
# Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
# DO NOT remove any keys from the input JSON object:
# {{
#     "brand": ["Brand1", "Brand2"] or '-',
#     "productName": ["Product1", "Product2"] or '-',
#     "ChineseProductPrice": {{
#         "ProductA": "123.00",
#         "ProductB": "456.00"
#     }} or '-',
#     "chineseProductSupport": ["Huawei 3-year warranty"] or '-',
#     "WesternProductPrice": {{
#         "ProductC": "123.00",
#         "ProductD": "456.00"
#     }} or '-',
#     "westernProductSupport": ["IBM maintenance"] or '-',
#     "kuServices": ["Professional advisory service"] or '-'
#     "analysisReport": "I extracted this because...."
# }}

# """

# SPLIT PROMPT FOR PRODUCT, SUPPORT, AND BRAND ANALYSIS
ANALYZE_ATTACHMENT_PRODUCT_BRAND_PROMPT = """
TASK:
1 Analyze the email attachment in the input JSON Object and determine the following key information:
    a. Specific Products: The specific model/name of the inquired or requested products (e.g, Huawei Atlas X300, APC-5000VA)
    b. Brand: The brand associated with the product.
    c. General Products: The products shown in general terms (e.g, Server, Switch, Storage, Infrastructure), which is not a model name.
    d. Analysis Report (Your reason of your output for extraction)

2. You are provided:
    a. You are provided an Email Attachment Content within the <att_input> tags for analysis
    b. You are provided multiple brand mapping data input within the <mapping_fields> tags to cross-reference the brands and products

3. Standardize and clean the data
    a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

INSTRUCTIONS:
    1. Extract all the products found in <att_input>
    2. Analyze the extracted products to find the generic product names (e.g, Generic: Smart Server, Specific: Huawei Atlas 800)
    3. Separate the generic and specific product names into different list specificProduct" and "generalProduct"
    4. ONLY determine the brand of products in "specificProduct" list with <mapping_fields>

RULES FOR EXTRACTING SPECIFIC PRODUCTS:
    - DO NOT extract if the brand cannot be directly determined or the product is not a model,
    - Based on the products mentioned in Subject Line or message body.
    - MUST include all the different products specified, even in the tables.
    - The products extracted MUST be specified with a model or brand, such as below example:  
        - HPE DL360 Gen10
        - Cisco Nexus 93180YC-EX
    - DO NOT extract products in message footer.
    - For any mentioning of product swapping, extract ONLY the product that is required after the swap. DO NOT extract the product before swap.
    
RULES FOR EXTRACTING GENERAL PRODUCTS:
    - Analyze the products extracted
    - Extract general IT product objects ONLY if the brand cannot be directly determined with <mapping_fields>, such as:
        - Server
        - Firewall
        - Storage
        - Other generic IT hardware components
    - MUST include all the different products specified, even in the tables.
    - ONLY extract if the word appears as a standalone product name or with minimal descriptive modifiers (e.g., "Switch" or "Smart Switch").    
    - DO NOT extract terms like "Switch" when they are part of a full product model name or specification (e.g., "Cisco Switch 4000").        
    - DO NOT extract products in message footer.
    - RETURN '-' if there is no generic product.

Extract the products according the following example:
The attachment content consist a sentence that shows the products: 
    "We are planning to deploy new firewalls along with the Palo Alto PA-850 in our data center. Cisco router needed for branch office setup"
Hence, you should extract the products as below:
    Generic product: Firewall, Cisco router
    Specific product: Palo Alto PA-850

RULES AND INSTRUCTIONS FOR BRAND MAPPING:
    1. ONLY map for the products in PRODUCT NAMES list, DO NOT use GENERAL PRODUCTS.
    2. If the product is very specific (e.g., “Catalyst 2960”), determine the brand based on the mapping fields <mapping_fields>
    3. Always match the product with the mapping fields <mapping_fields>
    4. Follow the context below when determinig brands:
        - The following is the explanation of the Teams:
            a. Data Center: 
                - Handles infrastructure-related topics: server hardware, networking, storage, and data center operations.    
            b. Software Team: 
                - Handles software inquiries — licensing, implementation, support, and infrastructure integration.
            c. EN & Collabs: 
                - Handles network and collaboration technologies — support effective communication and ensure reliable connectivity.
            d. Security Team: 
                - Handles all security products and topics - firewall, cyber threats, devices, security networks, and prevention systems.
        - Ensure the assigned Team aligns with the Brand like the example below:
            - Data Center: {dataCenterBrandMapping}
            - Software Team: {softwareBrandMapping}
            - EN & Collabs: {enCollabsBrandMapping}
            - Security Team: {securityBrandMapping} and {ciscoSecurePortfolioBrandMapping} 
    5. ONLY return each identified brand names ONCE in the list
    6. DO NOT make up random brands, STRICTLY follow the brands and products in <mapping_fields>

Determine the brand according the following example:
The Product Name identified from <att_input> is Cisco_Webex, and it was found in the items or product lines of {enCollabsBrandMapping} where the mapping field looks like below example:
{enCollabsBrandMapping} = {{
    "Cisco_Collab": ["PABX", "Webex", "A-Flex", "VideoConference"],
}}
Hence, the Brand mapped to Cisco_Webex is Cisco_Collab. 

Please be specific when matching the products to Brand, follow the example below:
When Product identified is MX100-HW is a Cisco brand, you may find it in both {enCollabsBrandMapping} and {securityBrandMapping} where the mapping fields looks like below example:
{enCollabsBrandMapping} = {{
    "Cisco": ["Cisco_Router & Switch & Wireless", "Catalyst", "Meraki"]
}}
{securityBrandMapping} = {{
    "Cisco_S&R": ["Catalyst C9K", "Catalyst 8K", "Meraki MS", "Meraki MV", "Meraki MX", "Meraki MDM", "IE Series", "IR series"],
}}
The Product "ACX7509" clearly indicate an ACX series, which can be found in the items or product lines of {securityBrandMapping} 
Hence, the Brand should be "Cisco_S&R", not "Cisco".

INPUT:
<att_input>
{att_input}
</att_input>

<mapping_fields>
{dataCenterBrandMapping}
{softwareBrandMapping}
{enCollabsBrandMapping}
{securityBrandMapping}
{ciscoSecurePortfolioBrandMapping}
</mapping_fields>

IMPORTANT:
    - Return a list if the information is found in the email or a value is determined
    - DO NOT return list or array if the information not found in the email or unable to determine a value, set the value to '-' instead

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "brand": ["Brand1", "Brand2"] or '-',
    "specificProduct": ["Product1", "Product2"] or '-',
    "generalProduct": ["Product1", "Product2"] or '-',
    "analysisReport": "I extracted this because...."
}}

"""

ANALYZE_ATTACHMENT_PRODUCT_SUPP_KU_PROMPT = """
TASK:
1 Analyze the email attachment in the input JSON Object and determine the following key information:
    a. Chinese Product Price: List of Chinese brand products (e.g., Huawei, Lenovo, xFusion, etc.) with price mentioned.
    b. Chinese Product Support: Extract any mention of warranty or support/maintenance services for Chinese brand products.
    c. Western Product Price: List of Western brand products (e.g., IBM, Dell, HPE, Cisco, Juniper, APC etc.) with price mentioned.
    d. Western Product Support: Extract any mention of warranty or support/maintenance services for Western brand products.
    e. KU Services: Extract any mention of professional services (such as consulting, configuration, or advisory services), especially those referring to the keyword "advise".
    f. Analysis Report (Your reason of your output for extraction)

2. You are provided:
    a. You are provided an Email Attachment Content within the <att_input> tags for analysis

3. Standardize and clean the data
    a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR EXTRACTING CHINESE PRODUCT PRICE:
    - From <att_input>, select only those products that are Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.).
    - For each selected product, extract its price and include it in the "ChineseProductPrice" dictionary, where the product name is the key and the price is the value.
    - Extract the MYR/RM currency price in the email attachment text ONLY IF it is explicitly mentioned
    - The RM price amount usually look like the format below:
        - MYR 100, RM 100, 100
    - If there are no prices found for the products in the <att_input>, DO NOT RETURN any value:
        - Example: You found a product 'IBM Server', but no specific prices are mentioned anywhere. Hence, DO NOT RETURN the product or any value.
    - DO NOT provide random value, if the price is not explicitly mentioned.
    - RETURN a string '-' if the list is empty:
        - Example: 'chineseProductPrice': '-'

RULES FOR EXTRACTING WESTERN PRODUCT PRICE:
    - From the <att_input>, select only those products that are Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC, etc.).
    - For each selected product, extract the price and include it in the "WesternProductPrice" dictionary, where the product name is the key and the price is the value.
    - Extract the product USD currency price in the email attachment text ONLY IF it is explicitly mentioned, otherwise convert the price amount to USD according to the USD/MYR rate.
    - The USD price amount usually look like the format below:
        - $100, USD 100, 100
    - If there are no prices found for the products in the <att_input>, DO NOT RETURN any value:
        - Example: You found a product 'IBM Server', but no specific prices are mentioned anywhere. Hence, DO NOT RETURN the product or any value.    - DO NOT extract random value, if the price is not explicitly mentioned.
    - RETURN a string '-' if the list is empty:
        - Example: 'westernProductPrice': '-'

RULES FOR EXTRACTING CHINESE PRODUCT SUPPORT:
    - Extract any mention of warranty or support/maintenance services for Chinese brand products only.
    - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
        - Example: Required warranty for Huawei switch router

RULES FOR EXTRACTING WESTERN PRODUCT SUPPORT:
    - Extract any mention of warranty or support/maintenance services for Western brand products only.
    - Return a detailed statement of which product required which warranty or support/maintenance services like the following example:
        - Example: Required warranty for Dell Server

RULES FOR EXTRACTING KU SERVICE:
    - Return a detailed statement of which product required which professional service like the following example:
        - Example: Required advisory service for Huawei switch router

INPUT:
<att_input>
{att_input}
</att_input>

IMPORTANT:
    - Return a list if the information is found in the email or a value is determined
    - DO NOT return list or array if the information not found in the email or unable to determine a value, set the value to '-' instead

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "ChineseProductPrice": {{
        "ProductA": "123.00",
        "ProductB": "456.00"
    }} or '-',
    "chineseProductSupport": ["Huawei 3-year warranty"] or '-',
    "WesternProductPrice": {{
        "ProductC": "123.00",
        "ProductD": "456.00"
    }} or '-',
    "westernProductSupport": ["IBM maintenance"] or '-',
    "kuServices": ["Professional advisory service"] or '-',
    "analysisReport": "I extracted this because...."
}}

"""

ANALYZE_ATTACHMENT_SUBJECT_TENDER_PROMPT = """
TASK:
1 Analyze the email attachment content in the input JSON Object and determine the following key information:
    1. Tender (True or false if the email is a tender process)

2. You are provided:
    a. An Email Attachment Content within the <att_input> tags for analysis

3. Standardize and clean the data:
    - Standardize attachment content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR DETERMINING TENDER:
    - An attachment content should be classified as a tender request if the content includes any of the following keywords or phrases with relevant context:
        - "RFQ" or "Request for Quotation"
        - "RFP" or "Request for Proposal"
        - "Tender"
        - "Tender code" or "Tender number"
        - "Closing date"
    - An attachment content should NOT be classified as a tender request if it only contains a general term like "quote", "propose", or "quotation" without any additional context suggesting a formal tender or bidding process.

INPUT:
<att_input>
{att_input}
</att_input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "isTender": True or False,
}}

"""

ANALYZE_GENERIC_PROMPT = """
TASK:
1 Analyze the generic product list in the input JSON Object and determine the following key information:
    1. Brand: The brand associated with the product. 
    2. Vendor: The company that owns the brand.
    3. Team: The team associated with the brand.
    4. Analysis Report (Your reason of your output for extraction)

2. You are provided:
    a. List with generic products within the <att_input> tags for analysis, such as:
        - Network
        - Switch
        - Server

3. Standardize and clean the data:
    - Standardize attachment content: remove extra spaces, normalize casing and formatting.
    - For missing values, use reasonable defaults or indicate the missing data in your analysis.
    - Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR DETERMINING BRAND:
    1. Always match the product with the mapping fields <mapping_fields>
    2. Follow the context below when determining brands:
        - Ensure the assigned Team aligns with the Brand like the example below:
            - Data Center: {dataCenterBrandMapping}
            - Software Team: {softwareBrandMapping}
            - EN & Collabs: {enCollabsBrandMapping}
            - Security Team: {securityBrandMapping}
    3. ONLY return each identified brand names ONCE in the list
    4. DO NOT make up random brands, STRICTLY follow the brands in <mapping_fields>
 
RULES TO DETERMINE VENDOR:
    1. Determine the vendor ONLY based on the extracted Brand
    2. ONLY return each identified vendor names ONCE in the list
    3. The following is the example for brand and vendor relationship:
        - The identified brand is Cisco, then the vendor should be Cisco

RULES TO DETERMINE THE TEAM WHO HANDLES THE BRAND:
    1. Determine the team who are responsible for handling all Brands, DO NOT miss out any Brand.
    2. Strictly follow the context below when determining teams:
        - Ensure the assigned Team aligns with the Brand like the example below:
            - Data Center: {dataCenterBrandMapping}
            - Software Team: {softwareBrandMapping}
            - EN & Collabs: {enCollabsBrandMapping}
            - Security Team: {securityBrandMapping}
    3. ONLY return each identified Team names ONCE in the list

Please be specific when matching Team with Brand such as the following example:
Scenario 1:
The product is Juniper Switch. Based on brand mapping data in <mapping_fields>, Switch can be found in Juniper and its from "EN & Collabs".
{enCollabsBrandMapping} = {{
    "Juniper": ["Switch"]
}}
Hence, the Team who handles the identified Brands must be EN & Collabs. 

INPUT:
<att_input>
{att_input}
</att_input>

<mapping_fields>
{dataCenterBrandMapping}
{softwareBrandMapping}
{enCollabsBrandMapping}
{securityBrandMapping}
</mapping_fields>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "brand": ["Brand1", "Brand2"] or '-',
    "vendor": ["vendor1", "vendor2"],
    "team": ["Team1", "Team2"],
    "analysisReport": "I extracted this because...."
}}

"""

# Analysis Result Consolidation Prompt
CONSOLIDATION_PROMPT = """
TASK:
1 Consolidate and clean the analysis content in the input JSON Object and determine the following key information:
    a. Product Name: The unique combination of Generic and Specific products.
    b. Chinese Product Price: List all Product Name and Price of Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.) found in the given products list.
    c. Chinese Product Support: Extract any mention of warranty or support/maintenance services for Chinese brand products.
    d. Western Product Price: List all Product Name and Price of Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC etc.) found in the given products list.
    e. Western Product Support: Extract any mention of warranty or support/maintenance services for Western brand products.
    f. KU Services: Extract any mention of professional services (such as consulting, configuration, or advisory services), especially those referring to the keyword "advise".
    e. Brand: The brand associated with the product.
    h. End User Name (Company or a person that USES the product or service)
    i. Industry (Industry of the End User's company)
    j. Reseller (Company that requests or inquires for product or service) 
    k. Redirector Address (The email address of the VSTEC entity who redirects the external request to VSTEC internal team)
    l. Subject (The subject of the message)
    m. Tender (True or false if the email is a tender process)
    n. Email Sent Date (The date of the message started)

2. You are provided:
    a. You are provided an all Analysis Result Content within the <email_body_analysis_input> and <email_attachment_analysis_input> tags for analysis
    b. The data within <email_attachment_analysis_input> are email attachment data, while data in <email_body_analysis_input> are email body data.

3. Standardize and clean the data
    a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

INSTRUCTIONS:
    1. Combine all the data in <email_body_analysis_input> and <email_attachment_analysis_input> based on the data respective categories.
    2. DO NOT miss out any values from data in <email_body_analysis_input> and <email_attachment_analysis_input> tag.
    3. RETURN a distinct value, DO NOT RETURN duplicated values.
    4. For Tender, as long as there is True in tender, then overwrite the Tender as True.

INSTRUCTIONS:
1. Compare both lists and identify brand names, general and specific product names, end user name, and industry that appear in both lists.
2. If the value appears in both <email_body_analysis_input> and <email_attachment_analysis_input>, DO NOT return it more than once.
3. Your final result must include ONLY UNIQUE values from <email_body_analysis_input>, even if some of them also exist in <email_attachment_analysis_input>.

EXAMPLE:
If:
- {attachmentProductBrands} = ["Cisco", "Cisco_S&R"]
- {productBrands} = ["Cisco"]

Then return:
["Cisco", "Cisco_S&R"]

INPUT:
<email_body_analysis_input>
{productBrands}
{endUser}
{reseller}
{subjectTender}
</email_body_analysis_input>

<email_attachment_analysis_input>
{attachmentProductBrands}
{attachmentTender}
{attachmentEndUser}
</email_attachment_analysis_input>

IMPORTANT:
    - Return a list if the information is found in the email or a value is determined
    - DO NOT return list or array if the information not found in the email or unable to determine a value, set the value to '-' instead

OUTPUT FORMAT:
Return ONLY a valid JSON object without any markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
DO NOT remove any keys from the input JSON object:
{{
    "brand": ["Brand1", "Brand2"] or '-',
    "productName": ["Product1", "Product2"] or '-',
    "ChineseProductPrice": {{
        "ProductA": "123.00",
        "ProductB": "456.00"
    }} or '-',
    "chineseProductSupport": ["Huawei 3-year warranty"] or '-',
    "WesternProductPrice": {{
        "ProductC": "123.00",
        "ProductD": "456.00"
    }} or '-',
    "westernProductSupport": ["IBM maintenance"] or '-',
    "kuServices": ["Professional advisory service"] or '-',
    "endUserName": "ABC Sdn. Bhd." or "ABC (Aaron Bin Corri)",
    "industry": "Information Technology",
    "vendor": ["vendor1", "vendor2"] or '-',
    "team": ["Team1", "Team2"]  or '-',
    "reseller": "XYZ Sdn. Bhd." or "XYZ (Xi Yang Zi)",
    "redirectorAddress": "abc@email.com",
    "subject": "Request - Alto Product",
    "emailSentDate": "DD-MM-YYYYTHH:MM:SS.microsecondsZ",
    "isTender": True or False
}}

"""