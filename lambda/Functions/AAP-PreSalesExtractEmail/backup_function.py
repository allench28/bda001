import boto3
import time
import uuid
import os
import json
import email
import base64
import requests
import pandas as pd
from requests_aws4auth import AWS4Auth
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import ResourceNotFoundException, BadRequestException
from urllib.parse import unquote_plus
from urllib.parse import unquote
from typing import List
from typing import Dict
from bedrock_function import promptBedrock
import re
import extract_msg

S3_BUCKET_NAME = os.environ.get('S3_BUCKET')
EXTRACTED_EMAIL_TABLE = os.environ.get('EXTRACTED_EMAIL_TABLE')
ROUTED_CONTENT_TABLE = os.environ.get('ROUTED_CONTENT_TABLE')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
SKILL_MATRIX_TABLE = os.environ.get('SKILL_MATRIX_TABLE')
MODEL_ID = os.environ.get('MODEL_ID')
MERCHANT_ID = os.environ.get('MERCHANT_ID')

SKILL_MATRIX_FILEKEY = f'presales/input/{MERCHANT_ID}/skill-matrix/SkillMatrix.csv'

SQS_CLIENT = boto3.client('sqs')
S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource("dynamodb")

EXTRACTED_EMAIL_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_EMAIL_TABLE)
ROUTED_CONTENT_DDB_TABLE = DDB_RESOURCE.Table(ROUTED_CONTENT_TABLE)
SKILL_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(SKILL_MATRIX_TABLE)

logger = Logger()
tracer = Tracer()

TEAM_BRAND_MAPPING = {
    "Data Center": ["Cisco_DC", "Huawei_DP", "Huawei_DC", "IBM_HW", "Legrand", "Dell", "HPE", "Lenovo", "xFusion", "SuperMicro", "APC", "Nutanix", "Nvidia Network", "Netapp"],
    "Software Team": ["SUSE", "Rubrik", "VMware", "Veeam", "Red Hat", "Microsoft", "Commvault", "IBM instana", "IBM Watsonx", "IBM", "Omnissa"],
    "EN & Collabs": ["Cisco", "Cisco_Collab", "Huawei", "Huawei_Collab", "Juniper", "Velocloud", "Raisecom_Switch", "Zoom", "Zebra"],
    "Security Team": ["Cisco_SEC", "Cisco_S&R", "Juniper_SEC", "Juniper_S&R", "Juniper_DC", "Huawei_SEC", "Huawei_S&R", "F5", "Palo Alto Network", "TrendMicro", "Splunk", "Microsoft_SEC", "Broadcom_SEC", "iBoss", "Platform and suites", "Network Security", "Device Security", "User Security", "Cloud Security", "Application Security", "Analytics", "Industrial Security", "Security Solutions", "Data Center Security", "Secure access service edge (SASE)", "Security service edge (SSE)"]
}

DATA_CENTER_BRAND_MAPPING = {
    "Cisco_DC": ["UCS", "Nexus", "MDS", "InterSight"],
    "Huawei_DP": ["FusionModule", "FusionPower", "FusionCol", "NetECO"],
    "Huawei_DC": ["Oceanstor", "DCS"],
    "IBM_HW": ["Power", "LinuxOne", "Flashsystem"],
    "Legrand": ["Legrand"],
    "Dell": ["DELL"],
    "HPE": ["HPE"],
    "Lenovo": ["Lenovo"],
    "xFusion": ["xFusion"],
    "SuperMicro": ["SuperMicro"],
    "APC": ["APC"],
    "Nutanix": ["Nutanix"],
    "Nvidia Network": ["Nvidia Network"],
    "Netapp": ["NetApp"]
}

SOFTWARE_BRAND_MAPPING = {
    "SUSE": ["SUSE"],
    "Rubrik": ["Rubrik"],
    "VMware": ["VMware"],
    "Veeam": ["Veeam"],
    "Red Hat": ["Red Hat"],
    "Microsoft": ["Microsoft"],
    "Commvault": ["Commvault"],
    "IBM instana": ["IBM instana"],
    "IBM Watsonx": ["IBM Watsonx"],
    "IBM": ["IBM"],
    "Omnissa": ["Omnissa"]
}

EN_N_COLLABS_BRAND_MAPPING = {
    "Cisco": ["Cisco_Router & Switch & Wireless", "Catalyst", "Meraki"],
    "Cisco_Collab": ["PABX", "Webex", "A-Flex", "VideoConference"],
    "Huawei": ["Huawei_Router & Switch & Wireless", "CloudEngine", "AirEngine"],
    "Huawei_Collab": ["Ideahub"],
    "Juniper": ["Juniper_Router & CampusSwitch & DataCenterSwitch & Wireless"],
    "Velocloud": ["VMware_SD-WAN"],
    "Raisecom_Switch": ["Raisecom_Switch"],
    "Zoom": ["Zoom"],
    "Zebra": ["Handheld", "Printer"],
}

SECURITY_BRAND_MAPPING = {
    "Cisco_SEC": ["Firepower", "ASA", "FMC", "Cisco ISE", "Secure Portfolio", "Meraki MX", "ISA"],
    "Cisco_S&R": ["Catalyst C9K", "Catalyst 8K", "Meraki MS", "Meraki MV", "Meraki MX", "Meraki MDM", "IE Series", "IR series"],
    "Juniper_SEC": ["SRX series", "Security Director", "Secure Connect", "Secure Edge Access"],
    "Juniper_S&R": ["EX series", "QFX series", "SRX series", "MX series", "ACX Series", "SSR series", "Mist", "Junos Space"],
    "Juniper_DC": ["QFX series", "PTX series", "Apstra"],
    "Huawei_SEC": ["HiSecEngine/USG series", "SecoManager", "AntiDDOS series"],
    "Huawei_S&R": ["CloudEngine/S series", "AR series", "NetEngine"],
    "F5": ["F5"],
    "Palo Alto Network": ["Palo Alto Network"],
    "TrendMicro": ["TrendMicro"],
    "Splunk": ["Splunk"],
    "Microsoft_SEC": ["Microsoft Defender"],
    "Broadcom_SEC": ["AVI network (NSX)"],
    "iBoss": ["iBoss"]
}

CISCO_SECURE_PORTFOLIO_BRAND_MAPPING = {
    "Platform and suites": [
        "Cisco Security Cloud", "Cisco breach Protection", "Cisco Cloud Protection", "Cisco User Protection"
    ],
    "Network Security": [
        "Cisco Firewall (Firepower & ASA)", "Cisco Security Cloud Control", "Cisco Identity Services Engine (ISE)",
        "Cisco Multicloud Defense", "Cisco XDR"
    ],
    "Device Security": [
        "Cisco Secure Client (Anyconnect)", "Cisco Secure Endpoint", "Cisco Security Connector",
        "Cisco Meraki Systems Managers (SM)"
    ],
    "User Security": [
        "Cisco DUO", "Cisco Secure Email Threat Defense", "Cisco Secure Access", "Cisco Secure Web Appliance"
    ],
    "Cloud Security": [
        "Cisco AI Defense", "Cisco Attack Surface Management", "Cisco Umbrella"
    ],
    "Application Security": [
        "Cisco Hypershield", "Cisco Secure Workload", "Cisco Web Application & API Protection (WAAP)"
    ],
    "Analytics": [
        "Cisco Secure Malware Analytics", "Cisco Secure Network Analytics", "Cisco Security Analytics and Logging",
        "Cisco Telemetry Broker"
    ],
    "Industrial Security": [
        "Cisco Industrial Threat Defense", "Cisco Cyber Vision", "Cisco Secure Equipment Access"
    ],
    "Security Solutions": [
        "Cisco Identity Intelligence", "Cisco Secure Hybrid Work"
    ],
    "Data Center Security": ["Industrial cybersecurity"],
    "Secure access service edge (SASE)": ["Security AI"],
    "Security service edge (SSE)": ["Zero Trust Access", "Zero trust"]
}

SUBJECT_TENDER_PROMPT = """
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

DETERMINE_PRODUCT_AND_BRAND_PROMPT = """
TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    a. Product Name: The name of the inquired or requested products (if mentioned)
    b. Chinese Products: List all Product Name of Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.) found in the given products list.
    c. Chinese Product Support: Extract any mention of warranty or support/maintenance services for Chinese brand products.
    d. Western Products: List all Product Name of Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC etc.) found in the given products list.
    e. Western Product Support: Extract any mention of warranty or support/maintenance services for Western brand products.
    f. KU Services: Extract any mention of professional services (such as consulting, configuration, or advisory services), especially those referring to the keyword "advise".
    e. Brand: The brand associated with the product.

2. You are provided:
    a. You are provided an Email Content within the <email_input> tags for analysis
    b. You are provided multiple brand mapping data input within the <mapping_fields> tags to cross-reference the brands and products

3. Standardize and clean the data
    a. Standardize email, product, and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES FOR EXTRACTING PRODUCT NAMES:
    - Based on the products mentioned in Subject Line or message body.
    - MUST include all the different products specified, even in the tables.
    - DO NOT extract products in message footer.
    - For any mentioning of product swapping, extract ONLY the product that is required after the swap. DO NOT extract the product before swap.
    
RULES FOR EXTRACTING CHINESE PRODUCTS:
    - From the "productName" list, select only those products that are Chinese brands (e.g., Huawei, Lenovo, xFusion, etc.) according to the mapping fields.
    - DO NOT provide any product names that are not found within the "productName" list.

RULES FOR EXTRACTING WESTERN PRODUCTS:
    - From the "productName" list, select only those products that are Western brands (e.g., IBM, Dell, HPE, Cisco, Juniper, APC etc.) according to the mapping fields.
    - DO NOT provide any product names that are not found within the "productName" list.

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

RULES AND INSTRUCTIONS FOR BRAND MAPPING:
    1. For products that are generic (e.g., “Router”, “Switch”, “Firewall”) follow the example below
        a. Search other parts of the message thread to find any **mentioned brand** 
        b. Link the mentioned brand accordingly to the mapping fields <mapping_fields>
        c. DO NOT make up random brands, STRICTLY follow ONLY the brand mentioned in the message thread
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
    "productName": ["Product1", "Product2"] or '-',
    "chineseProducts": ["Huawei OceanStor 5300"] or '-',
    "chineseProductSupport": ["Huawei 3-year warranty"] or '-',
    "westernProducts": ["IBM Power10"] or '-',
    "westernProductSupport": ["IBM maintenance"] or '-',
    "kuServices": ["Professional advisory service"] or '-'
}}

"""

ANALYZE_RESELLER_PROMPT = """
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

ANALYZE_EU_PROMPT = """
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

DETERMINE_TEAM_VENDOR_PROMPT = """
INPUT:
<email_input>
{brandInput}
</email_input>

<mapping_fields>
{team}
</mapping_fields>

TASK:
1 Analyze the email content in the input JSON Object and determine the following key information:
    a. Vendor: The company that owns the brand.
    b. Team: The team associated with the brand.

2. You are provided:
    a. You are provided a list of brands in {brandInput} for analysis
    b. You are provided the Teams that are responsible for handling different brands 
    c. You are provided multiple brand mapping data input within the <mapping_fields> tags to cross-reference the brands, and products

3. Standardize and clean the data
    a. Standardize product and brand fields: remove extra spaces, normalize casing and formatting.
    b. For missing values, use reasonable defaults or indicate the missing data in your analysis.
    c. Use fuzzy matching to handle near-identical descriptions or naming variations.

RULES TO DETERMINE VENDOR:
    1. Determine the vendor ONLY based on the given Brand in {brandInput} tag
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


@tracer.capture_method
def create_analyze_team_vendor_prompt(email_input, team):

    prompt = DETERMINE_TEAM_VENDOR_PROMPT.format(
        team=json.dumps(team, default=str),
        brandInput=json.dumps(email_input['brand'], default=str),
    )
    result, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result = clean_analysisResult(result)
    return cleaned_result


@tracer.capture_method
def create_determine_product_and_brand_prompt(email_input, data_center, software, enCollabs, security, cisco):

    prompt = DETERMINE_PRODUCT_AND_BRAND_PROMPT.format(
        dataCenterBrandMapping=json.dumps(data_center, default=str),
        softwareBrandMapping=json.dumps(software, default=str),
        enCollabsBrandMapping=json.dumps(enCollabs, default=str),
        securityBrandMapping=json.dumps(security, default=str),
        ciscoSecurePortfolioBrandMapping=json.dumps(cisco, default=str),
        email_input=json.dumps(email_input, default=str)
    )
    result, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result = clean_analysisResult(result)
    return cleaned_result


@tracer.capture_method
def create_analyze_entity_prompt(email_input):

    prompt = ANALYZE_EU_PROMPT.format(
        email_input=json.dumps(email_input, default=str)
    )
    result1, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result1 = clean_analysisResult(result1)

    prompt = ANALYZE_RESELLER_PROMPT.format(
        email_input=json.dumps(email_input, default=str)
    )
    result2, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result2 = clean_analysisResult(result2)

    return cleaned_result1, cleaned_result2


@tracer.capture_method
def create_analyze_subject_tender_prompt(email_input, subject, emailSentDate):

    prompt = SUBJECT_TENDER_PROMPT.format(
        email_input=json.dumps(email_input, default=str),
        subject=json.dumps(subject, default=str),
        emailSentDate=json.dumps(emailSentDate, default=str)
    )
    result, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result = clean_analysisResult(result)

    return cleaned_result

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # skillMatrixData = skillMatrixMapping(SKILL_MATRIX_FILEKEY)
        # storeSkillMatrixInDB(skillMatrixData)
        extractedEmailIds = []
        logger.info(event)
        for record in event["Records"]:
            filepath = record["s3"]["object"]["key"]
            filepathPrefix = filepath.split("/")[3]
            cleanFilePath = unquote_plus(filepath)
            sourceFile = cleanFilePath.split("/")[4]

            if filepathPrefix == "email":
                senderEmail, recipientEmail, ccList, subject, emailBody, emailSentDate = extractInfoFromEmail(
                    S3_BUCKET_NAME, cleanFilePath)

                endUser, reseller = create_analyze_entity_prompt(emailBody)
                
                productBrands = create_determine_product_and_brand_prompt(
                    emailBody, DATA_CENTER_BRAND_MAPPING, SOFTWARE_BRAND_MAPPING, EN_N_COLLABS_BRAND_MAPPING, SECURITY_BRAND_MAPPING, CISCO_SECURE_PORTFOLIO_BRAND_MAPPING)

                teamVendor = create_analyze_team_vendor_prompt(
                    productBrands, TEAM_BRAND_MAPPING)

                subjectTender = create_analyze_subject_tender_prompt(
                    emailBody, subject, emailSentDate)
                logger.info(endUser)
                logger.info(reseller)
                logger.info(productBrands)
                logger.info(teamVendor)
                logger.info(subjectTender)
                # Map extracted data to ExtractedEmail table
                extractedEmailId = str(uuid.uuid4())
                now = datetime.now().strftime('%d-%m-%YT%H:%M:%S.%fZ')
                extractedEmailIds.append(extractedEmailId)
                # Map extracted data to ExtractedEmail table
                extracted_email_item = {
                    'extractedEmailId': extractedEmailId,
                    'senderEmailAddress': reseller.get('requesterAddress', '-'),
                    'subject': subjectTender.get('subject', '-'),
                    'emailSentDate': subjectTender.get('emailSentDate', '-'),
                    'product': productBrands.get('productName', '-'),
                    'productMYR': productBrands.get('chineseProducts', '-'),
                    'suppMYR': productBrands.get('chineseProductSupport', '-'),
                    'productUSD': productBrands.get('westernProducts', '-'),
                    'suppUSD': productBrands.get('westernProductSupport', '-'),
                    'kuServicesMYR': productBrands.get('kuServices', '-'),
                    'brand': productBrands.get('brand', '-'),
                    'vendor': teamVendor.get('vendor', '-'),
                    'reseller': reseller.get('reseller', '-'),
                    'endUserName': endUser.get('endUserName', '-'),
                    'industry': endUser.get('industry', '-'),
                    'team': teamVendor.get('team', '-'),
                    "tender": subjectTender.get('isTender', False),
                    "sourceFile": sourceFile,
                    "merchantId": MERCHANT_ID,
                    'createdAt': now,
                    'createdBy': "System",
                    'updatedAt': now,
                    'updatedBy': "System",
                }
                EXTRACTED_EMAIL_DDB_TABLE.put_item(Item=extracted_email_item)

            archiveFile(cleanFilePath)

        payload = {
            'extractedEmailIds': extractedEmailIds,
            'merchantId': MERCHANT_ID,
            'subject': subject
        }
        response = sendToSQS(payload)

    except (ResourceNotFoundException, BadRequestException) as ex:
        if str(ex) == 'Email is not registered as merchant!':
            pass
            # continue
            # return sendErrorMail(senderEmail)
            return {'status': True, 'message': 'Send error email success'}
        else:
            return {
                'status': False,
                'message': str(ex)
            }
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}


@tracer.capture_method
def extractInfoFromEmail(S3_BUCKET_NAME, fileKey):
    response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey)
    emailRawBytes = response['Body'].read()

    with open('/tmp/temp_email.msg', 'wb') as temp_file:
        temp_file.write(emailRawBytes)

    msg = extract_msg.Message('/tmp/temp_email.msg')
    msg_message = msg.body

    senderEmail = re.search(
        r'<(.*?)>', msg.sender).group(1) if '<' in msg.sender else msg.sender
    recipientEmail = re.search(
        r'<(.*?)>', msg.to).group(1) if '<' in msg.to else msg.to
    ccList = [re.search(r'<(.*?)>', cc).group(1)
              if '<' in cc else cc for cc in re.split(r';|,', msg.cc)] if msg.cc else []
    subject = msg.subject
    emailBody = msg_message
    emailSentDate = msg.date

    return senderEmail, recipientEmail, ccList, subject, emailBody, emailSentDate


@tracer.capture_method
def skillMatrixMapping(fileKey) -> List[Dict]:
    response = S3_CLIENT.get_object(Bucket=S3_BUCKET_NAME, Key=fileKey)
    csvContent = pd.read_csv(
        response['Body'], dtype=str, encoding='utf-8').to_dict('records')

    skillMatrixContent = []

    # Field mapping for the CSV columns
    field_mapping = {
        'Name': 'name',
        'Role': 'role',
        'Email': 'email',
        'Primary Brand': 'primaryBrand',
        'Secondary Brand': 'secondaryBrand',
        'Team': 'team'
    }

    # Iterate through each row in the CSV
    for record in csvContent:
        # Clean and normalize the record
        record = {key.strip(): (value.strip() if isinstance(
            value, str) else value) for key, value in record.items()}

        # Map the record to the desired format
        mappedRecord = {}
        for key, mappedKey in field_mapping.items():
            cellValue = getCellValue(record, key)
            if key == 'Primary Brand' or key == 'Secondary Brand':
                if isinstance(cellValue, str):
                    # Split by comma and clean each item
                    cleaned_list = [v.strip()
                                    for v in cellValue.split(",") if v.strip()]
                elif isinstance(cellValue, list):
                    cleaned_list = [str(v).strip() for v in cellValue if v]
                else:
                    cleaned_list = []
                mappedRecord[mappedKey] = cleaned_list
            else:
                mappedRecord[mappedKey] = str(getCellValue(record, key, "-"))

        # Append the mapped record to the list
        skillMatrixContent.append(mappedRecord)

    return skillMatrixContent


@tracer.capture_method
def storeSkillMatrixInDB(skillMatrixData):
    for record in skillMatrixData:
        skillMatrixId = str(uuid.uuid4())
        now = datetime.now().strftime('%d-%m-%YT%H:%M:%S.%fZ')

        payload = {
            'skillMatrixId': skillMatrixId,
            'roleName': record.get('role', ''),
            'name': record.get('name', ''),
            'emailAddress': record.get('email', ''),
            'team': record.get('team', ''),
            'primaryBrand': record.get('primaryBrand', []),
            'secondaryBrand': record.get('secondaryBrand', []),
            'createdAt': now,
            'createdBy': "System",
            'updatedAt': now,
            'updatedBy': "System"
        }
        SKILL_MATRIX_DDB_TABLE.put_item(Item=payload)


@tracer.capture_method
def sendToSQS(payload):
    payloadJson = json.dumps(payload, default=decimalDefault)
    response = SQS_CLIENT.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=payloadJson
    )
    return response


@tracer.capture_method
def decimalDefault(obj):
    """Helper function for JSON serialization of Decimal types"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


@tracer.capture_method
def archiveFile(pathKey: str):
    """
    Archive the file by copying it to the archive folder in S3
    """
    # key = pathKey.replace('+', ' ')
    copy_source = {
        'Bucket': S3_BUCKET_NAME,
        'Key': pathKey
    }
    newKey = pathKey.replace(f"input/", "archive/")
    S3_CLIENT.copy_object(Bucket=S3_BUCKET_NAME,
                          CopySource=copy_source, Key=newKey)
    S3_CLIENT.delete_object(Bucket=S3_BUCKET_NAME, Key=pathKey)

# Get value from cell


@tracer.capture_method
def getCellValue(row, column, default=None):
    cell = row[column]
    if not pd.isna(cell):
        return cell
    else:
        return default


@tracer.capture_method
def original_message_extraction_prompt(email_input, senderEmail, recipientEmail, subject, emailSentDate, ccList):

    prompt = ORIGINAL_MESSAGE_EXTRACTION_PROMPT.format(
        email_input=json.dumps(email_input, default=str),
        senderEmail=json.dumps(senderEmail, default=str),
        recipientEmail=json.dumps(recipientEmail, default=str),
        subject=json.dumps(subject, default=str),
        emailSentDate=json.dumps(emailSentDate, default=str),
        ccList=json.dumps(ccList, default=str)
    )
    result, input_tokens, output_tokens = promptBedrock(prompt)
    cleaned_result = clean_analysisResult(result)

    return cleaned_result


@tracer.capture_method
def clean_analysisResult(analysis_data) -> Dict:
    try:
        json_patterns = [
            r'```(?:json)?\s*([\s\S]*?)\s*```',  # group(1) is the content
        ]

        json_str = None
        for pattern in json_patterns:
            json_match = re.search(pattern, analysis_data)
            if json_match:
                # Use group(1) for the first pattern, group(0) for the second
                if pattern.startswith('```'):
                    json_str = json_match.group(1)
                    json_str = json_str.strip()
                    # Remove leading 'json' if present
                    json_str = re.sub(r'^\s*json\s*', '',
                                      json_str, flags=re.IGNORECASE)
                else:
                    json_str = json_match.group(0)
                if json_str:
                    break

        if not json_str:
            start_idx = analysis_data.find('{')
            end_idx = analysis_data.rfind('}') + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = analysis_data[start_idx:end_idx]
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
            else:
                logger.exception(
                    {"message": "Could not locate valid JSON content by brackets"})

        if json_str:
            try:
                # logger.info(json_str)
                analysis_data = sanitizeAndParseJson(json_str)
                return analysis_data
            except json.JSONDecodeError as je:
                logger.exception({"message": f"JSON parsing error: {str(je)}"})

        fallback_response = constructFallbackResponse(analysis_data)
        return fallback_response

    except Exception as e:
        logger.exception(
            {"message": f"Exception in clean_analysisResult: {str(e)}"})
        return constructFallbackResponse(analysis_data)


@tracer.capture_method
def sanitizeAndParseJson(json_str):
    try:
        # First attempt to parse as is
        return json.loads(json_str)
    except json.JSONDecodeError:
        # If it fails, try to fix common issues

        # 1. Replace newlines in string values
        # This regex finds strings inside quotes and replaces newlines with spaces
        pattern = r'("(?:\\.|[^"\\])*")'

        def replace_newlines(match):
            return match.group(0).replace('\n', ' ')

        sanitized_str = re.sub(pattern, replace_newlines, json_str)

        # 2. Remove trailing commas in objects and arrays
        sanitized_str = re.sub(r',\s*}', '}', sanitized_str)
        sanitized_str = re.sub(r',\s*\]', ']', sanitized_str)

        try:
            # Try parsing the sanitized string
            return json.loads(sanitized_str)
        except json.JSONDecodeError as e:
            # If still failing, try a more brute force approach
            # Remove all newlines and excess whitespace
            compressed_str = re.sub(r'\s+', ' ', json_str).strip()

            try:
                return json.loads(compressed_str)
            except json.JSONDecodeError:
                # If all else fails, provide a more helpful error message
                raise ValueError(
                    f"Could not parse JSON even after sanitization. Original error: {str(e)}")


@tracer.capture_method
def constructFallbackResponse(result: str) -> Dict:
    """Construct fallback response when parsing fails"""
    return result
