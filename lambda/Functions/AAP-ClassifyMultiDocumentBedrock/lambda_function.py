import os
import json
import boto3
from typing import List, Dict
from aws_lambda_powertools import Logger, Tracer
from bedrock_function import prompt_bedrock


DOCUMENT_SPLITTER_LAMBDA_ARN = os.environ.get("DOCUMENT_SPLITTER_LAMBDA_ARN")
INPUT_PREFIX = os.environ.get('INPUT_PREFIX')

S3_CLIENT = boto3.client("s3")
LAMBDA_CLIENT = boto3.client("lambda")

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        logger.info(f"Receiving event: {event}")

        record = event["Records"][0]
        sns_message = json.loads(record.get("Sns", {}).get("Message", "{}"))

        job_id = sns_message.get("JobId")
        sns_document_location = sns_message.get("DocumentLocation")
        bucket = sns_document_location.get("S3Bucket")
        object_name = sns_document_location.get("S3ObjectName")
        merchant_id = object_name.split("/")[1]
        document_upload_id = object_name.split("/")[2]

        logger.info(f"Processing file: {object_name}")

        input_prefix = f"{INPUT_PREFIX}/{merchant_id}/{document_upload_id}/{job_id}"

        # List all Textract output files
        response = S3_CLIENT.list_objects_v2(Bucket=bucket, Prefix=input_prefix)

        # Get all Textract output files (excluding metadata.json)
        s3_files = [
            item["Key"]
            for item in response.get("Contents", [])
            if not item["Key"].endswith(".s3_access_check")
        ]

        logger.info(s3_files)
        all_document_lines = []
        for key in s3_files:
            file_content = S3_CLIENT.get_object(Bucket=bucket, Key=key)["Body"].read()
            document_lines = process_textract_output(file_content)
            all_document_lines.extend(document_lines)

        all_document_lines.sort(key=lambda x: int(x.split("|Page:")[1]))
        classification_result, input_tokens, output_tokens = classify_pages(
            all_document_lines
        )
        invoke_document_splitter(sns_document_location, job_id, classification_result)

    except Exception as e:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(e))
        logger.exception({"message": str(e)})
        raise  # Re-throw the exception to fail the entire batch


@tracer.capture_method
def process_textract_output(file_content):
    file_content_str = file_content.decode("utf-8")
    textract_result = json.loads(file_content_str)

    document_lines = []

    for block in textract_result["Blocks"]:
        if block.get("BlockType") == "LINE":
            if "Text" in block and "Page" in block:
                line = f"{block['Text']}|Page:{block['Page']}"
                document_lines.append(line)

    return document_lines


@tracer.capture_method
def classify_pages(document_lines: List[str]) -> Dict[str, List[int]]:
    all_pages = set()
    for line in document_lines:
        parts = line.split("|Page:")
        if len(parts) == 2:
            all_pages.add(int(parts[1]))

    classify_document_prompt = get_document_type_prompt(document_lines)

    result, input_tokens, output_tokens = prompt_bedrock(classify_document_prompt)

    classification = json.loads(result)

    logger.info(f"Classification Output: {classification}")
    logger.info(
        f"Classification complete. Input tokens: {input_tokens}, Output tokens: {output_tokens}"
    )

    return classification, input_tokens, output_tokens


@tracer.capture_method
def get_document_type_prompt(document_lines):
    page_samples = {}
    lines_per_page = {}

    for line in document_lines:
        parts = line.split("|Page:")
        if len(parts) == 2:
            text = parts[0]
            page = int(parts[1])

            if page not in lines_per_page:
                lines_per_page[page] = []

            lines_per_page[page].append(text)

    # for page, lines in lines_per_page.items():
    #     if len(lines) <= 50:
    #         page_samples[page] = lines
    #     else:
    #         middle_start = len(lines) // 2 - 2
    #         sample = lines[:20] + lines[middle_start : middle_start + 5] + lines[-20:]
    #         page_samples[page] = sample

    document_data = ""
    for page in sorted(lines_per_page.keys()):
        document_data += f"\n\n--- PAGE {page} ---\n"
        document_data += "\n".join(lines_per_page[page])

    prompt = f"""TASK:
Analyze the text from each page and classify which pages belong to which document type. Then identify separate instances of the same document type by carefully examining distinct document identifiers.

DOCUMENT TYPES AND KEY IDENTIFIERS:
1. INVOICE - Primary identifiers: invoice number, vendor name, invoice date, customer/bill-to name
   Secondary identifiers: total amount, tax amounts, payment terms

2. PURCHASE ORDER - Primary identifiers: PO number, order date, vendor name
   Secondary identifiers: delivery address, item quantities, prices

3. GOODS RECEIVED NOTE - Primary identifiers: GRN number, delivery date, supplier name
   Secondary identifiers: received by signatures, quantity received

DOCUMENT SEPARATION RULES (IN PRIORITY ORDER):
1. Different primary identifiers (different invoice numbers, PO numbers, etc.) ALWAYS indicate different documents
2. Same document type with same primary identifiers but different dates likely indicates different documents
3. Same document type with same primary identifiers and dates but different page numbering sequences indicates different documents
4. Pages with sequential numbering (Page 1 of 3, Page 2 of 3) should be grouped as one document
5. Pages with matching header/footer information likely belong to the same document

MULTI-PAGE DOCUMENT DETECTION:
- Look for "Page X of Y" or "Page X/Y" indicators
- Check for continued item listings or subtotals carried forward
- Look for page breaks that split logical sections of a document
- Check if a page appears incomplete (e.g., no total amount on what looks like an invoice)

IMPORTANT:
- Each page can only belong to a single document instance. For example: Page 1 cannot belong to two or more different document instances.

DOCUMENT TEXT SAMPLE:
{document_data}

OUTPUT FORMAT:
You STRICTLY only the return the response in the example JSON object below, you are NOT ALLOWED to deviate from this format. DO NOT PROVIDE ANY OTHER DETAILS:
{{
  "invoice": [
    {{
      "pages": [list of page numbers],
      "identifier": explanation
    }},
    {{
      "pages": [list of page numbers],
      "identifier": explanation
    }}
  ],
  "purchase_order": [
    {{
      "pages": [list of page numbers],
      "identifier": explanation
    }}
  ],
  "goods_received": [
    {{
      "pages": [list of page numbers],
      "identifier": explanation
    }}
  ],
  "unclassified": [list of page numbers]
}}

example response:
{{
  "invoice": [
    {{
      "pages": [1,2],
      "identifier": "Brief description like 'Invoice #12345 from ABC Corp'"
    }},
    {{
      "pages": [4],
      "identifier": "Brief description like 'Invoice #67890 from XYZ Inc'"
    }}
  ],
  "purchase_order": [
    {{
      "pages": [3],
      "identifier": "Brief description like 'PO #45678 to Supplier Inc'"
    }}
  ],
  "goods_received": [
    {{
      "pages": [5],
      "identifier": "Brief description like 'GRN for Delivery on 2023-04-15'"
    }},
    {{
      "pages": [7,8],
      "identifier": "Brief description like 'GRN for Delivery on 2023-06-15'"
    }}
  ],
  "unclassified": [6]
}}


IMPORTANT:
- Every page must be classified into exactly one category and one instance
- Look for document identifiers to separate multiple instances of the same document type
- Group pages that belong to the same document instance together
- If you're unsure about a page, place it in "unclassified"
- Analyze context between pages to determine if they belong to the same document instance
- Look for document headers, footers, page numbers to identify multi-page documents
"""

    return prompt


@tracer.capture_method
def invoke_document_splitter(document_location, job_id, classification_result):
    classification_result_with_metadata = {
        "classification": classification_result,
        "original_document_location": document_location,
        "job_id": job_id,
    }

    logger.info(
        f"Invoking Document Splitter with metadata: {classification_result_with_metadata}"
    )

    LAMBDA_CLIENT.invoke(
        FunctionName=DOCUMENT_SPLITTER_LAMBDA_ARN,
        InvocationType="Event",
        Payload=json.dumps(classification_result_with_metadata),
    )
