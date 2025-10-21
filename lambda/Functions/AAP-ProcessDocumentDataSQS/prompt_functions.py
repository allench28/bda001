import json
import re
from pathlib import Path

from aws_lambda_powertools import Logger, Tracer
from bedrock_function import promptBedrock
from prompts import (generate_document_data_prompt,
                     generate_document_type_prompt)
from template_functions import (get_template_from_type_mapping,
                                load_json_templates)
from agent_configuration_functions import generate_agent_prompt


logger = Logger()
tracer = Tracer()


@tracer.capture_method
def get_document_type(document_lines=None, binary_content=None, file_extension=None):
    # Determine document type - passing binary_content and file_type

    document_templates = load_json_templates()
    prompt_data = generate_document_type_prompt(document_templates)
    if document_lines:
        prompt_data += f"\n\nDocument Text:\n{'\n'.join(document_lines)}"

    document_type, input_tokens, output_tokens = promptBedrock(
        prompt_data, binary_content, file_extension)
    
    if get_template_from_type_mapping(document_type):
        return document_type, input_tokens, output_tokens

    raise ValueError("Failed to determine document type")


@tracer.capture_method
def get_document_data(prompt_details, documentType, binary_content, file_extension, document_lines=None):
    input_tokens = 0
    output_tokens = 0
    document_data_list = []
    
    template = get_template_from_type_mapping(documentType)
    fields_list = template.get("fieldsList", [])
    
    extracted_document_text = None
    if document_lines:
        extracted_document_text = "\n".join(document_lines)

    for fields_list_index, template_fields in enumerate(fields_list):
        if documentType == "Invoice":
            # prompt from agent config
            prompt_data = generate_agent_prompt(extracted_document_text, prompt_details)

        else:
            # sui bin prompt 
            prompt_data = generate_document_data_prompt(documentType, template, fields_list_index, extracted_document_text)

        response, data_input_tokens, data_output_tokens = promptBedrock(
            prompt_data, binary_content, file_extension
        )

        input_tokens += data_input_tokens
        output_tokens += data_output_tokens

        # I want to remove template_fields entirely
        results = process_bedrock_document_data_response(response, template_fields)
        document_data_list.extend(results)

    if not results:
        raise ValueError("Failed to process document data. No data found.")

    if len(document_data_list) <= 0:
        raise ValueError("Failed to process document data. No data found.")
    return document_data_list, input_tokens, output_tokens


@tracer.capture_method
def process_bedrock_document_data_response(bedrock_response, template_fields):
    match = re.search(r"<data>(.*?)</data>", bedrock_response, re.DOTALL)
    logger.info(f"Match: {match.group(1)}")
    if not match:
        raise ValueError(f"Failed to extract CSV for {document_type}")
    bedrock_datas = match.group(1).split("\n")
        
    results = []
    index = 0
    for field_data in bedrock_datas:
        if not field_data or index >= len(template_fields):
            continue
        field_name = template_fields[index]["fieldName"]
        field_type = template_fields[index]["fieldType"]
        field_mapping = template_fields[index]["fieldMapping"]
        data_parts = field_data.split("|")
        result_data = {
            "fieldName": field_name,
            "fieldType": field_type,
            "fieldMapping": field_mapping,
            "fieldValue": data_parts[1],
            "confidenceScore": data_parts[2],
            "fileLineIndex": data_parts[3],
            "explanation": data_parts[4] if len(data_parts) > 4 else ""
        }
        results.append(result_data)
        index += 1
        
    return results
