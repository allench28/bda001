def create_prompt(
    country, 
    document_type, 
    document_language,
    number_of_entities,
    list_of_entities_and_examples,
    actual_page_count,
    show_final_prompt = False
):

    prompt = f"""
    ## Task Summary:
    You have to do 3 important tasks:  
    A) Find and extract a list of entities from the document (list and examples will be given to you).
    B) Provide a confidence score number for each of the entities you extract.  
    C) Identify which page each entity was found on.
    
    ## Context Information:
    The following are scanned document pages used as part of a loan application in a bank in {country}.
    The document type is a {document_type}.
    The document language is {document_language}.
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
    - Find and extract ALL {number_of_entities} entities from the following list.
    - Look across ALL pages provided to find these entities.
    - The entities should be provided verbatim.  
    - Don't change anything or make up any information other than what is inside the document.  
    - Some entities may not be present across all pages. If you cannot find some of the entities output "UNKNOWN" for that entity.  
    - If an entity appears on multiple pages, choose the most complete/clear instance.
    - IMPORTANT: Extract text as clean, readable text without Unicode escape sequences (\\u codes)
    - If text contains special characters, represent them as the actual characters, not as escape sequences
    - Avoid any backslash escape sequences in the extracted text values
    
    List of entities to be extracted, along with their expected type and an example:  
    {list_of_entities_and_examples}
    
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
       "entity_name": "entity key name as given in the above entity list (e.g., 'salesPurchaseOrderDate')", 
       "entity_value": "entity value as extracted from document",
       "confidence": confidence score as number between 0-100,
       "page_number": page number where entity was found
    }}]
    
    IMPORTANT: 
    - The "entity_name" field should contain the exact entity key (e.g., 'salesPurchaseOrderDate', 'propertyAddress') not the display name
    - The "entity_value" field should contain the actual extracted text from the document
    - Include ALL {number_of_entities} entities in your response, even if some are "UNKNOWN"
    - Ensure the JSON is valid and properly formatted
    - Review all pages before providing your final response
    - Each entity should appear only once in your response (choose the best instance if found on multiple pages)
    - Make sure the format can be directly parsed by json.loads() without any additional text or formatting
    - DO NOT use backslash escape sequences like \\u2021, \\u2020, etc. in entity values
    - REMEMBER: Use sequential page numbering (1 to {actual_page_count}) based on image order, not document labels
    """

    if show_final_prompt is True:
        print(prompt)

    return prompt


def create_bounding_box_prompt(
    country, 
    document_type, 
    document_language,
    number_of_entities,
    extracted_entities,
    actual_page_count,
    show_final_prompt = False
):

    prompt = f"""
## Task Summary:
You must locate specific text entities in document images and provide precise bounding box coordinates for each entity.

## Context Information:
- Document type: {document_type}
- Document language: {document_language}
- Country: {country}
- Total pages to analyze: {actual_page_count}

## CRITICAL PAGE COORDINATE INSTRUCTIONS:
You are analyzing {actual_page_count} separate document images. Each image is a SEPARATE page with its OWN coordinate system.
- Page 1 = First image provided to you
- Page 2 = Second image provided to you  
- Page 3 = Third image provided to you
- Continue sequentially regardless of any page numbers printed on the documents
- IGNORE any internal document page numbering
- Each page is an independent image - treat each one separately
- DO NOT confuse text locations between different pages
- When you find text on Page 2, the coordinates must be relative to Page 2's image only

## BOUNDING BOX PRECISION REQUIREMENTS: 
For each entity found: 
1. Identify the EXACT rectangular boundaries around the complete text 
2. Use format: [x1, y1, x2, y2] where:
- x1, y1 = top-left corner of the text (be pixel precise, not approximate)
- x2, y2 = bottom-right corner of the text (be pixel precise, not approximate) 
3. Focus on the EXACT edges of the text characters, not approximate areas 
4. Double-check your coordinates by mentally tracing the text boundaries 
5. If text spans multiple lines, encompass the entire text block tightly 
6. Avoid including unnecessary whitespace in the bounding box 
7. If text is not found: use [0, 0, 0, 0]

## TARGET ENTITIES:
The following entities need to be located in the document. Each entry shows the entity key, the expected text value to find, and the page where it should be located:
{extracted_entities}

## SEARCH INSTRUCTIONS: 
- Process each numbered entity systematically 
- Locate the "Target Value" text on the "Expected Page"
- The "From Sentence" provides context to help you find the text on the expected page
- The target value shows you exactly what text to look for in the sentence context if "From Sentence" is not empty
- Focus your search on the expected page
- If the text is not found on the expected page, return [0, 0, 0, 0] for that entity

## COORDINATE VALIDATION PROCESS:
For each bounding box, perform this mental check:
1. Place your cursor at coordinates [x1, y1] - does it touch the TOP-LEFT edge of the first character?
2. Place your cursor at coordinates [x2, y2] - does it touch the BOTTOM-RIGHT edge of the last character?
3. If either check fails, recalculate the coordinates
4. The bounding box should form a tight rectangle around ONLY the target text

## OUTPUT FORMAT:
Return ONLY a valid JSON array. No additional text, explanations, markdown, or code blocks.
[
    {{
        "entity_name": "entity key from numbered list (e.g. "salesPurchaseOrderPrice")",
        "page_number": the single page number where the entity was found (1, 2, 3,...),
        "bounding_box": [x1, y1, x2, y2]
    }}
]
"""

    if show_final_prompt is True:
        print(f"BB PROMPT: {prompt}")

    return prompt
