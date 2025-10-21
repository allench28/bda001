# requirements
# - able to handle multiple types
# - just need to pass in document template for this to work

DOCUMENT_INSTRUCTION = """
- unless specified by the field's instruction, STRICTLY FORMAT ALL FOUND DATE VALUES in DD/MM/YYYY format. Example 22/10/24 is wrong while 22/10/2024 is correct.
- unless specified by the field's instruction, STRICTLY FORMAT ALL FOUND TIME VALUES in 'HH/MM' 24 hours format. Example '2:45 pm' is wrong while '14:45' is correct.
- format all final gender outputs to male as M and female as F
- please also provide a confidence score on how confident you are with each answer in a number between 0 to 100
- Please transcribe this image character by character, specifying any ambiguous letters or numbers that is suspected in your explanation.
- Please pay special attention to distinguishing between similar characters like 0/O/Q/D (zero, capital O, Q, D), 1/I/l/| (one, capital i, lowercase L, vertical bar), 2/Z/z (two, capital and lowercase Z), 5/S/s/§ (five, capital and lowercase S, section symbol), 6/b/G (six, lowercase b, capital G), 8/B/& (eight, capital B, ampersand), 9/g/q (nine, lowercase g and q), rn/m (r+n vs m), vv/w (double v vs w), and cl/d (c+l vs d).
- if the specified field is not present or applicable in the document, answer "". STRICTLY DO NOT ASSUME any field's value it's value based on other fields if it is not present. 
- if you cannot provide an answer for the field above, or if a field is not present or applicable in the document, answer ""
- if a field type is 'table' it means it is a table column, return all the values of the specified table column in order with a ',' delimiter and encapsulate the list with [], including empty rows in which the value is ""
"""

GIVEN_DOCUMENT_TEXT_INSTRUCTION = """
- for each field STRICTLY first find the values in the document, then find and compare to a value from the extracted text values that is simila (about 80% similar), if there is a simila value found then STRICTLY use the value from the extracted text instead as it is more accurate. Do this before formating the output with instruction number 4.
- Note that some values consist of mltiple words joined together, join them with space or not when it is appropriate. 
"""

OUTPUT_FORMAT_INSTRUCTION = """
You STRICTLY only the return the reponse of all field values in the example csv format below in order, you are NOT ALLOWED to deviate from this format:
<data>
<fieldIndex1>|<fieldValue1>|<confidenceScore1>|<explenationWhenSecified1>
<fieldIndex2>|<fieldValue2>|<confidenceScore2>|<explenationWhenSecified2>
</data>
example response:
<data>
1|Dan|69|'Dan' is written in the name field
2|12/04/2024|90
3|F|50|
4||40|The total amount is not present in the document file
5|[3,23,5,,not applicable]|40|The values of each row in the quantity table column
</data>
"""


def generate_document_data_prompt(document_type, document_template, field_list_index, extracted_document_text=None):

    fields_list = document_template.get('fieldsList', [])
    if not len(fields_list) > 0:
        raise ValueError(f"No fields provided in the template for {document_type} document type")
    fields = fields_list[field_list_index]
    total_fields= len(fields)
    if not total_fields > 0:
        raise ValueError(f"No fields provided in the template for {document_type} document type")

    field_instruction = ""
    for index, field in enumerate(fields):
        field_instruction += f"{field['fieldName']}|{field['fieldType']}|{field['instruction']}|{field.get('getExplanation', False)}"
        if index < len(fields) - 1:
            field_instruction += "\n"
    template_instruction = document_template.get('instruction', "")

    instruction = f"""Here is a {document_type} document file {"and it's extracted text values line by line seperated with line breaks inside the <doc> tags." if extracted_document_text else ""}
{"<doc>{extracted_document_text}</doc>" if extracted_document_text else ""}

From the document find the values for all {total_fields} fields specified in the following <fields> tag. In each point item specifies the field name, field type, instruction and whether or not an explanation for the field value found is to be provided, seperated with the | delimiter:
<fields>
{field_instruction}
</fields>

Here are some additiona instrictions to be applied when extracting data for all fields that you must STRICTLY ADHERE:
<instruction>
{DOCUMENT_INSTRUCTION}
{GIVEN_DOCUMENT_TEXT_INSTRUCTION if extracted_document_text else ""}
{template_instruction}
</instruction>

{OUTPUT_FORMAT_INSTRUCTION}

There should only be {total_fields} lines inside the <data> tags, if there are more or less lines, please check the instructions above and the document again to ensure all fields are captured correctly.
"""

    return instruction
