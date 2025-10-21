def generate_document_type_prompt(document_templates):
    instruction = "Given the document, identify the document type based on the characteristics provided, extract the same value provided in context\n\nCharacteristics of the document type (must follow exactly the instruction given in -, be mindful that the instruction is case sensitive, the document need to match the characteristics given:\n"
    
    for template in document_templates:
        for index, type_data in enumerate(template.get('typeDatas', [])):
            instruction += f"{index}. {type_data.get('typeName')}\n- {type_data.get('instruction')}\n\n"
            
    instruction += "You must only return the document type with no additional explanation"
    
    return instruction