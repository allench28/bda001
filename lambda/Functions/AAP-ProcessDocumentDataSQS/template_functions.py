import json
import os
from pathlib import Path

from aws_lambda_powertools import Logger, Tracer

logger = Logger()
tracer = Tracer()

@tracer.capture_method
def load_json_templates():
    templates = []
    folder_name = "templates"
    # Get the current directory of the script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.join(current_dir, folder_name)

    # Iterate through all files in the templates directory
    for filename in os.listdir(templates_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(templates_dir, filename)
            with open(file_path, 'r') as file:
                try:
                    template_data = json.load(file)
                    templates.append(template_data)
                except json.JSONDecodeError as e:
                    print(f"Error loading {filename}: {e}")

    return templates


@tracer.capture_method
def get_template_from_type_mapping(type_name):
    templates = load_json_templates()
    for template in templates:
        for type_data in template.get('typeDatas', []):
            if type_data.get('typeName').lower() == type_name.lower():
                logger.info(f"TEMPLATE: {template}")
                return template
    return None
