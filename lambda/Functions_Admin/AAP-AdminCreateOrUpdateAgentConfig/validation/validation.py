import re
import os
import boto3
from typing import Any
from enum import Enum
from aws_lambda_powertools import Logger
from .models import Service, ProcessingFrequencyConfig, FrequencyType, ServiceType, Configuration, AgentConfigRequest, TriggerType
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException, ResourceNotFoundException

logger=Logger()

def get_default_prompt(service_action):
    return f"""
You are an advanced document analysis assistant specializing in {service_action}.

OBJECTIVE:
Extract structured information from invoice documents while maintaining data integrity and accuracy.

PROCESSING GUIDELINES:
- Identify and extract all standard invoice fields (invoice number, date, total amount, tax, etc.)
- Recognize vendor-specific formatting and adapt extraction accordingly
- Maintain original numerical precision and currency formatting
- Flag any inconsistencies between calculated totals and stated totals
- Preserve hierarchical relationships between line items
- Identify and extract payment terms and due dates

QUALITY ASSURANCE:
For each extracted field, assess confidence level and flag uncertain extractions for human review.

OUTPUT FORMAT:
Return data in a consistent structured format optimized for database integration and downstream processing.
"""


def _is_potential_prompt_injection(prompt: str) -> bool:
    injection_patterns = [
        r"ignore previous instructions", 
        r"execute",
        r"system",
        r"admin",
        r"http[s]?://",  # URLs that could lead to external exploitation
        r"<script>",  # HTML/JavaScript injection
        r"\{\{.*\}\}",  # Template injection patterns
    ]

    # Check if any of the patterns are found in the prompt
    for pattern in injection_patterns:
        if re.search(pattern, prompt, re.IGNORECASE):
            return True

    return False

def _validate_system_prompt(prompt: str, service_action) -> str:
    logger.info(prompt)
    # Check if the prompt is empty or just whitespace
    if not prompt or prompt.strip() == "":
        return get_default_prompt(service_action)

    prompt = prompt.strip()

    if _is_potential_prompt_injection(prompt):
        raise BadRequestException("Please enter a different prompt. The prompt contains potential security risks")

    return prompt

def _validate_enum(enum_cls, value: Any, field_name: str) -> Enum:
    if not value:
        raise BadRequestException(f"{field_name} is required")
    try:
        return enum_cls(value)
    except ValueError:
        raise BadRequestException(f"Invalid {field_name} value")

def _validate_boolean_fields(bool_field: Any, field_name: str) -> bool:
    if bool_field is not None and not isinstance(bool_field, bool):
        raise BadRequestException(f"{field_name} must be a boolean value")

def _validate_name(name: str) -> str:
    if not name or not isinstance(name, str) or len(name.strip()) == 0:
        raise BadRequestException("Name must be a non-empty string")
    if len(name) > 100:  # Example length limit
        raise BadRequestException("Name must be less than 100 characters")

    return name.strip()

def _validate_description(description: str) -> str:
    if not description or not isinstance(description, str) or len(description.strip()) == 0:
        raise BadRequestException("Description must be a non-empty string")
    if len(description) > 500:  # Example length limit
        raise BadRequestException("Description must be less than 500 characters")

    return description.strip()

def _validate_service(service_data: dict) -> Service:
    if not service_data.get('actions'):
        raise BadRequestException("Actions are required for the service")

    return Service(
        type=_validate_enum(ServiceType, service_data.get('type'), "type"),
        actions=service_data.get('actions')
    )

def _validate_configuration(config_data: dict, service_obj, is_update) -> Configuration:
    service_action = service_obj.actions

    freq_config_obj = _validate_processing_frequency_config(config_data.get('processingFrequencyConfig', {}))

    mapping_one_url = config_data.get('mappingOneURL', '')
    mapping_two_url = config_data.get('mappingTwoURL', '')

    if not mapping_one_url:
        raise BadRequestException("Mapping One is required")

    if not mapping_two_url:
        raise BadRequestException("Mapping Two is required")
    
    if is_update:
        valid_prompt = _validate_system_prompt(config_data.get('systemPrompt').get('updatedPrompt'), service_action)
        prompt_config_obj = {
            'promptArn': config_data.get('systemPrompt').get('promptArn'),
            'promptVersion': config_data.get('systemPrompt').get('promptVersion'),
            'updatedPrompt': valid_prompt
        }

        return Configuration(
            mapping_one_url=config_data.get('mappingOneURL'),
            mapping_two_url=config_data.get('mappingTwoURL'),
            content_checking=config_data.get('contentChecking'),
            system_prompt=prompt_config_obj,
            processing_frequency_config=freq_config_obj
        )
    
    else:
        valid_prompt = _validate_system_prompt(config_data.get('systemPrompt'), service_action)

        return Configuration(
            mapping_one_url=config_data.get('mappingOneURL'),
            mapping_two_url=config_data.get('mappingTwoURL'),
            content_checking=config_data.get('contentChecking'),
            system_prompt=valid_prompt,
            processing_frequency_config=freq_config_obj
        )

def _validate_processing_frequency_config(freq_config: dict) -> ProcessingFrequencyConfig:

    trigger_type = _validate_enum(TriggerType, freq_config.get('triggerType'), "triggerType")
    trigger_frequency_type = _validate_enum(FrequencyType, freq_config.get('triggerFrequencyType'), "triggerFrequencyType")
    trigger_frequency_value = int(freq_config.get('triggerFrequencyValue', 1))

    if trigger_frequency_type == FrequencyType.DAYS:
        if not (1 <= trigger_frequency_value):
            raise BadRequestException("Processing frequency must be greater than 0 for DAYS")
    elif trigger_frequency_type == FrequencyType.HOURS:
        if not (1 <= trigger_frequency_value <= 23):
            raise BadRequestException("Processing frequency must be between 1 and 23 for HOURS")
    elif trigger_frequency_type == FrequencyType.MINUTES:
        if not (1 <= trigger_frequency_value <= 59):
            raise BadRequestException("Processing frequency must be between 1 and 59 for MINUTES")

    email_recipients = freq_config.get('emailRecipients')
    if not isinstance(email_recipients, list) or len(email_recipients) == 0:
        raise BadRequestException("Email recipients must be a non-empty list of email addresses")

    for email in email_recipients:
        if not isinstance(email, str) or len(email.strip()) == 0:
            raise BadRequestException("Email recipients must be a non-empty list of email addresses")

    return ProcessingFrequencyConfig(
        trigger_type=trigger_type,
        trigger_frequency_type=trigger_frequency_type,
        trigger_frequency_value=trigger_frequency_value,
        email_recipients=freq_config.get('emailRecipients')
    )


def validate_request(request_data: dict, is_update) -> AgentConfigRequest:
        # Validate required fields
    required_fields = ['name', 'description', 'configuration', 'service', 'activeStatus']
    for field in required_fields:
        if not request_data.get(field):
            raise BadRequestException(f"{field} is required")
        
    configuration = request_data.get('configuration')
    service = request_data.get('service')

    # Validate Name and Description
    valid_name = _validate_name(request_data.get('name', ''))
    valid_description = _validate_description(request_data.get('description', ''))

    # Validate ActiveStatus and ContentChecking
    _validate_boolean_fields(request_data.get("activeStatus"), "activeStatus")
    _validate_boolean_fields(configuration.get('contentChecking'), "contentChecking")

    # Validate Service
    service_obj = _validate_service(service)

    # Validate Configuration
    config_obj = _validate_configuration(configuration, service_obj, is_update)

    # Create and return validated AgentConfigRequest
    return AgentConfigRequest(
        name=valid_name,
        description=valid_description,
        service=service_obj,
        configuration=config_obj,
        active_status=request_data['activeStatus'],
        merchantId=request_data['merchantId']
    )
