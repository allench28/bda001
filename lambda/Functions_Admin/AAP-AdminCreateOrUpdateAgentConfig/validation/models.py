from typing import List
from enum import Enum
from dataclasses import dataclass

class TriggerType(str, Enum):
    EMAIL = "EMAIL"
    BATCH = "BATCH"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper_value = value.upper()
            for member in cls:
                if member.value == upper_value:
                    return member
        return None

class FrequencyType(str, Enum):
    MINUTES = "MINUTES"
    HOURS = "HOURS"
    DAYS = "DAYS"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper_value = value.upper()
            for member in cls:
                if member.value == upper_value:
                    return member
        return None

class ServiceType(str, Enum):
    ACCOUNT_RECEIVABLE = "Account Receivable"
    ACCOUNT_PAYABLE = "Account Payable"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            upper_value = value.upper()
            for member in cls:
                if member.value == upper_value:
                    return member
        return None

@dataclass
class Service:
    type: ServiceType
    actions: str

    def to_dict(self) -> dict:
        return {
            "type": self.type.value,
            "actions": self.actions
        }

@dataclass
class ProcessingFrequencyConfig:
    trigger_type: TriggerType
    trigger_frequency_type: FrequencyType
    trigger_frequency_value: int
    email_recipients: List[str]

    def to_dict(self) -> dict:
        return {
            "triggerType": self.trigger_type.value,
            "triggerFrequencyType": self.trigger_frequency_type.value,
            "triggerFrequencyValue": self.trigger_frequency_value,
            "emailRecipients": self.email_recipients
        }

@dataclass
class Configuration:
    mapping_one_url: str
    mapping_two_url: str
    content_checking: bool
    system_prompt: str | dict
    processing_frequency_config: ProcessingFrequencyConfig

    def to_dict(self) -> dict:
        return {
            "mappingOneURL": self.mapping_one_url,
            "mappingTwoURL": self.mapping_two_url,
            "contentChecking": self.content_checking,
            "systemPrompt": self.system_prompt,
            "processingFrequencyConfig": self.processing_frequency_config.to_dict()
        }

@dataclass
class AgentConfigRequest:
    name: str
    description: str
    service: Service
    configuration: Configuration
    active_status: bool
    merchantId: str

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "service": self.service.to_dict(),
            "configuration": self.configuration.to_dict(),
            "activeStatus": self.active_status,
            "merchantId": self.merchantId
        }