from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    Tags
)
import os
from constructs import Construct
from .environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'LITE_DEMO')

class LiteDemoDynamoDBStack(Stack):
    """
    DynamoDB Stack for Lite Demo
    
    Tables:
    1. Documents Table - Store document metadata, status, and extraction results
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get table name from environment configuration
        table_name = DynamoDBTableMap[env]['LITE_DEMO_DOCUMENTS'].format(
            PROJECT_NAME.lower().replace('_', '-')
        )

        # ===== Documents Table =====
        documents_table = dynamodb.Table(
            self,
            f'{PROJECT_NAME}-LiteDemo-Documents',
            table_name=table_name,
            partition_key=dynamodb.Attribute(
                name='documentId',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.RETAIN,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES
        )

        # Add tags
        Tags.of(self).add('Project', PROJECT_NAME)
        Tags.of(self).add('Environment', env)
        Tags.of(self).add('Stack', 'LiteDemo-DynamoDB')
        Tags.of(self).add('ManagedBy', 'CDK')

        # Export table reference
        self.documents_table = documents_table
        self.documents_table_name = table_name
