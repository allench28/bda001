from aws_cdk import (
    Stack,
    aws_sns as sns,
    aws_ssm as ssm,
    Tags
)
import os
from constructs import Construct
from .environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'LITE_DEMO')

class LiteDemoSNSStack(Stack):
    """
    SNS Stack for Lite Demo
    
    Topics:
    1. Processing Mismatch Topic - Notify when document processing results are mismatched
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create SNS Topic for processing mismatch notifications
        mismatch_topic = sns.Topic(
            self,
            f'{PROJECT_NAME}-LiteDemo-ProcessingMismatch',
            topic_name=f'{PROJECT_NAME}-LiteDemo-ProcessingMismatch',
            display_name='Lite Demo Processing Mismatch Notifications'
        )

        # Store SNS Topic ARN in SSM Parameter for Lambda functions to reference
        topic_arn_parameter = ssm.StringParameter(
            self,
            f'{PROJECT_NAME}-LiteDemo-SNSTopicArn-Param',
            parameter_name=f'/{PROJECT_NAME}/LiteDemo/SNSTopicArn',
            string_value=mismatch_topic.topic_arn,
            description='Lite Demo SNS Topic ARN for processing mismatch notifications',
            tier=ssm.ParameterTier.STANDARD
        )

        # Add tags
        Tags.of(self).add('Project', PROJECT_NAME)
        Tags.of(self).add('Environment', env)
        Tags.of(self).add('Stack', 'LiteDemo-SNS')
        Tags.of(self).add('ManagedBy', 'CDK')

        # Export references
        self.mismatch_topic = mismatch_topic
        self.topic_arn = mismatch_topic.topic_arn