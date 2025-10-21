import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_ssm as ssm,
    Tags
)
from constructs import Construct
from aap_backend_cdk.environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')

class AapBackendLambdaEventBridgeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')

        lambda_dir = './lambda/Functions/'
        docker_dir = './lambda/DockerFunction/'

        # Lambda IAM Role
        AapLambdaRole = iam.Role.from_role_arn(
            self, f'{PROJECT_NAME}' + 'LambdaRoleMY',
            f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}LambdaRoleMY'
        )

        # Lambda Layers
        LambdaBaseLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'LambdaBaseLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'LambdaBaseLayerArn', f'{PROJECT_NAME}' + '-LambdaBaseLayerArn').string_value
        )

        GenericLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'GenericLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'GenericLayerArn', f'{PROJECT_NAME}' + '-GenericLayerArn').string_value
        )

        CreateGlueTablePartition = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-CreateGlueTablePartition',
            function_name=f'{PROJECT_NAME}' + '-CreateGlueTablePartition',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-CreateGlueTablePartition'),
            layers=[LambdaBaseLayer],
            description="Function to create partition for glue tables.",
            role=AapLambdaRole,
            environment={
                'S3OUTPATH': s3Map[env]['athenaResult'].format(PROJECT_NAME.lower()),
            },
            timeout=Duration.minutes(15),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )


        # Eventbridge Rules
        ScheduledCreateGlueTablePartitionRule = events.Rule(
            self, 'ScheduledCreateGlueTablePartitionRule',
            description='Schedule Rule to create partition for glue table for every last day of month',
            rule_name=f'{PROJECT_NAME}-ScheduledCreateGlueTablePartitionRule',
            schedule=events.Schedule.cron(day='L', hour='9', minute='0'),
            targets=[
                targets.LambdaFunction(
                    CreateGlueTablePartition
                )
            ],
            enabled=True
        )

        # GetNetsuiteDocuments = lambda_.Function(
        #     self, f'{PROJECT_NAME}' + '-GetNetsuiteDocuments',
        #     function_name=f'{PROJECT_NAME}' + '-GetNetsuiteDocuments',
        #     runtime=lambda_.Runtime.PYTHON_3_12,
        #     handler='lambda_function.lambda_handler',
        #     code=lambda_.Code.from_asset(lambda_dir + 'AAP-GetNetsuiteDocuments'),
        #     layers=[LambdaBaseLayer, GenericLayer],
        #     description="Function to retrieve documents from NetSuite",
        #     role=AapLambdaRole,
        #     environment={
        #         'NS_CREDENTIALS': f'{env}/netsuiteClientKey',
        #         'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME.upper()),
        #         'PROCESS_DOCUMENT_QUEUE_URL': f'https://sqs.{REGION_NAME}.amazonaws.com/{ACCOUNT_ID}/ProcessDocumentDataQueue'
        #     },
        #     timeout=Duration.minutes(10),
        #     tracing=lambda_.Tracing.ACTIVE,
        #     memory_size=1024
        # )

        # Schedule for retrieving NetSuite documents 10 minutes
        # ScheduledGetNetsuiteDocumentsRule = events.Rule(
        #     self, 'ScheduledGetNetsuiteDocumentsRule',
        #     description='Schedule Rule to retrieve documents from NetSuite daily',
        #     rule_name=f'{PROJECT_NAME}-ScheduledGetNetsuiteDocumentsRule',
        #     schedule=events.Schedule.cron(minute='10'),
        #     targets=[
        #         targets.LambdaFunction(
        #             GetNetsuiteDocuments
        #         )
        #     ],
        #     enabled=True
        # )