import os
from aws_cdk import (
    Stack,
    Duration,
    Fn,
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
ES_ENDPOINT = os.environ.get('ES_ENDPOINT', '')

class AapBackendLambdaDynamoDbStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs) 
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')

        lambda_dir = './lambda/Functions/'

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

        # Lambda Functions
        SendDdbToESMY = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-SendDdbToESMY',
            function_name=f'{PROJECT_NAME}' + '-SendDdbToESMY',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-SendDdbToES'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to Send DynamoDB Data to Elasticsearch",
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        AuditLogStream = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AuditLogStream',
            function_name=f'{PROJECT_NAME}' + '-AuditLogStream',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AuditLogStream'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to Insert Event To Kinesis Firehose and Stream to S3",
            role=AapLambdaRole,
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )
        
        # DynamoDB Triggers to Elastisearch
        ddbToEsTableList = [
            'Merchant',
            'UserGroup',
            'UserMatrix',
            'User',
            'APIUserMatrix',
            'AgentConfigurations',
            'ExtractedDocuments',
            'ExtractedDocumentsLineItems',
            'InboxMonitoring',
            'DocumentUpload',
            'Timeline',
            'DownloadJob',
            'ThreeWayMatchingResults',
            'ThreeWayMatchingLineItems',
            'ExtractedGrn',
            'ExtractedGrnLineItems',
            'SequenceNumberGenerator',
            'BankStatement',
            'PaymentGatewayReport',
            'SalesStatement',
            'ReconciliationResults',
            'ExtractedPo',
            'ExtractedPoLineItems',
            'Store',
            'ExtractedReferralLetter',
            'FixedAsset',
            'AcquisitionJournal',
        ]

        auditLogStreamList = [
            'Merchant',
            'UserGroup',
            'UserMatrix',
            'User',
            'APIUserMatrix',
            'AgentConfigurations',
            'ExtractedDocuments',
            'InboxMonitoring',
            'DocumentUpload',
            'Timeline',
            'DownloadJob',
            'ThreeWayMatchingResults',
            'ThreeWayMatchingLineItems',
            'ExtractedGrn',
            'ExtractedGrnLineItems',
            'SequenceNumberGenerator',
            'ExtractedPo',
            'ExtractedPoLineItems',
            'FixedAsset',
            'AcquisitionJournal',
        ]
    
        for table in ddbToEsTableList:
            SendDdbToESMY.add_event_source_mapping(
                '{}StreamArn'.format(table),
                event_source_arn=Fn.import_value('{}{}StreamArn'.format(PROJECT_NAME.title(), table)),
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                batch_size=100
            )

        for table in auditLogStreamList:
            AuditLogStream.add_event_source_mapping(
                '{}StreamArn'.format(table),
                event_source_arn=Fn.import_value('{}{}StreamArn'.format(PROJECT_NAME.title(), table)),
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                batch_size=100
            )


        # ThreeWayMatching = lambda_.Function(
        #     self, f'{PROJECT_NAME}' + '-ThreeWayMatching',
        #     function_name=f'{PROJECT_NAME}' + '-ThreeWayMatching',
        #     runtime=lambda_.Runtime.PYTHON_3_12,
        #     handler='lambda_function.lambda_handler',
        #     code=lambda_.Code.from_asset(lambda_dir + 'AAP-ThreeWayMatching'),
        #     layers=[LambdaBaseLayer, GenericLayer],
        #     description="Function to perform 3 way matching",
        #     role=AapLambdaRole,
        #     environment={},
        #     timeout=Duration.seconds(300),
        #     tracing=lambda_.Tracing.ACTIVE,
        #     memory_size=512
        # )

        # ThreeWayMatching = lambda_.Function.from_function_arn(
        #     self, f'{PROJECT_NAME}' + '-ThreeWayMatching',
        #     f'arn:aws:lambda:{self.region}:{ACCOUNT_ID}:function:{PROJECT_NAME.title()}-ThreeWayMatching'
        # )

        # ThreeWayMatching.add_event_source_mapping(
        #     '{}StreamArn'.format('ExtractedDocuments'),
        #     event_source_arn=Fn.import_value('{}{}StreamArn'.format(PROJECT_NAME.title(), 'ExtractedDocuments')),
        #     starting_position=lambda_.StartingPosition.TRIM_HORIZON,
        #     batch_size=100
        # )
