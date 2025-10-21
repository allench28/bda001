import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_ec2 as ec2,
    aws_ssm as ssm,
    Tags
)
from constructs import Construct
from aap_backend_cdk.environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')
ES_ENDPOINT = os.environ.get('ES_ENDPOINT', '')
ES_DOMAIN_NAME = os.environ.get('ES_DOMAIN_NAME', '')
ADMIN_USER_POOL_ID = os.environ.get('ADMIN_USER_POOL_ID', '')
# VPC_ID = os.environ.get('VPC_ID')
# PRIVATE_SUBNET_IDS = os.environ.get('PRIVATE_SUBNET_IDS').split(',')
# SECURITY_GROUP_ID = os.environ.get('SECURITY_GROUP_ID')

class AapBackendLambdaGeneralStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')
        
        lambda_dir = './lambda/Functions/'
        
        # VPC and Security Groups
        # AapVpcPrivateSubnets = ec2.Vpc.from_vpc_attributes(
        #     self, f'{PROJECT_NAME}' + 'VpcPrivateSubnets',
        #     availability_zones=['ap-southeast-5a'],
        #     vpc_id=VPC_ID,
        #     private_subnet_ids=PRIVATE_SUBNET_IDS,
        # )
        
        # AapSecurityGroup = ec2.SecurityGroup.from_security_group_id(
        #     self, f'{PROJECT_NAME}' + 'DefaultSecurityGroup',
        #     security_group_id=SECURITY_GROUP_ID
        # )
        
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

        # AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
        #     self, 'AwsPandasLayer',
        #     "arn:aws:lambda:ap-southeast-5:336392948345:layer:AWSSDKPandas-Python312:16"
        # )

        AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'AwsPandasLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'AwsPandasLayerArn', f'{PROJECT_NAME}' + '-AwsPandasLayerArn').string_value
        )

        OpenpyxlLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'OpenpyxlLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'OpenpyxlLayerArn', f'{PROJECT_NAME}' + '-OpenpyxlLayerArn').string_value
        )

        # Lambda Functions
        CreateDeleteElasticSearch = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-CreateDeleteElasticSearch',
            function_name=f'{PROJECT_NAME}' + '-CreateDeleteElasticSearch',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-CreateDeleteElasticSearch'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to create and delete elasticsearch cluster",
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_NAME': elasticsearchMap[env]['domainName'].format(PROJECT_NAME.lower()),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=128
        )
        
        CreateElasticSearchSnapshot = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-CreateElasticSearchSnapshot',
            function_name=f'{PROJECT_NAME}' + '-CreateElasticSearchSnapshot',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-CreateElasticSearchSnapshot'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to create elasticsearch snapshot",
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.title()),
                'S3_BUCKET': elasticsearchMap[env]['s3BackupBucket'].format(PROJECT_NAME.lower()),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=128
        )
        
        RestoreElasticSearchSnapshot = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-RestoreElasticSearchSnapshot',
            function_name=f'{PROJECT_NAME}' + '-RestoreElasticSearchSnapshot',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-RestoreElasticSearchSnapshot'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to restore elasticsearch snapshot",
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.title()),
                'S3_BUCKET': elasticsearchMap[env]['s3BackupBucket'].format(PROJECT_NAME.lower()),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=128
        )

        DeleteElasticSearchIndex = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-DeleteElasticSearchIndex',
            function_name=f'{PROJECT_NAME}' + '-DeleteElasticSearchIndex',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-DeleteElasticSearchIndex'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to delete elasticsearch index",
            role=AapLambdaRole,
            environment={
                'HOST': ES_ENDPOINT,
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=128
        )

        RedisHelper = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-RedisHelper',
            function_name=f'{PROJECT_NAME}' + '-RedisHelper',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-RedisHelper'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Helper Function to interact with cache",
            role=AapLambdaRole,
            environment={
                'REDIS_ENDPOINT': 'NA',
                'REDIS_PORT': 'NA',
                'ES_ENDPOINT': ES_ENDPOINT
            },
            # vpc=AapVpcPrivateSubnets,
            # security_groups=[AapSecurityGroup],
            # allow_public_subnet=True,
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=1024
        )

        # AdminExportUploadedDocumentsCSV
        AdminExportUploadedDocumentsCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExportUploadedDocumentsCSV',
            function_name=f'{PROJECT_NAME}' + '-AdminExportUploadedDocumentsCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExportUploadedDocumentsCSV'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export uploaded documents as CSV',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        # AdminExportExtractedDocumentCSV
        AdminExportExtractedDocumentCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExportExtractedDocumentCSV',
            function_name=f'{PROJECT_NAME}' + '-AdminExportExtractedDocumentCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExportExtractedDocumentCSV'),
            layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer, OpenpyxlLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export extracted itemList in document as CSV',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'BR_MERCHANT_ID': MERCHANT_ID_MAP[env]['BR'],
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        AdminExportExtractedLineItemsCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExportExtractedLineItemsCSV',
            function_name=f'{PROJECT_NAME}' + '-AdminExportExtractedLineItemsCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExportExtractedLineItemsCSV'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export extracted line items as CSV',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        AdminExportExtractedPoCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExportExtractedPoCSV',
            function_name=f'{PROJECT_NAME}' + '-AdminExportExtractedPoCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExportExtractedPoCSV'),
            layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer, OpenpyxlLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export extracted po as CSV',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        AdminExportExtractedPoLineItemsCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExportExtractedPoLineItemsCSV',
            function_name=f'{PROJECT_NAME}' + '-AdminExportExtractedPoLineItemsCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExportExtractedPoLineItemsCSV'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export extracted po line items as CSV',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )


        AdminExportReconciliationResultsCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExportReconciliationResultsCSV',
            function_name=f'{PROJECT_NAME}' + '-AdminExportReconciliationResultsCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExportReconciliationResultsCSV'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export reconciliation results as CSV',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        # AdminExport3WayMatchingResults
        AdminExport3WayMatchingResults = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExport3WayMatchingResults',
            function_name=f'{PROJECT_NAME}' + '-AdminExport3WayMatchingResults',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExport3WayMatchingResults'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export result of three way matching as CSV',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'THREE_WAY_MATCHING_RESULTS_TABLE': DynamoDBMap['THREE_WAY_MATCHING_RESULTS_TABLE'].format(PROJECT_NAME),
                'THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE': DynamoDBMap['THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        # UploadMappingFilesToDdb
        UploadMappingFilesToDdb = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-UploadMappingFilesToDdb',
            function_name=f'{PROJECT_NAME}' + '-UploadMappingFilesToDdb',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-UploadMappingFilesToDdb'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to upload supplier/supplier item mapping files to DynamoDB',
            role=AapLambdaRole,
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        CreateMerchant = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-CreateMerchant',
            function_name=f'{PROJECT_NAME}' + '-CreateMerchant',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-CreateMerchant'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to create merchant",
            role=AapLambdaRole,
            environment={
                'MERCHANT_TABLE': f"{PROJECT_NAME}-Merchant",
                'USER_TABLE': f"{PROJECT_NAME}-User",
                'USER_GROUP_TABLE': f"{PROJECT_NAME}-UserGroup",
                'USER_MATRIX_TABLE': f"{PROJECT_NAME}-UserMatrix",
                'COGNITO_USER_POOL': ADMIN_USER_POOL_ID,
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        ExportPresalesCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ExportPresalesCSV',
            function_name=f'{PROJECT_NAME}' + '-ExportPresalesCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ExportPresalesCSV'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to export presales data as CSV",
            role=AapLambdaRole,
            environment={
                'ROUTE_CONTENT_TABLE': DynamoDBMap['ROUTE_CONTENT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_EMAIL_TABLE': DynamoDBMap['EXTRACTED_EMAIL_TABLE'].format(PROJECT_NAME),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminExportFixedAssetAutomation = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminExportFixedAssetAutomation',
            function_name=f'{PROJECT_NAME}' + '-AdminExportFixedAssetAutomation',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminExportFixedAssetAutomation'),
            layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer, OpenpyxlLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description='Function to export Fixed Asset Card and Acquisition Journal',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'SMART_EYE_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'FIXED_ASSET_TABLE': DynamoDBMap['FIXED_ASSET_TABLE'].format(PROJECT_NAME),
                'ACQUISITION_JOURNAL_TABLE': DynamoDBMap['ACQUISITION_JOURNAL_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        
        # Eventbridge Schedule Rules
        DailyCreateResourceRule = events.Rule(
            self, 'DailyCreateResourceRule',
            description='Schedule Rule to Trigger Create Resources at 8.30am daily',
            rule_name=f'{PROJECT_NAME}-DailyCreateResourceRule',
            schedule=events.Schedule.cron(minute='30', hour='00'),
            targets=[
                targets.LambdaFunction(
                    CreateDeleteElasticSearch,
                    event=events.RuleTargetInput.from_object({'mode': 'create'})
                )
            ],
            enabled=True if env != 'prod' else False
        )
        
        DailyDeleteResourceRule = events.Rule(
            self, 'DailyDeleteResourceRule',
            description='Schedule Rule to Trigger Delete Resources at 11.00pm daily',
            rule_name=f'{PROJECT_NAME}-DailyDeleteResourceRule',
            schedule=events.Schedule.cron(minute='00', hour='15'),
            targets=[
                targets.LambdaFunction(
                    CreateDeleteElasticSearch,
                    event=events.RuleTargetInput.from_object({'mode': 'delete'})
                )
            ],
            enabled=True if env != 'prod' else False
        )
        
        DailyCreateEsSnapshotRule = events.Rule(
            self, 'DailyCreateEsSnapshotRule',
            description='Schedule Rule to Trigger Create ES Snapshot at 10.45pm daily',
            rule_name=f'{PROJECT_NAME}-DailyCreateEsSnapshotRule',
            schedule=events.Schedule.cron(minute='45', hour='14'),
            targets=[
                targets.LambdaFunction(
                    CreateElasticSearchSnapshot
                )
            ],
            enabled=True if env != 'prod' else False
        )
        
        DailyRestoreEsSnapshotRule = events.Rule(
            self, 'DailyRestoreEsSnapshotRule',
            description='Schedule Rule to Trigger Restore ES Snapshot at 9.00am daily',
            rule_name=f'{PROJECT_NAME}-DailyRestoreEsSnapshotRule',
            schedule=events.Schedule.cron(minute='00', hour='01'),
            targets=[
                targets.LambdaFunction(
                    RestoreElasticSearchSnapshot
                )
            ],
            enabled=True if env != 'prod' else False
        )
        