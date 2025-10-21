import os
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    Fn,
    CfnOutput,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    aws_lambda_event_sources as lambda_event_sources,
    aws_ssm as ssm,
    aws_s3_notifications as s3n,
    aws_s3_deployment as s3_deploy,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    Tags
)
from constructs import Construct
from aap_backend_cdk.environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')
ES_ENDPOINT = os.environ.get('ES_ENDPOINT', '')

class AapBackendLambdaS3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')

        s3_dir = './s3/'
        lambda_dir = './lambda/Functions/'

        # Lambda IAM Role
        AapLambdaRole = iam.Role.from_role_arn(
            self, f'{PROJECT_NAME}' + 'LambdaRoleMY',
            f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}LambdaRoleMY'
        )

        TextractAsyncRole = iam.Role.from_role_arn(
            self, f'{PROJECT_NAME}' + 'TextractAsyncRoleMY',
            f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}TextractAsyncRoleMY'
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

        QRCodeLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'QRCodeLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'QRCodeLayerArn', f'{PROJECT_NAME}' + '-QRCodeLayerArn').string_value
        )

        PillowLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'PillowLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'PillowLayerArn', f'{PROJECT_NAME}' + '-PillowLayerArn').string_value
        )
        
        AwsWranglerLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'AwsWranglerLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'AwsWranglerLayerArn', f'{PROJECT_NAME}' + '-AwsWranglerLayerArn').string_value
        )

        PyPDF2Layer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'PyPDF2Layer',
            ssm.StringParameter.from_string_parameter_name(self, 'PyPDF2LayerArn', f'{PROJECT_NAME}' + '-PyPDF2LayerArn').string_value
        )

        ExtractEmailMsgLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'ExtractEmailMsgLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'ExtractEmailMsgLayerArn', f'{PROJECT_NAME}' + '-ExtractEmailMsgLayerArn').string_value
        )

        AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'AwsPandasLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'AwsPandasLayerArn', f'{PROJECT_NAME}' + '-AwsPandasLayerArn').string_value
        )

        PillowPymupdfLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'PillowPymupdfLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'PillowPymupdfLayerArn', f'{PROJECT_NAME}' + '-PillowPymupdfLayerArn').string_value
        )

        # S3 Buckets
        SmartEyeDocumentsBucket = s3.Bucket(
            self, f'{PROJECT_NAME.lower()}' + '-smarteye-documents-bucket-{}-my'.format(env),
            bucket_name = f'{PROJECT_NAME.lower()}' + '-smarteye-documents-bucket-{}-my'.format(env),
            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.HEAD
                    ],
                    allowed_origins=[
                        '*'
                    ],
                    allowed_headers=[
                        '*'
                    ],
                    exposed_headers=[
                        'x-amz-server-side-encryption',
                        'x-amz-request-id',
                        'x-amz-id-2',
                        'ETag'
                    ]
                )
            ],
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=True
        )
        
        SmartEyeDocumentsBucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="GiveSESPermissionToWriteEmail",
                actions=["s3:PutObject"],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"{SmartEyeDocumentsBucket.bucket_arn}/*"
                ],
                principals=[iam.ServicePrincipal("ses.amazonaws.com")]
            )
        )
        
        SmartEyeDocumentsBucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowSSLRequestsOnly",
                actions=["s3:*"],
                effect=iam.Effect.DENY,
                resources=[
                    f"{SmartEyeDocumentsBucket.bucket_arn}",
                    f"{SmartEyeDocumentsBucket.bucket_arn}/*"
                ],
                conditions={
                    "Bool": {
                        "aws:SecureTransport": "false"
                    }
                },
                principals=[iam.AnyPrincipal()]
            )
        )

        # SNS Topic
        StartTextractMultiDocumentAsyncSNS = sns.Topic(
            self, 'StartTextractMultiDocumentAsyncSNS', 
            display_name="StartTextractMultiDocumentAsyncSNS-{}".format(env), 
            topic_name="StartTextractMultiDocumentAsyncSNS-{}".format(env),
        )

        EmailAttachmentTextractSNS = sns.Topic(
            self, 'EmailAttachmentTextractSNS', 
            display_name="EmailAttachmentTextractSNS-{}".format(env), 
            topic_name="EmailAttachmentTextractSNS-{}".format(env),
        )

        ## Lambda Functions
        StartCheckDocumentAnalysis = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-StartCheckDocumentAnalysis',
            function_name=f'{PROJECT_NAME}' + '-StartCheckDocumentAnalysis',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-StartCheckDocumentAnalysis'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to upload document to S3",
            role=AapLambdaRole,
            environment={
                'S3_BUCKET': SmartEyeDocumentsBucket.bucket_name
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        CreateTextractAsync = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-CreateTextractAsync',
            function_name=f'{PROJECT_NAME}' + '-CreateTextractAsync',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-CreateTextractAsync'),
            layers=[LambdaBaseLayer, GenericLayer, QRCodeLayer, PillowLayer],
            description="Function to upload document to S3",
            role=AapLambdaRole,
            environment={
                'S3_BUCKET': SmartEyeDocumentsBucket.bucket_name,
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'INBOX_MONITORING_TABLE': DynamoDBMap['INBOX_MONITORING_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        StartTextractMultiDocumentAsync = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-StartTextractMultiDocumentAsync',
            function_name=f'{PROJECT_NAME}' + '-StartTextractMultiDocumentAsync',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-StartTextractMultiDocumentAsync'),
            layers=[LambdaBaseLayer, GenericLayer, PyPDF2Layer],
            description="Function to start Textract Async for multi document",
            role=AapLambdaRole,
            environment={
                'SNS_TOPIC_ARN': StartTextractMultiDocumentAsyncSNS.topic_arn,
                'SNS_ROLE_ARN': f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}TextractAsyncRole',
                'ROOT_OUTPUT_PREFIX': 'textract_output',
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=128
        )

        ClassifyMultiDocument = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ClassifyMultiDocument',
            function_name=f'{PROJECT_NAME}' + '-ClassifyMultiDocument',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ClassifyMultiDocumentBedrock'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to classify multi document",
            role=AapLambdaRole,
            environment={
                'BEDROCK_ROLE': "arn:aws:iam::637423227750:role/BedrockCrossAccountRole",
                'DOCUMENT_SPLITTER_LAMBDA_ARN': f'arn:aws:lambda:ap-southeast-5:{ACCOUNT_ID}:function:{PROJECT_NAME}-DocumentSplitter',
                'INPUT_PREFIX': 'textract_output',
                'MODEL_ID': BedrockModel[env]['model-3.5']
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=128
        )

        StartTextractMultiDocumentAsyncSNS.grant_publish(ClassifyMultiDocument)

        ClassifyMultiDocument.add_event_source(
            lambda_event_sources.SnsEventSource(StartTextractMultiDocumentAsyncSNS)
        )

        DocumentSplitter = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-DocumentSplitter',
            function_name=f'{PROJECT_NAME}' + '-DocumentSplitter',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-DocumentSplitter'),
            layers=[LambdaBaseLayer, GenericLayer, PyPDF2Layer],
            description="Function to split document",
            role=AapLambdaRole,
            environment={
                'OUTPUT_PREFIX': 'input',
                'DESTINATION_BUCKET': SmartEyeDocumentsBucket.bucket_name,
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=128
        )

        ProcessS3ReconciliationDocument = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessS3ReconciliationDocument',
            function_name=f'{PROJECT_NAME}' + '-ProcessS3ReconciliationDocument',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ProcessS3ReconciliationDocument'),
            layers=[LambdaBaseLayer, GenericLayer, AwsWranglerLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description="Function to upload document to S3",
            role=AapLambdaRole,
            environment={
                'SALES_STATEMENT_TABLE': DynamoDBMap['SALES_ENTRY_TABLE'].format(PROJECT_NAME),
                'BANK_STATEMENT_TABLE': DynamoDBMap['BANK_TRANSACTION_TABLE'].format(PROJECT_NAME),
                'PAYMENT_GATEWAY_REPORT_TABLE': DynamoDBMap['PAYMENT_GATEWAY_TABLE'].format(PROJECT_NAME),
                'PAYMENT_TRANSACTION_TABLE': DynamoDBMap['PAYMENT_TRANSACTION_TABLE'].format(PROJECT_NAME),
                'ODOO_PAYMENT_TABLE': DynamoDBMap['PAYMENT_REPORT_ERP_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'STORE_TABLE': DynamoDBMap['STORE_TABLE'].format(PROJECT_NAME),
                'S3_BUCKET_NAME': SmartEyeDocumentsBucket.bucket_name,
                'ES_DOMAIN_ENDPOINT': elasticsearchMap[env]['endpoint'],
                'SQS_QUEUE_URL': '',
                'CREATE_DOCUMENT_SQS_QUEUE_URL': SqsMap[env]['CreateDocumentQueue'].format(ACCOUNT_ID),
                'GLUE_JOB_NAME': glueMap[env]['processS3ReconcilationDocumentJob'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        PresalesEmailProcessing = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-PresalesEmailProcessing',
            function_name=f'{PROJECT_NAME}' + '-PresalesEmailProcessing',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-PresalesEmailProcessing'),
            layers=[LambdaBaseLayer, GenericLayer, ExtractEmailMsgLayer, PyPDF2Layer, AwsPandasLayer],
            # layers=[LambdaBaseLayer, GenericLayer, ExtractEmailMsgLayer, PyPDF2Layer],            
            description="Function to extract email",
            role=AapLambdaRole,
            environment={
                'EXTRACTED_ATTACHMENT_PREFIX': 'presales/output/extracted_attachment',
                'EXTRACTED_EMAIL_TABLE': DynamoDBMap['EXTRACTED_EMAIL_TABLE'].format(PROJECT_NAME),
                'MERCHANT_ID': MERCHANT_ID_MAP[env]['VSTECS'],
                'MODEL_ID': BedrockModel[env]['model-3.7'],
                'ROOT_OUTPUT_PREFIX': 'presales/output/textract_output',
                'ROUTED_CONTENT_TABLE': DynamoDBMap['ROUTE_CONTENT_TABLE'].format(PROJECT_NAME),
                'S3_BUCKET': SmartEyeDocumentsBucket.bucket_name,
                'SKILL_MATRIX_TABLE': DynamoDBMap['SKILL_MATRIX_TABLE'].format(PROJECT_NAME),
                'SNS_TOPIC_ARN': EmailAttachmentTextractSNS.topic_arn,
                'SNS_ROLE_ARN': f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}TextractAsyncRole',
                'SQS_ANALYSIS_QUEUE_URL': SqsMap[env]['PresalesEmailAnalysisQueue'].format(ACCOUNT_ID),
                'STEP_FUNCTION_ARN': stepFunctionMap[env]['EmailAttachmentTextractStepFunctionArn'].format(ACCOUNT_ID),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        FixedAssetAutomation = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-FixedAssetAutomation',
            function_name=f'{PROJECT_NAME}' + '-FixedAssetAutomation',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-FixedAssetAutomation'),
            layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description="Function to Automate Fixed Asset Processing",
            role=AapLambdaRole,
            environment={
                'SMART_EYE_BUCKET': SmartEyeDocumentsBucket.bucket_name,
                'FIXED_ASSET_TABLE': DynamoDBMap['FIXED_ASSET_TABLE'].format(PROJECT_NAME),
                'ACQUISITION_JOURNAL_TABLE': DynamoDBMap['ACQUISITION_JOURNAL_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'SEQUENCE_NUMBER_GENERATOR_TABLE': DynamoDBMap['SEQUENCE_NUMBER_GENERATOR_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'AGENT_MAPPING_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
                'MODEL_ID': BedrockModel[env]['model-3.7'],
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ConverseDocumentExtractionFM = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ConverseDocumentExtractionFM',
            function_name=f'{PROJECT_NAME}' + '-ConverseDocumentExtractionFM',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ConverseDocumentExtractionFM'),
            layers=[LambdaBaseLayer, GenericLayer, PillowPymupdfLayer],
            description='Function to export Fixed Asset Card and Acquisition Journal',
            role=AapLambdaRole,
            environment={
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'SQS_QUEUE': SqsMap[env]['ProcessConverseExtractionOutputQueue'].format(ACCOUNT_ID),
                'ES_S3ACCESS_ROLE': elasticsearchMap[env]['s3BackupRole'].format(ACCOUNT_ID, PROJECT_NAME.lower()),
                'S3_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=1024
        )

        ConverseDocumentExtractionBR = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ConverseDocumentExtractionBR',
            function_name=f'{PROJECT_NAME}' + '-ConverseDocumentExtractionBR',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ConverseDocumentExtractionBR'),
            layers=[LambdaBaseLayer, GenericLayer, PillowPymupdfLayer],
            description='Function to export BR AP Invoice documents with Converse API',
            role=AapLambdaRole,
            environment={
                'SQS_QUEUE': SqsMap[env]['ProcessConverseExtractionOutputQueueBR'].format(ACCOUNT_ID),
                'S3_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'CLAUDE_MODEL_ID': BedrockModel[env]['claude-4'],
                'NOVA_MODEL_ID': BedrockModel[env]['nova-pro'],
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )
        
        
        # ## Lambda Event Source Mapping
        # StartCheckDocumentAnalysis.add_event_source(
        #     lambda_event_sources.S3EventSource(
        #         SmartEyeDocumentsBucket,
        #         events=[s3.EventType.OBJECT_CREATED],
        #         filters=[s3.NotificationKeyFilter(prefix="input/")]
        #     )
        # )

        ConverseDocumentExtractionFM.add_event_source(
            lambda_event_sources.S3EventSource(
                SmartEyeDocumentsBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="input/{}".format(MERCHANT_ID_MAP[env]['FM']))]
            )
        )

        ConverseDocumentExtractionBR.add_event_source(
            lambda_event_sources.S3EventSource(
                SmartEyeDocumentsBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="input/{}".format(MERCHANT_ID_MAP[env]['BR']))]
            )
        )

        CreateTextractAsync.add_event_source(
            lambda_event_sources.S3EventSource(
                SmartEyeDocumentsBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="email/")]
            )
        )

        # ARReconciliation.add_event_source(
        #     lambda_event_sources.S3EventSource(
        #         SmartEyeDocumentsBucket,
        #         events=[s3.EventType.OBJECT_CREATED],
        #         filters=[s3.NotificationKeyFilter(prefix=f"reconciliation/input/{MERCHANT_ID_MAP[env]['GENTING']}/bank-statement/")]
        #     )
        # )

        ProcessS3ReconciliationDocument.add_event_source(
            lambda_event_sources.S3EventSource(
                SmartEyeDocumentsBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix=f"reconciliation/input/")]
            )
        )

        StartTextractMultiDocumentAsync.add_event_source(
            lambda_event_sources.S3EventSource(
                SmartEyeDocumentsBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="email_document/")]
            )
        )

        PresalesEmailProcessing.add_event_source(
            lambda_event_sources.S3EventSource(
                SmartEyeDocumentsBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="presales/input/{}/email/".format(MERCHANT_ID_MAP[env]['VSTECS']))]
            )
        )

        FixedAssetAutomation.add_event_source(
            lambda_event_sources.S3EventSource(
                SmartEyeDocumentsBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="fixed_asset/upload/{}/".format(MERCHANT_ID_MAP[env]['FM']))]
            )
        )


        CfnOutput(
            self, 'EmailAttachmentTextractSNSArn',
            value=EmailAttachmentTextractSNS.topic_arn,
            description='ARN of the Email Attachment Textract SNS Topic',
            export_name='EmailAttachmentTextractSNSArn'
        )

        
