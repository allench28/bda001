import os
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_iam as iam,
    aws_ssm as ssm,
    aws_sqs as sqs,
    Tags
)
from constructs import Construct
from aap_backend_cdk.environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')
DEFAULT_PROMPT_ID = os.environ.get('DEFAULT_PROMPT_ID', '')
MODEL_ID = os.environ.get('MODEL_ID', '')
REGION_NAME = os.environ.get('REGION_NAME', '')
BEDROCK_ROLE_ARN = os.environ.get('BEDROCK_ROLE_ARN', '')
ES_ENDPOINT = os.environ.get('ES_ENDPOINT', '')

class AapBackendLambdaSqsStack(Stack):

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

        # Lambda Layers
        LambdaBaseLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'LambdaBaseLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'LambdaBaseLayerArn', f'{PROJECT_NAME}' + '-LambdaBaseLayerArn').string_value
        )

        GenericLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'GenericLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'GenericLayerArn', f'{PROJECT_NAME}' + '-GenericLayerArn').string_value
        )

        AwsWranglerLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'AwsWranglerLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'AwsWranglerLayerArn', f'{PROJECT_NAME}' + '-AwsWranglerLayerArn').string_value
        )

        # AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
        #     self, 'AwsPandasLayer',
        #     "arn:aws:lambda:ap-southeast-5:336392948345:layer:AWSSDKPandas-Python312:16"
        # )

        AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'AwsPandasLayer',
            ssm.StringParameter.from_string_parameter_name(self, 'AwsPandasLayerArn', f'{PROJECT_NAME}' + '-AwsPandasLayerArn').string_value
        )

        # SQS definition
        ProcessDocumentDataQueueDLQ = sqs.Queue(
            self, 'ProcessDocumentDataQueueDLQ',
            queue_name='ProcessDocumentDataQueueDLQ',
        )
        
        ProcessDocumentDataQueue = sqs.Queue(
            self, 'ProcessDocumentDataQueue',
            queue_name='ProcessDocumentDataQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ProcessDocumentDataQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        # CreateDocument Queue and DLQ
        CreateDocumentQueueDLQ = sqs.Queue(
            self, 'CreateDocumentQueueDLQ',
            queue_name='CreateDocumentQueueDLQ',
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(300),
            receive_message_wait_time=Duration.seconds(20),
            delivery_delay=Duration.seconds(20),
            max_message_size_bytes=262144  # 256KB
        )

        CreateDocumentQueue = sqs.Queue(
            self, 'CreateDocumentQueue',
            queue_name='CreateDocumentQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=10, queue=CreateDocumentQueueDLQ),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(300),
            receive_message_wait_time=Duration.seconds(20),
            delivery_delay=Duration.seconds(20),
            max_message_size_bytes=262144  # 256KB
        )

        ExtractDocumentQueueDLQ = sqs.Queue(
            self, 'ExtractDocumentQueueDLQ',
            queue_name='ExtractDocumentQueueDLQ',
        )

        ExtractDocumentQueue = sqs.Queue(
            self, 'ExtractDocumentQueue',
            queue_name='ExtractDocumentQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ExtractDocumentQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        AnalyzeThreeWayMatchingQueueDLQ = sqs.Queue(
            self, 'AnalyzeThreeWayMatchingQueueDLQ',
            queue_name='AnalyzeThreeWayMatchingQueueDLQ',
        )

        AnalyzeThreeWayMatchingQueue = sqs.Queue(
            self, 'AnalyzeThreeWayMatchingQueue',
            queue_name='AnalyzeThreeWayMatchingQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=AnalyzeThreeWayMatchingQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        N8nErpQueueDLQ = sqs.Queue(
            self, 'N8nErpQueueDLQ',
            queue_name='N8nErpQueueDLQ',
        )

        N8nErpQueue = sqs.Queue(
            self, 'N8nErpQueue',
            queue_name='N8nErpQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=N8nErpQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        TriggerReconciliationQueueDLQ = sqs.Queue(
            self, 'TriggerReconciliationQueueDLQ',
            queue_name='TriggerReconciliationQueueDLQ',
        )

        TriggerReconciliationQueue = sqs.Queue(
            self, 'TriggerReconciliationQueue',
            queue_name='TriggerReconciliationQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=TriggerReconciliationQueueDLQ),
            visibility_timeout=Duration.minutes(15),
            delivery_delay=Duration.seconds(15),
        )

        AnalyzeReconciliationResultQueueDLQ = sqs.Queue(
            self, 'AnalyzeReconciliationResultQueueDLQ',
            queue_name='AnalyzeReconciliationResultQueueDLQ',
        )

        AnalyzeReconciliationResultQueue = sqs.Queue(
            self, 'AnalyzeReconciliationResultQueue',
            queue_name='AnalyzeReconciliationResultQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=AnalyzeReconciliationResultQueueDLQ),
            visibility_timeout=Duration.minutes(15),
            delivery_delay=Duration.seconds(2),
            receive_message_wait_time=Duration.seconds(20)
        )

        RouteContentQueueDLQ = sqs.Queue(
            self, 'RouteContentQueueDLQ',
            queue_name='RouteContentQueueDLQ',
        )

        RouteContentQueue = sqs.Queue(
            self, 'RouteContentQueue',
            queue_name='RouteContentQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=RouteContentQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        ProcessConverseExtractionOutputQueueDLQ = sqs.Queue(
            self, 'ProcessConverseExtractionOutputQueueDLQ',
            queue_name='ProcessConverseExtractionOutputQueueDLQ',
        )
        
        ProcessConverseExtractionOutputQueue = sqs.Queue(
            self, 'ProcessConverseExtractionOutputQueue',
            queue_name='ProcessConverseExtractionOutputQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ProcessConverseExtractionOutputQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        ProcessConverseExtractionOutputQueueBRDLQ = sqs.Queue(
            self, 'ProcessConverseExtractionOutputQueueBRDLQ',
            queue_name='ProcessConverseExtractionOutputQueueBRDLQ',
        )
        
        ProcessConverseExtractionOutputQueueBR = sqs.Queue(
            self, 'ProcessConverseExtractionOutputQueueBR',
            queue_name='ProcessConverseExtractionOutputQueueBR',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ProcessConverseExtractionOutputQueueBRDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        ## Lambda Functions
        ProcessDocumentDataSQS = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessDocumentDataSQS',
            function_name=f'{PROJECT_NAME}' + '-ProcessDocumentDataSQS',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ProcessDocumentDataSQS'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to upload document to S3",
            role=AapLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': 'AgentConfigurations',
                'AGENT_MAPPINGS_BUCKET_NAME': '{}-agent-config-mappings-{}'.format(PROJECT_NAME.lower(), env),
                'BEDROCK_ROLE_ARN': BEDROCK_ROLE_ARN,
                'CONVERT_TO_CSV_QUEUE': ExtractDocumentQueue.queue_url,
                'DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN': 'arn:aws:bedrock:ap-southeast-5:{}:prompt/{}'.format(ACCOUNT_ID, DEFAULT_PROMPT_ID),
                'EMAIL_NOTIFICATION_LAMBDA_ARN': 'arn:aws:lambda:ap-southeast-5:{}:function:{}-NotificationToEmailRecipients'.format(ACCOUNT_ID, PROJECT_NAME.title()),
                'EXTRACTED_DOCUMENT_TABLE': 'ExtractedDocuments',
                'MODEL_ID': MODEL_ID,
                'PRIMARY_KEY': 'extractedDocumentId',
                'REGION': REGION_NAME,
                'S3_BUCKET_NAME': '{}-smarteye-documents-bucket-{}'.format(PROJECT_NAME.lower(), env),
                'USE_TEXTRACT': 'true'

            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ExtractedDocumentToCsvSQS = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ExtractedDocumentToCsvSQS',
            function_name=f'{PROJECT_NAME}' + '-ExtractedDocumentToCsvSQS',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ExtractedDocumentToCsvSQS'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to upload document to S3",
            role=AapLambdaRole,
            environment={
                'AAP_SMART_EYE_BUCKET_NAME': '{}-smarteye-documents-bucket-{}'.format(PROJECT_NAME.lower(), env),
                'EXTRACTED_DOCUMENT_TABLE': 'ExtractedDocuments',
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        Analyze3WayMatchingResultsSQS = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-Analyze3WayMatchingResultsSQS',
            function_name=f'{PROJECT_NAME}' + '-Analyze3WayMatchingResultsSQS',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-Analyze3WayMatchingResultsSQS'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to perform bedrock analysis on 3 way matching results",
            role=AapLambdaRole,
            environment={
                'MODEL_ID': BedrockModel[env]['model-3.7'],
                'THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE': DynamoDBMap['THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE'].format(PROJECT_NAME),
                'THREE_WAY_MATCHING_RESULTS_TABLE': DynamoDBMap['THREE_WAY_MATCHING_RESULTS_TABLE'].format(PROJECT_NAME),
                'SQS_QUEUE_URL': SqsMap[env]['3WayMatchingResultsSQS'].format(ACCOUNT_ID),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'JOB_TRACKING_TABLE': DynamoDBMap['JOB_TRACKING_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'AGENT_MAPPING_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        TriggerN8nWorkflowSQS = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-TriggerN8nWorkflowSQS',
            function_name=f'{PROJECT_NAME}' + '-TriggerN8nWorkflowSQS',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-TriggerN8nWorkflowSQS'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to trigger n8n workflow to send document to ERP",
            role=AapLambdaRole,
            environment={
                'N8N_INVOICE_WEBHOOK_URL': N8nMap['invoice'],
                'N8N_PO_WEBHOOK_URL': N8nMap['po'],
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'SMART_EYE_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ARReconciliation = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ARReconciliation',
            function_name=f'{PROJECT_NAME}' + '-ARReconciliation',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ARReconciliation'),
            layers=[LambdaBaseLayer, GenericLayer, AwsWranglerLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description="Function to upload document to S3",
            role=AapLambdaRole,
            reserved_concurrent_executions=5,
            environment={
                'SALES_STATEMENT_TABLE': DynamoDBMap['SALES_ENTRY_TABLE'].format(PROJECT_NAME),
                'BANK_STATEMENT_TABLE': DynamoDBMap['BANK_TRANSACTION_TABLE'].format(PROJECT_NAME),
                'PAYMENT_GATEWAY_REPORT_TABLE': DynamoDBMap['PAYMENT_GATEWAY_TABLE'].format(PROJECT_NAME),
                'PAYMENT_TRANSACTION_TABLE': DynamoDBMap['PAYMENT_TRANSACTION_TABLE'].format(PROJECT_NAME),
                'ODOO_PAYMENT_TABLE': DynamoDBMap['PAYMENT_REPORT_ERP_TABLE'].format(PROJECT_NAME),
                'STORE_TABLE': DynamoDBMap['STORE_TABLE'].format(PROJECT_NAME),
                'S3_BUCKET_NAME': '{}-smarteye-documents-bucket-{}'.format(PROJECT_NAME.lower(), env),
                'RECONCILIATION_RESULTS_TABLE': DynamoDBMap['RECONCILIATION_RESULTS_TABLE'].format(PROJECT_NAME),
                'SQS_QUEUE_URL': AnalyzeReconciliationResultQueue.queue_url,
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'JOB_TRACKING_TABLE': DynamoDBMap['JOB_TRACKING_TABLE'].format(PROJECT_NAME),
                'ANALYZE_RECONCILIATION_QUEUE_URL': TriggerReconciliationQueue.queue_url,
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        AnalyzeARReconciliationResultsSQS = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AnalyzeARReconciliationResults',
            function_name=f'{PROJECT_NAME}' + '-AnalyzeARReconciliationResults',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AnalyzeARReconciliationResults'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to perform bedrock analysis on AR reconciliation results",
            role=AapLambdaRole,
            environment={
                'RECONCILIATION_RESULTS_TABLE': DynamoDBMap['RECONCILIATION_RESULTS_TABLE'].format(PROJECT_NAME),
                'MODEL_ID': "anthropic.claude-3-5-sonnet-20240620-v1:0",
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        RouteContent = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-RouteContent',
            function_name=f'{PROJECT_NAME}' + '-RouteContent',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-RouteContent'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to route content to S3",
            role=AapLambdaRole,
            environment={
                'ROUTE_CONTENT_TABLE': DynamoDBMap['ROUTE_CONTENT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_EMAIL_TABLE': DynamoDBMap['EXTRACTED_EMAIL_TABLE'].format(PROJECT_NAME),
                'SKILL_MATRIX_TABLE': DynamoDBMap['SKILL_MATRIX_TABLE'].format(PROJECT_NAME),
                # 'SMART_EYE_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
                'MODEL_ID': BedrockModel[env]['model-3.7'],
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )


        CreateDocumentFromSQS = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-CreateDocumentFromSQS',
            function_name=f'{PROJECT_NAME}' + '-CreateDocumentFromSQS',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-CreateDocumentFromSQS'),
            layers=[LambdaBaseLayer, GenericLayer, AwsWranglerLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description="Function to create document from SQS",
            role=AapLambdaRole,
            environment={
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
            },
            reserved_concurrent_executions=10,
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ProcessConverseExtractionOutputFM = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessConverseExtractionOutputFM',
            function_name=f'{PROJECT_NAME}' + '-ProcessConverseExtractionOutputFM',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ProcessConverseExtractionOutputFM'),
            layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description="Function to process the extraction result from BDA",
            role=AapLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENT_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'AGENT_MAPPING_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
                'MODEL_ID': BedrockModel[env]['model-3.7'],
                'N8N_SQS_QUEUE': SqsMap[env]['N8N_SQS_QUEUE'],
                'SMARTEYE_DOCUMENTS_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'SEQUENCE_NUMBER_GENERATOR_TABLE': DynamoDBMap['SEQUENCE_NUMBER_GENERATOR_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN_ENDPOINT': ES_ENDPOINT,
                'SUPPLIER_INDEX': elasticsearchMap[env]['SUPPLIER_INDEX'],
                'LINE_ITEM_INDEX': elasticsearchMap[env]['LINE_ITEM_INDEX'],
                'STORE_INDEX': elasticsearchMap[env]['STORE_INDEX'],
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ProcessConverseExtractionOutputBR = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessConverseExtractionOutputBR',
            function_name=f'{PROJECT_NAME}' + '-ProcessConverseExtractionOutputBR',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ProcessConverseExtractionOutputBR'),
            layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer],
            # layers=[LambdaBaseLayer, GenericLayer],
            description="Function to process the extraction from AAP-ConverseDocumentExtractionBR",
            role=AapLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'AGENT_MAPPING_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENT_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'MODEL_ID': BedrockModel[env]['model-3.7'],
                'SMARTEYE_DOCUMENTS_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'SUPPLIER_TABLE': DynamoDBMap['SUPPLIER_TABLE'].format(PROJECT_NAME),
                'SUPPLIER_ITEM_TABLE': DynamoDBMap['SUPPLIER_ITEM_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512,
            reserved_concurrent_executions=5
        )

        # SQS Trigger
        ProcessDocumentDataSQS.add_event_source(
            lambda_event_sources.SqsEventSource(
                ProcessDocumentDataQueue,
                batch_size=10,
                enabled=True
            )
        )

        ExtractedDocumentToCsvSQS.add_event_source(
            lambda_event_sources.SqsEventSource(
                ExtractDocumentQueue,
                batch_size=10,
                enabled=True
            )
        )

        Analyze3WayMatchingResultsSQS.add_event_source(
            lambda_event_sources.SqsEventSource(
                AnalyzeThreeWayMatchingQueue,
                batch_size=10,
                enabled=True
            )
        )

        TriggerN8nWorkflowSQS.add_event_source(
            lambda_event_sources.SqsEventSource(
                N8nErpQueue,
                batch_size=10,
                enabled=True
            )
        )

        ARReconciliation.add_event_source(
            lambda_event_sources.SqsEventSource(
                TriggerReconciliationQueue,
                batch_size=1,
                enabled=True
            )
        )

        AnalyzeARReconciliationResultsSQS.add_event_source(
            lambda_event_sources.SqsEventSource(
                AnalyzeReconciliationResultQueue,
                batch_size=3,
                max_batching_window=Duration.seconds(10),
                report_batch_item_failures=True,
                enabled=True
            )
        )
        
        RouteContent.add_event_source(
            lambda_event_sources.SqsEventSource(
                RouteContentQueue,
                batch_size=10,
                enabled=True
            )
        )

        CreateDocumentFromSQS.add_event_source(
            lambda_event_sources.SqsEventSource(
                CreateDocumentQueue,
                batch_size=1,
                max_concurrency=10,
                enabled=True
            )
        )

        ProcessConverseExtractionOutputFM.add_event_source(
            lambda_event_sources.SqsEventSource(
                ProcessConverseExtractionOutputQueue,
                batch_size=10,
                enabled=True
            )
        )  

        ProcessConverseExtractionOutputBR.add_event_source(
            lambda_event_sources.SqsEventSource(
                ProcessConverseExtractionOutputQueueBR,
                batch_size=1,
                enabled=True
            )
        )         
        

        
