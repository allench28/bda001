import os
import subprocess

from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    Fn,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    aws_lambda_event_sources as lambda_event_sources,
    aws_ssm as ssm,
    aws_s3_notifications as s3n,
    aws_s3_deployment as s3_deploy,
    aws_sqs as sqs,
    Tags,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as stepfunctions_tasks,
)
from constructs import Construct
from aap_backend_cdk.environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')
BDA_REGION = os.environ.get('BDA_REGION', 'us-east-1')

class AapBackendBDALambdaStack(Stack):

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
        
        # # Lambda Layers
        # AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
        #     self, 'AwsPandasLayer',
        #     "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
        # )

        LambdaBaseLayer = lambda_.LayerVersion(
            self, 'BDALambdaBase',
            layer_version_name='{}-BDALambdaBase'.format(PROJECT_NAME.title()),
            code=self.create_dependencies_layer('./lambda/Layers/BDALambdaBase'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer with AwsLambdaPowerTools, SimpleJson, Requests, aws4auth and elasticsearch Dependency",
            removal_policy=RemovalPolicy.RETAIN
        )

        BDAGenericLayer = lambda_.LayerVersion(
            self, 'BDAGenericLayer',
            layer_version_name='{}-BDAGenericLayer'.format(PROJECT_NAME.title()),
            code=lambda_.Code.from_asset('./lambda/Layers/Generic'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer for common code",
            removal_policy=RemovalPolicy.RETAIN
        )
        
        # SQS Queue
        ProcessBDAExtractionOutputQueueDLQ = sqs.Queue(
            self, 'ProcessBDAExtractionOutputQueueDLQ',
            queue_name='ProcessBDAExtractionOutputQueueDLQ',
        )
        
        ProcessBDAExtractionOutputQueue = sqs.Queue(
            self, 'ProcessBDAExtractionOutputQueue',
            queue_name='ProcessBDAExtractionOutputQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ProcessBDAExtractionOutputQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        ProcessBDAExtractionOutputGRNDLQ = sqs.Queue(
            self, 'ProcessBDAExtractionOutputGRNDLQ',
            queue_name='ProcessBDAExtractionOutputGRNDLQ',
        )

        ProcessBDAExtractionOutputGRNQueue = sqs.Queue(
            self, 'ProcessBDAExtractionOutputGRNQueue',
            queue_name='ProcessBDAExtractionOutputGRNQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ProcessBDAExtractionOutputGRNDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        ProcessBDAExtractionOutputPODLQ = sqs.Queue(
            self, 'ProcessBDAExtractionOutputPODLQ',
            queue_name='ProcessBDAExtractionOutputPODLQ',
        )

        ProcessBDAExtractionOutputPOQueue = sqs.Queue(
            self, 'ProcessBDAExtractionOutputPOQueue',
            queue_name='ProcessBDAExtractionOutputPOQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ProcessBDAExtractionOutputPODLQ),
            visibility_timeout=Duration.minutes(15)
        )

        BrProcessBDAExtractionOutputDLQ = sqs.Queue(
            self, 'BrProcessBDAExtractionOutputDLQ',
            queue_name='BrProcessBDAExtractionOutputDLQ',
        )

        BrProcessBDAExtractionOutputQueue = sqs.Queue(
            self, 'BrProcessBDAExtractionOutputQueue',
            queue_name='BrProcessBDAExtractionOutputQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=BrProcessBDAExtractionOutputDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        ProcessBDAExtractionOutputMedRefLetterQueueDLQ = sqs.Queue(
            self, 'ProcessBDAExtractionOutputMedRefLetterQueueDLQ',
            queue_name='ProcessBDAExtractionOutputMedRefLetterQueueDLQ',
        )
        
        ProcessBDAExtractionOutputMedRefLetterQueue = sqs.Queue(
            self, 'ProcessBDAExtractionOutputMedRefLetterQueue',
            queue_name='ProcessBDAExtractionOutputMedRefLetterQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=ProcessBDAExtractionOutputMedRefLetterQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        # S3 Buckets
        BDAProcessingBucket = s3.Bucket(
            self, f'{PROJECT_NAME.lower()}' + '-bda-processing-{}'.format(env),
            bucket_name = f'{PROJECT_NAME.lower()}' + '-bda-processing-{}'.format(env),
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
        
        BDAProcessingBucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowSSLRequestsOnly",
                actions=["s3:*"],
                effect=iam.Effect.DENY,
                resources=[
                    f"{BDAProcessingBucket.bucket_arn}",
                    f"{BDAProcessingBucket.bucket_arn}/*"
                ],
                conditions={
                    "Bool": {
                        "aws:SecureTransport": "false"
                    }
                },
                principals=[iam.AnyPrincipal()]
            )
        )

        ## Lambda Functions
        StartDocumentExtractionBDA = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-StartDocumentExtractionBDA',
            function_name=f'{PROJECT_NAME}' + '-StartDocumentExtractionBDA',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-StartDocumentExtractionBDA'),
            layers=[LambdaBaseLayer, BDAGenericLayer],
            description="Function to initiate document extraction using Bedrock Data Automation (BDA)",
            role=AapLambdaRole,
            environment={
                'BDA_RUNTIME_ENDPOINT': f'https://bedrock-data-automation-runtime.{BDA_REGION}.amazonaws.com',
                'OUTPUT_BUCKET': BDAProcessingBucket.bucket_name,
                'DATA_AUTOMATION_PROJECT_ARN': f'arn:aws:bedrock:{BDA_REGION}:{ACCOUNT_ID}:data-automation-project',
                'DATA_AUTOMATION_PROFILE_ARN': f'arn:aws:bedrock:{BDA_REGION}:{ACCOUNT_ID}:data-automation-profile',
                'SQS_QUEUE': ProcessBDAExtractionOutputQueue.queue_url,
                'GRN_SQS_QUEUE': ProcessBDAExtractionOutputGRNQueue.queue_url,
                'PO_SQS_QUEUE': ProcessBDAExtractionOutputPOQueue.queue_url,
                'BR_SQS_QUEUE': BrProcessBDAExtractionOutputQueue.queue_url,
                'MEDICAL_REFERRAL_LETTER_SQS_QUEUE': ProcessBDAExtractionOutputMedRefLetterQueue.queue_url, #for Asia1Health
                'BDA_CONFIGURATION_TABLE': f'{PROJECT_NAME}-BDAConfiguration',
                'TIMELINE_TABLE': f'{PROJECT_NAME}-Timeline',
                'EXTRACTED_DOCUMENTS_TABLE': f'{PROJECT_NAME}-ExtractedDocuments',
                'EXTRACTED_GRN_TABLE': f'{PROJECT_NAME}-ExtractedGrn',
                'EXTRACTED_PO_TABLE': f'{PROJECT_NAME}-ExtractedPo',
                'DOCUMENT_UPLOAD_TABLE': f'{PROJECT_NAME}-DocumentUpload',
                'N8N_SQS_QUEUE': SqsMap[env]['N8N_SQS_QUEUE'],
                'BR_MERCHANT_ID': MERCHANT_ID_MAP[env]['BR'],
                'STEP_FUNCTION_ARN': stepFunctionMap[env]['retryBDAPollingStepFunctionArn'].format(ACCOUNT_ID),
                'FM_MERCHANT_ID': MERCHANT_ID_MAP[env]['FM']
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=2048,
            recursive_loop=lambda_.RecursiveLoop.ALLOW
        )

        ProcessBDAExtractionOutput = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutput',
            function_name=f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutput',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ProcessBDAExtractionOutput'),
            # layers=[LambdaBaseLayer, BDAGenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, BDAGenericLayer],
            description="Function to process the extraction result from BDA",
            role=AapLambdaRole,
            environment={
                'BDA_PROCESSING_BUCKET': BDAProcessingBucket.bucket_name,
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
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        BrProcessBDAExtractionOutput = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-BrProcessBDAExtractionOutput',
            function_name=f'{PROJECT_NAME}' + '-BrProcessBDAExtractionOutput',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-BrProcessBDAExtractionOutput'),
            # layers=[LambdaBaseLayer, BDAGenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, BDAGenericLayer],
            description="Function to process the extraction result from BDA for Baskin Robin merchant",
            role=AapLambdaRole,
            environment={
                'BDA_PROCESSING_BUCKET': BDAProcessingBucket.bucket_name,
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENT_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'AGENT_MAPPING_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
                'MODEL_ID': BedrockModel[env]['model-3.7'],
                'N8N_SQS_QUEUE': SqsMap[env]['N8N_SQS_QUEUE'],
                'SMARTEYE_DOCUMENTS_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'SUPPLIER_TABLE': DynamoDBMap['SUPPLIER_TABLE'].format(PROJECT_NAME),
                'SUPPLIER_ITEM_TABLE': DynamoDBMap['SUPPLIER_ITEM_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ProcessBDAExtractionOutputGRN = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutputGRN',
            function_name=f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutputGRN',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ProcessBDAExtractionOutputGRN'),
            # layers=[LambdaBaseLayer, BDAGenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, BDAGenericLayer],
            # need to add pandas layer
            description="Function to process the GRN extraction result from BDA",
            role=AapLambdaRole,
            environment={
                'BDA_PROCESSING_BUCKET': BDAProcessingBucket.bucket_name,
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_TABLE': DynamoDBMap['EXTRACTED_GRN_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_GRN_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'SMARTEYE_DOCUMENTS_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'AGENT_MAPPING_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
                'MODEL_ID': BedrockModel[env]['model-3.5']
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ProcessBDAExtractionOutputPO = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutputPO',
            function_name=f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutputPO',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ProcessBDAExtractionOutputPO'),
            # layers=[LambdaBaseLayer, BDAGenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, BDAGenericLayer],
            # need to add pandas layer
            description="Function to process the PO extraction result from BDA",
            role=AapLambdaRole,
            environment={
                'BDA_PROCESSING_BUCKET': BDAProcessingBucket.bucket_name,
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'SMARTEYE_DOCUMENTS_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'AGENT_MAPPING_BUCKET': s3Map[env]['AGENT_MAPPING_BUCKET'].format(PROJECT_NAME.lower(), env),
                'MODEL_ID': BedrockModel[env]['model-3.5']
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        ProcessBDAExtractionOutputMedRefLetter = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutputMedRefLetter',
            function_name=f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutputMedRefLetter',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + f'{PROJECT_NAME}' + '-ProcessBDAExtractionOutputMedRefLetter'),
            # layers=[LambdaBaseLayer, BDAGenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, BDAGenericLayer],
            # need to add pandas layer
            description="Function to process the Medical Referral Letter extraction result from BDA",
            role=AapLambdaRole,
            environment={
                'BDA_PROCESSING_BUCKET': BDAProcessingBucket.bucket_name,
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_REFERRAL_LETTER_TABLE': DynamoDBMap['EXTRACTED_REFERRAL_LETTER_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        StartDocumentExtractionBDA.add_event_source(
            lambda_event_sources.S3EventSource(
                BDAProcessingBucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix='input/')]
            )
        )

        ProcessBDAExtractionOutput.add_event_source(
            lambda_event_sources.SqsEventSource(
                ProcessBDAExtractionOutputQueue,
                batch_size=10,
                enabled=True
            )
        )

        BrProcessBDAExtractionOutput.add_event_source(
            lambda_event_sources.SqsEventSource(
                BrProcessBDAExtractionOutputQueue,
                batch_size=10,
                enabled=True
            )
        )

        ProcessBDAExtractionOutputGRN.add_event_source(
            lambda_event_sources.SqsEventSource(
                ProcessBDAExtractionOutputGRNQueue,
                batch_size=10,
                enabled=True
            )
        )

        ProcessBDAExtractionOutputPO.add_event_source(
            lambda_event_sources.SqsEventSource(
                ProcessBDAExtractionOutputPOQueue,
                batch_size=10,
                enabled=True
            )
        )

        ProcessBDAExtractionOutputMedRefLetter.add_event_source(
            lambda_event_sources.SqsEventSource(
                ProcessBDAExtractionOutputMedRefLetterQueue,
                batch_size=1,
                enabled=True
            )
        )

        # Step Functions
        # Step Function for Retry Polling
        RetryBDAPolling_job = stepfunctions_tasks.LambdaInvoke(
            self, "Retry BDA Polling Job",
            lambda_function=StartDocumentExtractionBDA,
            output_path="$.Payload",
        )

        RetryBDAPolling_wait_job = stepfunctions.Wait(
            self, "Wait for Retry BDA Polling Jobs",
            time= stepfunctions.WaitTime.timestamp_path("$.retryPollingBDAWaitTime")
        )

        RetryBDAPolling_definition = RetryBDAPolling_wait_job.next(RetryBDAPolling_job)

        RetryBDAPollingStepFunctionMachine = stepfunctions.StateMachine(
            self, "RetryBDAPollingStepFunctionMachine",
            definition=RetryBDAPolling_definition,
            state_machine_name="RetryBDAPollingStepFunctionMachine"
        )


    def create_dependencies_layer(self, localPath):
        main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        while localPath[0] == '.' or localPath[0] == '/':
            localPath = localPath[1:]

        layerPath = f'{main_dir}/{localPath}'

        if not os.path.exists(f'{layerPath}/python'):
            subprocess.check_call(f'pip install -r {layerPath}/requirements.txt -t {layerPath}/python', shell=True)
        return lambda_.Code.from_asset(layerPath)

        