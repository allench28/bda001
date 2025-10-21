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
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as stepfunctions_tasks,
    aws_ssm as ssm,
    aws_sqs as sqs,
    Tags
)
from constructs import Construct
from aap_backend_cdk.environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')

class AapBackendLambdaStepFunctionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')

        lambda_dir = './lambda/Functions/'

        # Lambda IAM Role
        AapLambdaRole = iam.Role.from_role_arn(
            self, f'{PROJECT_NAME}' + 'LambdaRoleMY',
            f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}LambdaRoleMY'
        )

        AapEmailAttachmentTextractRole = iam.Role.from_role_arn(
            self, f'{PROJECT_NAME}' + 'EmailAttachmentTextractRoleMY',
            f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}EmailAttachmentTextractRoleMY'
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

        email_attachment_textract_sns_arn = Fn.import_value('EmailAttachmentTextractSNSArn')

        # SQS
        PresalesEmailDataAnalysisQueueDLQ = sqs.Queue(
            self, 'PresalesEmailDataAnalysisQueueDLQ',
            queue_name='PresalesEmailDataAnalysisQueueDLQ',
        )

        PresalesEmailDataAnalysisQueue = sqs.Queue(
            self, 'PresalesEmailDataAnalysisQueue',
            queue_name='PresalesEmailDataAnalysisQueue',
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=PresalesEmailDataAnalysisQueueDLQ),
            visibility_timeout=Duration.minutes(15)
        )

        # Lambda
        PresalesEmailStepFunctionCheck = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-PresalesEmailStepFunctionCheck',
            function_name=f'{PROJECT_NAME}' + '-PresalesEmailStepFunctionCheck',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-PresalesEmailStepFunctionCheck'),
            layers=[LambdaBaseLayer, AwsPandasLayer],
            # layers=[LambdaBaseLayer],            
            description="",
            role=AapLambdaRole,
            environment={},
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        PresalesEmailAttachmentAnalysis = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-PresalesEmailAttachmentAnalysis',
            function_name=f'{PROJECT_NAME}' + '-PresalesEmailAttachmentAnalysis',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-PresalesEmailAttachmentAnalysis'),
            layers=[AwsPandasLayer, LambdaBaseLayer, ExtractEmailMsgLayer, GenericLayer],
            # layers=[LambdaBaseLayer, GenericLayer,ExtractEmailMsgLayer],
            description="",
            role=AapLambdaRole,
            environment={
                'EXTRACTED_ATTACHMENT_PREFIX': 'presales/output/extracted_attachment',
                'EXTRACTED_EMAIL_TABLE': DynamoDBMap['EXTRACTED_EMAIL_TABLE'].format(PROJECT_NAME),
                'MERCHANT_ID': MERCHANT_ID_MAP[env]['VSTECS'],
                'MODEL_ID': BedrockModel[env]['model-3.7'],
                'ROOT_OUTPUT_PREFIX': 'presales/output/textract_output',
                'ROUTED_CONTENT_TABLE': DynamoDBMap['ROUTE_CONTENT_TABLE'].format(PROJECT_NAME),
                'S3_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'SKILL_MATRIX_TABLE': DynamoDBMap['SKILL_MATRIX_TABLE'].format(PROJECT_NAME),
                'SNS_TOPIC_ARN': email_attachment_textract_sns_arn,
                'SNS_ROLE_ARN': f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}TextractAsyncRole',
                'SQS_ANALYSIS_QUEUE_URL': SqsMap[env]['PresalesEmailAnalysisQueue'].format(ACCOUNT_ID),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        PreSalesExtractEmail = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-PreSalesExtractEmail',
            function_name=f'{PROJECT_NAME}' + '-PreSalesExtractEmail',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-PreSalesExtractEmail'),
            layers=[AwsPandasLayer, LambdaBaseLayer, GenericLayer, ExtractEmailMsgLayer],
            # layers=[LambdaBaseLayer, GenericLayer, ExtractEmailMsgLayer],
            description="",
            role=AapLambdaRole,
            environment={
                'EMAIL_ANALYSIS_RESULT_TABLE': DynamoDBMap['EMAIL_ANALYSIS_RESULT_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_EMAIL_TABLE': DynamoDBMap['EXTRACTED_EMAIL_TABLE'].format(PROJECT_NAME),
                'MERCHANT_ID': MERCHANT_ID_MAP[env]['VSTECS'],
                'MODEL_ID': BedrockModel[env]['model-3.7'],
                'ROUTED_CONTENT_TABLE': DynamoDBMap['ROUTE_CONTENT_TABLE'].format(PROJECT_NAME),
                'S3_BUCKET': s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'SKILL_MATRIX_TABLE': DynamoDBMap['SKILL_MATRIX_TABLE'].format(PROJECT_NAME),
                'SQS_QUEUE_URL': SqsMap[env]['RouteContentSQS'].format(ACCOUNT_ID),
            },
            timeout=Duration.seconds(300),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )
        
        # Step functions Definition
        
        ## EmailAttachmentTextract Step Function Tasks
        presales_email_attachment_analysis_task = stepfunctions_tasks.LambdaInvoke(
            self, "PresalesEmailAttachmentAnalysis",
            lambda_function=PresalesEmailAttachmentAnalysis,
            retry_on_service_exceptions=True,
            payload_response_only=True
        )

        presales_email_attachment_analysis_task.add_retry(
            errors=[
                "Lambda.ClientExecutionTimeoutException",
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException"
            ],
            interval=Duration.seconds(1),
            max_attempts=3,
            backoff_rate=2,
            jitter_strategy=stepfunctions.JitterType.FULL
        )

        # Map state definition
        map_state_definition = {
            "Type": "Map",
            "ItemProcessor": {
                "ProcessorConfig": {
                    "Mode": "INLINE"
                },
                "StartAt": "PollTextractJob",
                "States": {
                    "PollTextractJob": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::lambda:invoke",
                        "Output": "{% $states.result.Payload %}",
                        "Arguments": {
                            "FunctionName": PresalesEmailStepFunctionCheck.function_arn,
                            "Payload": "{% $states.input %}"
                        },
                        "Retry": [
                            {
                                "ErrorEquals": [
                                    "Lambda.ServiceException",
                                    "Lambda.AWSLambdaException", 
                                    "Lambda.SdkClientException",
                                    "Lambda.TooManyRequestsException"
                                ],
                                "IntervalSeconds": 1,
                                "MaxAttempts": 3,
                                "BackoffRate": 2,
                                "JitterStrategy": "FULL"
                            }
                        ],
                        "End": True
                    }
                }
            },
            "Next": "AnalyzeEmailData"
        }

        map_state = stepfunctions.CustomState(
            self, "MapTextractJobs",
            state_json=map_state_definition
        )

        # Final email analysis task
        analyze_email_data_task = stepfunctions_tasks.LambdaInvoke(
            self, "AnalyzeEmailData", 
            lambda_function=PreSalesExtractEmail,
            retry_on_service_exceptions=True,
            payload_response_only=True
        )

        analyze_email_data_task.add_retry(
            errors=[
                "Lambda.ClientExecutionTimeoutException",
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException"
            ],
            interval=Duration.seconds(1),
            max_attempts=3,
            backoff_rate=2,
            jitter_strategy=stepfunctions.JitterType.FULL
        )

        # Create the email attachment textract definition chain
        EmailAttachmentTextract_definition = presales_email_attachment_analysis_task\
            .next(map_state)\
            .next(analyze_email_data_task)

        # Create the EmailAttachmentTextract State Machine
        EmailAttachmentTextractStepFunctionMachine = stepfunctions.StateMachine(
            self, "EmailAttachmentTextractStepFunctionMachine",
            definition=EmailAttachmentTextract_definition,
            timeout=Duration.minutes(5),
            state_machine_name="EmailAttachmentTextractStepFunctionMachine",
            query_language=stepfunctions.QueryLanguage.JSONATA,
            role=AapEmailAttachmentTextractRole
        )

        ## SQS Trigger
        PreSalesExtractEmail.add_event_source(
            lambda_event_sources.SqsEventSource(
                PresalesEmailDataAnalysisQueue,
                batch_size=10,
                enabled=True
            )
        )