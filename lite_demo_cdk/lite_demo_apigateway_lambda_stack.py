from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_apigateway as apigateway,
    aws_ssm as ssm,
    aws_s3 as s3,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3_notifications as s3n,
    Duration,
    Tags
)
import os
from constructs import Construct
from .environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'LITE_DEMO')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')

"""
Lite Demo API Gateway Lambda Stack
This stack creates simplified Lambda functions for demo purposes without authorization.

Functions:
1. LiteDemoGenerateS3UploadLink
   POST /lite-demo/generate-upload-link
   OPTIONS /lite-demo/generate-upload-link

2. LiteDemoGenerateS3DownloadLink
   GET /lite-demo/generate-download-link
   OPTIONS /lite-demo/generate-download-link

3. LiteDemoS3EventProcessor (S3 Event Triggered)
   Automatically triggered when file uploaded to S3 input/ folder
"""

class LiteDemoApiGatewayLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, dynamodb_stack=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import Lambda Layers from SSM Parameter Store
        LambdaBaseLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'LambdaBaseLayer', 
            ssm.StringParameter.from_string_parameter_name(self, 'LambdaBaseLayerArn', 'AAP-LambdaBaseLayerArn').string_value
        )

        # Create API Gateway
        api = apigateway.RestApi(
            self, 
            f'{PROJECT_NAME}-LiteDemo-RestApi',
            rest_api_name=f'{PROJECT_NAME}-LiteDemo-RestApi',
            description='Lite Demo API Gateway for simplified S3 operations',
            deploy_options=apigateway.StageOptions(
                stage_name='prod',
                throttling_rate_limit=100,
                throttling_burst_limit=200
            )
        )

        # Create API resources
        lite_demo_resource = api.root.add_resource('lite-demo')
        upload_link_resource = lite_demo_resource.add_resource('generate-upload-link')
        download_link_resource = lite_demo_resource.add_resource('generate-download-link')

        # Environment variables
        s3_bucket_name = S3Map[env]['LITE_DEMO_BUCKET'].format(PROJECT_NAME.lower().replace('_', ''), RegionMap[env])
        documents_table_name = DynamoDBTableMap[env]['LITE_DEMO_DOCUMENTS'].format(PROJECT_NAME.lower().replace('_', '-'))
        
        common_env = {
            'LITE_DEMO_BUCKET': s3_bucket_name,
            'DOCUMENTS_TABLE_NAME': documents_table_name,
            'POWERTOOLS_SERVICE_NAME': f'{PROJECT_NAME}-LiteDemo',
            'POWERTOOLS_METRICS_NAMESPACE': f'{PROJECT_NAME}-LiteDemo',
            'LOG_LEVEL': 'INFO',
            'POWERTOOLS_LOG_LEVEL': 'INFO'
        }
        
        # S3 Processor specific env vars (for BDA)
        s3_processor_env = {
            **common_env,
            'BDA_RUNTIME_ENDPOINT': 'https://bedrock-data-automation-runtime.us-east-1.amazonaws.com',
            'OUTPUT_BUCKET': s3_bucket_name,
            'BDA_PROJECT_ARN': BDAMap[env]['PROJECT_ARN'],
            'BDA_PROFILE_ARN': BDAMap[env]['PROFILE_ARN']
        }

        # IAM Policy for S3 operations
        s3_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                's3:GetObject',
                's3:PutObject',
                's3:DeleteObject',
                's3:ListBucket'
            ],
            resources=[
                f'arn:aws:s3:::{s3_bucket_name}',
                f'arn:aws:s3:::{s3_bucket_name}/*'
            ]
        )

        # IAM Policy for DynamoDB operations
        dynamodb_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'dynamodb:PutItem',
                'dynamodb:GetItem',
                'dynamodb:UpdateItem',
                'dynamodb:Query',
                'dynamodb:Scan'
            ],
            resources=[
                f'arn:aws:dynamodb:{RegionMap[env]}:{AccountMap[env]}:table/{documents_table_name}',
                f'arn:aws:dynamodb:{RegionMap[env]}:{AccountMap[env]}:table/{documents_table_name}/index/*'
            ]
        )

        # IAM Policy for Bedrock Data Automation
        bedrock_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'bedrock:InvokeDataAutomationAsync',
                'bedrock:GetDataAutomationStatus',
                'bedrock:InvokeModel',
                'bedrock:InvokeModelWithResponseStream'
            ],
            resources=[
                # Specific project and profile
                BDAMap[env]['PROJECT_ARN'],
                BDAMap[env]['PROFILE_ARN'],
                # Wildcard for all BDA resources in us-east-1
                'arn:aws:bedrock:us-east-1:*:data-automation-project/*',
                'arn:aws:bedrock:us-east-1:*:data-automation-profile/*',
                # Wildcard for foundation models (if needed)
                'arn:aws:bedrock:*::foundation-model/*'
            ]
        )

        # Lambda execution role
        # lambda_role = iam.Role.from_role_arn(
        #     self, f'{PROJECT_NAME}' + 'LambdaRole',
        #     f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME}LambdaRole'
        # )
        lambda_role = iam.Role(
            self,
            f'{PROJECT_NAME}-LiteDemo-LambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSXRayDaemonWriteAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonBedrockFullAccess')
            ]
        )
        lambda_role.add_to_policy(s3_policy)
        lambda_role.add_to_policy(dynamodb_policy)
        lambda_role.add_to_policy(bedrock_policy)

        # ===== Lambda Function 1: Generate S3 Upload Link =====
        lambda_generate_upload = lambda_.Function(
            self,
            f'{PROJECT_NAME}-LiteDemoGenerateS3UploadLink',
            function_name=f'{PROJECT_NAME}-LiteDemoGenerateS3UploadLink',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset('lambda/Functions_LiteDemo/AAP-LiteDemoGenerateS3UploadLink'),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment=common_env,
            layers=[LambdaBaseLayer],
            role=lambda_role,
            tracing=lambda_.Tracing.ACTIVE
        )

        # API Gateway Integration for Upload Link
        upload_integration = apigateway.LambdaIntegration(
            lambda_generate_upload,
            proxy=True,
            integration_responses=[
                apigateway.IntegrationResponse(
                    status_code='200',
                    response_parameters={
                        'method.response.header.Access-Control-Allow-Origin': "'*'"
                    }
                )
            ]
        )

        upload_link_resource.add_method(
            'POST',
            upload_integration,
            method_responses=[
                apigateway.MethodResponse(
                    status_code='200',
                    response_parameters={
                        'method.response.header.Access-Control-Allow-Origin': True
                    }
                )
            ]
        )

        # CORS for Upload Link
        upload_link_resource.add_method(
            'OPTIONS',
            apigateway.MockIntegration(
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code='200',
                        response_parameters={
                            'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                            'method.response.header.Access-Control-Allow-Methods': "'POST,OPTIONS'",
                            'method.response.header.Access-Control-Allow-Origin': "'*'"
                        }
                    )
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.NEVER,
                request_templates={
                    'application/json': '{"statusCode": 200}'
                }
            ),
            method_responses=[
                apigateway.MethodResponse(
                    status_code='200',
                    response_parameters={
                        'method.response.header.Access-Control-Allow-Headers': True,
                        'method.response.header.Access-Control-Allow-Methods': True,
                        'method.response.header.Access-Control-Allow-Origin': True
                    }
                )
            ]
        )

        # ===== Lambda Function 2: Generate S3 Download Link =====
        lambda_generate_download = lambda_.Function(
            self,
            f'{PROJECT_NAME}-LiteDemoGenerateS3DownloadLink',
            function_name=f'{PROJECT_NAME}-LiteDemoGenerateS3DownloadLink',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset('lambda/Functions_LiteDemo/AAP-LiteDemoGenerateS3DownloadLink'),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment=common_env,
            layers=[LambdaBaseLayer],
            role=lambda_role,
            tracing=lambda_.Tracing.ACTIVE
        )

        # API Gateway Integration for Download Link
        download_integration = apigateway.LambdaIntegration(
            lambda_generate_download,
            proxy=True,
            integration_responses=[
                apigateway.IntegrationResponse(
                    status_code='200',
                    response_parameters={
                        'method.response.header.Access-Control-Allow-Origin': "'*'"
                    }
                )
            ]
        )

        download_link_resource.add_method(
            'GET',
            download_integration,
            method_responses=[
                apigateway.MethodResponse(
                    status_code='200',
                    response_parameters={
                        'method.response.header.Access-Control-Allow-Origin': True
                    }
                )
            ]
        )

        # CORS for Download Link
        download_link_resource.add_method(
            'OPTIONS',
            apigateway.MockIntegration(
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code='200',
                        response_parameters={
                            'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                            'method.response.header.Access-Control-Allow-Methods': "'GET,OPTIONS'",
                            'method.response.header.Access-Control-Allow-Origin': "'*'"
                        }
                    )
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.NEVER,
                request_templates={
                    'application/json': '{"statusCode": 200}'
                }
            ),
            method_responses=[
                apigateway.MethodResponse(
                    status_code='200',
                    response_parameters={
                        'method.response.header.Access-Control-Allow-Headers': True,
                        'method.response.header.Access-Control-Allow-Methods': True,
                        'method.response.header.Access-Control-Allow-Origin': True
                    }
                )
            ]
        )

        # ===== Lambda Function 3: S3 Event Processor =====
        lambda_s3_processor = lambda_.Function(
            self,
            f'{PROJECT_NAME}-LiteDemoS3EventProcessor',
            function_name=f'{PROJECT_NAME}-LiteDemoS3EventProcessor',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset('lambda/Functions_LiteDemo/AAP-LiteDemoS3EventProcessor'),
            timeout=Duration.minutes(5),
            memory_size=1024,
            environment=s3_processor_env,
            layers=[LambdaBaseLayer],
            role=lambda_role,
            tracing=lambda_.Tracing.ACTIVE
        )

        # Import existing S3 bucket
        lite_demo_bucket = s3.Bucket.from_bucket_name(
            self, 'LiteDemoBucket',
            s3_bucket_name
        )
        
        # Add S3 notification using s3_notifications (works with existing buckets)
        lite_demo_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_s3_processor),
            s3.NotificationKeyFilter(
                prefix='input/',
                suffix='.pdf'
            )
        )

        # Add tags to all resources
        Tags.of(self).add('Project', PROJECT_NAME)
        Tags.of(self).add('Environment', env)
        Tags.of(self).add('Stack', 'LiteDemo')
        Tags.of(self).add('ManagedBy', 'CDK')

        # Store references
        self.api = api
        self.lambda_generate_upload = lambda_generate_upload
        self.lambda_generate_download = lambda_generate_download
        self.lambda_s3_processor = lambda_s3_processor
