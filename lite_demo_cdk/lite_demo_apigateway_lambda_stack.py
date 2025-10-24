from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_apigateway as apigateway,
    aws_ssm as ssm,
    aws_s3 as s3,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    Duration,
    Tags
)
import os
from constructs import Construct
from .environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'LITE_DEMO')
# Account ID will be auto-detected from CDK context
ACCOUNT_ID = os.environ.get('ACCOUNT_ID')

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

3. LiteDemoGetDocument
   GET /lite-demo/get-document
   OPTIONS /lite-demo/get-document

4. LiteDemoGetResult
   GET /lite-demo/get-result
   OPTIONS /lite-demo/get-result

4. LiteDemoS3EventProcessor (S3 Event Triggered)
   Automatically triggered when file uploaded to S3 input/ folder
"""

class LiteDemoApiGatewayLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, dynamodb_stack=None, s3_stack=None, bda_stack=None, sns_stack=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import Lambda Layers - try SSM first, fallback to None
        try:
            LambdaBaseLayer = lambda_.LayerVersion.from_layer_version_arn(
                self, 'LambdaBaseLayer', 
                ssm.StringParameter.from_string_parameter_name(self, 'LambdaBaseLayerArn', 'AAP-LambdaBaseLayerArn').string_value
            )
        except:
            # Fallback: No custom layer
            LambdaBaseLayer = None

        # Use AWS managed pandas layer (available in most regions)
        try:
            AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
                self, 'AwsPandasLayer',
                f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python312:19"
            )
        except:
            AwsPandasLayer = None

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
        get_result_resource = lite_demo_resource.add_resource('get-result')
        get_document_resource = lite_demo_resource.add_resource('get-document')

        # Environment variables
        # Get S3 bucket name from S3 stack reference (unique name)
        if s3_stack:
            s3_bucket_name = s3_stack.bucket_name
        else:
            # Fallback: Read from SSM Parameter if stack reference not available
            s3_bucket_name = ssm.StringParameter.from_string_parameter_name(
                self, 
                'LiteDemoBucketNameFromSSM',
                f'/{PROJECT_NAME}/LiteDemo/S3BucketName'
            ).string_value
        
        documents_table_name = DynamoDBTableMap[env]['LITE_DEMO_DOCUMENTS'].format(PROJECT_NAME.lower().replace('_', '-'))
        
        common_env = {
            'LITE_DEMO_BUCKET': s3_bucket_name,
            'DOCUMENTS_TABLE_NAME': documents_table_name,
            'POWERTOOLS_SERVICE_NAME': f'{PROJECT_NAME}-LiteDemo',
            'POWERTOOLS_METRICS_NAMESPACE': f'{PROJECT_NAME}-LiteDemo',
            'LOG_LEVEL': 'INFO',
            'POWERTOOLS_LOG_LEVEL': 'INFO'
        }
        
        # Get SNS Topic ARN
        if sns_stack:
            sns_topic_arn = sns_stack.topic_arn
        else:
            sns_topic_arn = ssm.StringParameter.from_string_parameter_name(
                self,
                'LiteDemoSNSTopicArnFromSSM',
                f'/{PROJECT_NAME}/LiteDemo/SNSTopicArn'
            ).string_value

        # S3 Processor specific env vars (for BDA)
        bda_project_arn = bda_stack.project_arn if bda_stack else BDAMap[env]['PROJECT_ARN']

        s3_processor_env = {
            **common_env,
            'BDA_RUNTIME_ENDPOINT': f'https://bedrock-data-automation-runtime.{self.region}.amazonaws.com',
            'OUTPUT_BUCKET': s3_bucket_name,
            'BDA_PROJECT_ARN': bda_project_arn,
            'BDA_PROFILE_ARN': f'arn:aws:bedrock:{self.region}:{self.account}:data-automation-profile/us.data-automation-v1',
            'SNS_TOPIC_ARN': sns_topic_arn,
            'REGION': self.region
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
                f'arn:aws:dynamodb:{self.region}:{self.account}:table/{documents_table_name}',
                f'arn:aws:dynamodb:{self.region}:{self.account}:table/{documents_table_name}/index/*'
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
                bda_project_arn,
                BDAMap[env]['PROFILE_ARN'],
                # Wildcard for all BDA resources in us-east-1
                'arn:aws:bedrock:us-east-1:*:data-automation-project/*',
                'arn:aws:bedrock:us-east-1:*:data-automation-profile/*',
                # Wildcard for foundation models (if needed)
                'arn:aws:bedrock:*::foundation-model/*'
            ]
        )

        # IAM Policy for SNS operations
        sns_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'sns:Publish'
            ],
            resources=[
                sns_topic_arn
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
        lambda_role.add_to_policy(sns_policy)

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

        # ===== Lambda Function 3: Get Document =====
        lambda_get_document = lambda_.Function(
            self,
            f'{PROJECT_NAME}-LiteDemoGetDocument',
            function_name=f'{PROJECT_NAME}-LiteDemoGetDocument',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset('lambda/Functions_LiteDemo/AAP-LiteDemoGetDocument'),
            timeout=Duration.seconds(300),
            memory_size=128,
            environment=common_env,
            layers=[LambdaBaseLayer],
            role=lambda_role,
            tracing=lambda_.Tracing.ACTIVE
        )

        # API Gateway Integration for Download Link
        get_document_integration = apigateway.LambdaIntegration(
            lambda_get_document,
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

        get_document_resource.add_method(
            'GET',
            get_document_integration,
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
        get_document_resource.add_method(
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

        # ===== Lambda Function 4: Export Result =====
        lambda_get_result = lambda_.Function(
            self,
            f'{PROJECT_NAME}-LiteDemoExportResult',
            function_name=f'{PROJECT_NAME}-LiteDemoExportResult',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset('lambda/Functions_LiteDemo/AAP-LiteDemoExportResult'),
            timeout=Duration.seconds(300),
            memory_size=128,
            environment=common_env,
            layers=[LambdaBaseLayer, AwsPandasLayer],
            role=lambda_role,
            tracing=lambda_.Tracing.ACTIVE
        )

        # API Gateway Integration for Download Link
        get_result_integration = apigateway.LambdaIntegration(
            lambda_get_result,
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

        get_result_resource.add_method(
            'GET',
            get_result_integration,
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
        get_result_resource.add_method(
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
            layers=[LambdaBaseLayer, AwsPandasLayer],
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
        # Upsert API URL to SSM Parameter Store using AwsCustomResource
        from aws_cdk.custom_resources import AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId, AwsSdkCall
        api_url_param = AwsCustomResource(
            self,
            'LiteDemoApiUrlGwUpsert',
            on_update=AwsSdkCall(
                service='SSM',
                action='putParameter',
                parameters={
                    'Name': 'lite-demo-api-url-gw',
                    'Value': api.url,
                    'Type': 'String',
                    'Overwrite': True,
                    'Description': 'Lite Demo API Gateway URL',
                    'Tier': 'Standard'
                },
                physical_resource_id=PhysicalResourceId.of('lite-demo-api-url-gw')
            ),
            policy=AwsCustomResourcePolicy.from_sdk_calls(resources=AwsCustomResourcePolicy.ANY_RESOURCE)
        )

        self.lambda_generate_upload = lambda_generate_upload
        self.lambda_generate_download = lambda_generate_download
        self.lambda_s3_processor = lambda_s3_processor


