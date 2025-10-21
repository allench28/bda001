from aws_cdk import (
    # Duration,
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_apigateway as apigateway,
    Duration,
    aws_ssm as ssm,
    aws_cognito as cognito,
    Tags
)
import os
from datetime import datetime
from constructs import Construct
from .environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')
ES_ENDPOINT = os.environ.get('ES_ENDPOINT', '')
ADMIN_USER_POOL_ARN = os.environ.get('ADMIN_USER_POOL_ARN', 'arn:aws:cognito-idp:ap-southeast-5:582554346432:userpool/ap-southeast-5_1vOfv8CWn')

"""
Function Names:
1. AdminCreateOrUpdateAgentConfig
POST /agents
OPTIONS /agents
2. AdminGetAgentConfig
GET /agents
3. AdminDeleteAgentConfig
DELETE /agents
OPTIONS /agents
4. AdminGenerateS3UploadLink
POST /generates3uploadlink
OPTIONS /generates3uploadlink
5. AdminGetPrompt
GET /prompts
6. AdminGetSettings
GET /services
7. AdminGetDocument
GET /documents
8. AdminApproveExtractedDocument
POST /documents/approve
OPTIONS /documents/approve
9. AdminSaveExtractedDocument
POST /documents/save
OPTIONS /documents/save
10. AdminDeleteExtractedDocument
DELETE /documents
OPTIONS /documents
11. AdminGenerateS3DownloadLink
POST /generates3downloadlink
OPTIONS /generates3downloadlink
12. AdminGetUserMerchantList
GET /users/validate
13. AdminListUserMatrix
GET /usermatrix/list
14. AdminCreateOrUpdateUser
POST /users
OPTIONS /users
15. AdminDeleteUser
DELETE /users
OPTIONS /users
16. AdminCreateOrUpdateUserGroup
POST /usergroup
OPTIONS /usergroup
17. AdminDeleteUserGroup
DELETE /usergroup
OPTIONS /usergroup
18. AdminGetUserMatrix
GET /usermatrix/detail
19. AdminGetUserGroups
GET /usergroups
20. AdminGetUsers
GET /users

Based on the list of function names above, create a set of API Gateway resources and Lambda functions that will be used to implement the backend admin API.
As of now, assume no environment variables are needed for the Lambda functions.
Since this is an API, the timeout for the Lambda functions should be set to 30 seconds, and memory size should be set to 256 MB.
Make sure lambda tracing is active.
Use Lambda Role: AAPLambdaRole
Use Lambda Layer: LambdaBaseLayer, GenericLayer
Import the Lambda Role using from_role_arn
Import the Lambda Layer using from_layer_version_arn
"""


class AapBackendAdminLambdaApiGatewayStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')

        lambda_dir = './lambda/Functions_Admin/'
        # Datetime now
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')


        # The code that defines your stack goes here
        
        AAPLambdaRole = iam.Role.from_role_arn(
            self, 'AAPLambdaRoleMY', 
            f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}LambdaRoleMY'
        )

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
        #     ssm.StringParameter.from_string_parameter_name(self, 'AwsPandasLayerArn', f'{PROJECT_NAME}' + '-AwsPandasLayerArn').string_value
        # )

        user_pool = cognito.UserPool.from_user_pool_id(
            self, 'ImportedUserPool',
            user_pool_id=ADMIN_USER_POOL_ARN
        )

        auth = apigateway.CognitoUserPoolsAuthorizer(
            self, f'{PROJECT_NAME}CognitoAuthorizer',
            cognito_user_pools=[user_pool],
            authorizer_name=f'{PROJECT_NAME}-Cognito-Authorizer',
            identity_source='method.request.header.Authorization'
        )


        # AdminCreateOrUpdateAgentConfig
        AdminCreateOrUpdateAgentConfig = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminCreateOrUpdateAgentConfig',
            function_name=f'{PROJECT_NAME}' + '-AdminCreateOrUpdateAgentConfig',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminCreateOrUpdateAgentConfig'),
            # layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Create or Update Agent Config',
            role=AAPLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'AGENT_CONFIGURATION_BUCKET': S3Map[env]['AGENT_CONFIGURATION_BUCKET'].format(PROJECT_NAME.lower(), env),
                'EMAIL_POLLING_LAMBDA_ARN': LambdaArnMap['EMAIL_POLLING_LAMBDA_ARN'].format(RegionMap[env], AccountMap[env], PROJECT_NAME.lower()),
                'AWS_ACCOUNT_ID': ACCOUNT_ID,
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetAgentConfig
        AdminGetAgentConfig = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetAgentConfig',
            function_name=f'{PROJECT_NAME}' + '-AdminGetAgentConfig',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetAgentConfig'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Agent Config',
            role=AAPLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )


        # AdminDeleteAgentConfig
        AdminDeleteAgentConfig = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminDeleteAgentConfig',
            function_name=f'{PROJECT_NAME}' + '-AdminDeleteAgentConfig',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminDeleteAgentConfig'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Delete Agent Config',
            role=AAPLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'AGENT_CONFIGURATION_BUCKET': S3Map[env]['AGENT_CONFIGURATION_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGenerateS3UploadLink
        AdminGenerateS3UploadLink = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGenerateS3UploadLink',
            function_name=f'{PROJECT_NAME}' + '-AdminGenerateS3UploadLink',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGenerateS3UploadLink'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Generate S3 Upload Link',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'AGENT_CONFIGURATION_BUCKET': S3Map[env]['AGENT_CONFIGURATION_BUCKET'].format(PROJECT_NAME.lower(), env),
                'SMART_EYE_BUCKET': S3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetPrompt
        AdminGetPrompt = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetPrompt',
            function_name=f'{PROJECT_NAME}' + '-AdminGetPrompt',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetPrompt'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Prompt',
            role=AAPLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN': BedrockPromptManagementArnMap['DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN'],
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetSettings
        AdminGetSettings = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetSettings',
            function_name=f'{PROJECT_NAME}' + '-AdminGetSettings',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetSettings'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Settings',
            role=AAPLambdaRole,
            environment={
                'AGENT_CONFIGURATION_TABLE': DynamoDBMap['AGENT_CONFIGURATION_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetDocument
        AdminGetDocument = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetDocument',
            function_name=f'{PROJECT_NAME}' + '-AdminGetDocument',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetDocument'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Document',
            role=AAPLambdaRole,
            environment={
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_TABLE': DynamoDBMap['EXTRACTED_GRN_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_GRN_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_REFERRAL_LETTER_TABLE': DynamoDBMap['EXTRACTED_REFERRAL_LETTER_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminApproveExtractedDocument
        AdminApproveExtractedDocument = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminApproveExtractedDocument',
            function_name=f'{PROJECT_NAME}' + '-AdminApproveExtractedDocument',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminApproveExtractedDocument'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Approve Extracted Document',
            role=AAPLambdaRole,
            environment={
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_TABLE': DynamoDBMap['EXTRACTED_GRN_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_GRN_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'N8N_SQS_QUEUE': SqsMap[env]['N8N_SQS_QUEUE'],
                'SEQUENCE_NUMBER_GENERATOR_TABLE' : DynamoDBMap['SEQUENCE_NUMBER_GENERATOR_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminSaveExtractedDocument
        AdminSaveExtractedDocument = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminSaveExtractedDocument',
            function_name=f'{PROJECT_NAME}' + '-AdminSaveExtractedDocument',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminSaveExtractedDocument'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Save Extracted Document',
            role=AAPLambdaRole,
            environment={
                'EXTRACTED_DOCUMENTS_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_TABLE': DynamoDBMap['EXTRACTED_GRN_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_GRN_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEM_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_REFERRAL_LETTER_TABLE': DynamoDBMap['EXTRACTED_REFERRAL_LETTER_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminDeleteExtractedDocument
        AdminDeleteExtractedDocument = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminDeleteExtractedDocument',
            function_name=f'{PROJECT_NAME}' + '-AdminDeleteExtractedDocument',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminDeleteExtractedDocument'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Delete Extracted Document',
            role=AAPLambdaRole,
            environment={
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGenerateS3DownloadLink
        AdminGenerateS3DownloadLink = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGenerateS3DownloadLink',
            function_name=f'{PROJECT_NAME}' + '-AdminGenerateS3DownloadLink',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGenerateS3DownloadLink'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Generate S3 Download Link',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'SMART_EYE_BUCKET': S3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetUserMerchantList
        AdminGetUserMerchantList = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetUserMerchantList',
            function_name=f'{PROJECT_NAME}' + '-AdminGetUserMerchantList',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetUserMerchantList'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get User Merchant List',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME)
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminListUserMatrix
        AdminListUserMatrix = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminListUserMatrix',
            function_name=f'{PROJECT_NAME}' + '-AdminListUserMatrix',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListUserMatrix'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List User Matrix',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_MATRIX_TABLE': DynamoDBMap['USER_MATRIX_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME)
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminCreateOrUpdateUser
        AdminCreateOrUpdateUser = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminCreateOrUpdateUser',
            function_name=f'{PROJECT_NAME}' + '-AdminCreateOrUpdateUser',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminCreateOrUpdateUser'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Create or Update User',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'COGNITO_USER_POOL': CognitoMap['COGNITO_USER_POOL']
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminDeleteUser
        AdminDeleteUser = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminDeleteUser',
            function_name=f'{PROJECT_NAME}' + '-AdminDeleteUser',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminDeleteUser'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Delete User',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'COGNITO_USER_POOL': CognitoMap['COGNITO_USER_POOL']
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminCreateOrUpdateUserGroup
        AdminCreateOrUpdateUserGroup = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminCreateOrUpdateUserGroup',
            function_name=f'{PROJECT_NAME}' + '-AdminCreateOrUpdateUserGroup',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminCreateOrUpdateUserGroup'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Create or Update User Group',
            role=AAPLambdaRole,
            environment={
                #'SQS_URL': SqsMap[env],
                #'SQS_URL': SqsMap[env],
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_MATRIX_TABLE': DynamoDBMap['USER_MATRIX_TABLE'].format(PROJECT_NAME)
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminDeleteUserGroup
        AdminDeleteUserGroup = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminDeleteUserGroup',
            function_name=f'{PROJECT_NAME}' + '-AdminDeleteUserGroup',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminDeleteUserGroup'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Delete User Group',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_MATRIX_TABLE': DynamoDBMap['USER_MATRIX_TABLE'].format(PROJECT_NAME)
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetUserMatrix
        AdminGetUserMatrix = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetUserMatrix',
            function_name=f'{PROJECT_NAME}' + '-AdminGetUserMatrix',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetUserMatrix'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get User Matrix',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_MATRIX_TABLE': DynamoDBMap['USER_MATRIX_TABLE'].format(PROJECT_NAME)
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetUserGroups
        AdminGetUserGroups = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetUserGroups',
            function_name=f'{PROJECT_NAME}' + '-AdminGetUserGroups',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetUserGroups'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get User Groups',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminGetUsers
        AdminGetUsers = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminGetUsers',
            function_name=f'{PROJECT_NAME}' + '-AdminGetUsers',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetUsers'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Users',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminListUploadedDocuments
        AdminListUploadedDocument = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminListUploadedDocuments',
            function_name=f'{PROJECT_NAME}' + '-AdminListUploadedDocuments',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListUploadedDocuments'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List Uploaded Documents',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ES_ENDPOINT,
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_TABLE': DynamoDBMap['EXTRACTED_GRN_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_REFERRAL_LETTER_TABLE': DynamoDBMap['EXTRACTED_REFERRAL_LETTER_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminListAuditTrail
        AdminListAuditTrail = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminListAuditTrail',
            function_name=f'{PROJECT_NAME}' + '-AdminListAuditTrail',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListAuditTrail'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List Audit Trail',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ES_ENDPOINT,
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminTriggerExportCSV
        AdminTriggerExportCSV = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminTriggerExportCSV',
            function_name=f'{PROJECT_NAME}' + '-AdminTriggerExportCSV',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminTriggerExportCSV'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to trigger CSV generation',
            environment={
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ES_ENDPOINT,
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'EXPORT_EXTRACTED_DOCUMENTS_LAMBDA': f'{PROJECT_NAME}-AdminExportExtractedDocumentCSV',
                'EXPORT_UPLOADED_DOCUMENTS_LAMBDA': f'{PROJECT_NAME}-AdminExportUploadedDocumentsCSV',
                'EXPORT_EXTRACTED_LINE_ITEMS_LAMBDA': f'{PROJECT_NAME}-AdminExportExtractedLineItemsCSV',
                'EXPORT_EXTRACTED_PO_LAMBDA': f'{PROJECT_NAME}-AdminExportExtractedPoCSV',
                'EXPORT_EXTRACTED_PO_LINE_ITEMS_LAMBDA': f'{PROJECT_NAME}-AdminExportExtractedPoLineItemsCSV',
                'EXPORT_THREE_WAY_MATCHING_LAMBDA': f'{PROJECT_NAME}-AdminExport3WayMatchingResults',
                'EXPORT_RECONCILIATION_RESULTS_LAMBDA': f'{PROJECT_NAME}-AdminExportReconciliationResultsCSV',            },
            role=AAPLambdaRole,
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminListExtractedDocuments
        AdminListExtractedDocuments = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminListExtractedDocuments',
            function_name=f'{PROJECT_NAME}' + '-AdminListExtractedDocuments',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListExtractedDocuments'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List Extracted Documents',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ES_ENDPOINT,
                'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminListExtractedReferralLetter
        AdminListExtractedReferralLetter = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminListExtractedReferralLetter',
            function_name='AAP-AdminListExtractedReferralLetter',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListExtractedReferralLetter'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List Extracted Referral Letters',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ES_ENDPOINT,
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminGetDownloadJobStatus = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminGetDownloadJobStatus',
            function_name=f'{PROJECT_NAME}' + '-AdminGetDownloadJobStatus',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetDownloadJobStatus'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Download Job Status',
            role=AAPLambdaRole,
            environment={
                'DOWNLOAD_JOB_TABLE': DynamoDBMap['DOWNLOAD_JOB_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminGetMerchant = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminGetMerchant',
            function_name=f'{PROJECT_NAME}' + '-AdminGetMerchant',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetMerchant'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Merchant',
            role=AAPLambdaRole,
            environment={
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminCreateOrUpdateMerchant = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminCreateOrUpdateMerchant',
            function_name=f'{PROJECT_NAME}' + '-AdminCreateOrUpdateMerchant',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminCreateOrUpdateMerchant'),
            # layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Create or Update Merchant',
            role=AAPLambdaRole,
            environment={
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_MATRIX_TABLE': DynamoDBMap['USER_MATRIX_TABLE'].format(PROJECT_NAME),
                'AGENT_CONFIGURATION_BUCKET': S3Map[env]['AGENT_CONFIGURATION_BUCKET'].format(PROJECT_NAME.lower(), env),
                'COGNITO_USER_POOL': CognitoMap['COGNITO_USER_POOL'],
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminDeleteMerchant = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminDeleteMerchant',
            function_name=f'{PROJECT_NAME}' + '-AdminDeleteMerchant',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminDeleteMerchant'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Delete Merchant',
            role=AAPLambdaRole,
            environment={
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminListExtractedGRN = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminListExtractedGRN',
            function_name=f'{PROJECT_NAME}' + '-AdminListExtractedGRN',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListExtractedGRN'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List Extracted GRN',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ElasticSearchMap[env]['endpoint'],
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_GRN_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminListExtractedPo = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminListExtractedPo',
            function_name=f'{PROJECT_NAME}' + '-AdminListExtractedPo',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListExtractedPo'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List Extracted PO',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ElasticSearchMap[env]['endpoint'],
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminList3WayMatchingResults
        AdminList3WayMatchingResults = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminList3WayMatchingResults',
            function_name=f'{PROJECT_NAME}' + '-AdminList3WayMatchingResults',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminList3WayMatchingResults'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List 3 Way Matching Results',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ElasticSearchMap[env]['endpoint'],
                'THREE_WAY_MATCHING_RESULTS_TABLE': DynamoDBMap['THREE_WAY_MATCHING_RESULTS_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # ThreeWayMatching
        ThreeWayMatching = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'ThreeWayMatching',
            function_name=f'{PROJECT_NAME}' + '-ThreeWayMatching',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-ThreeWayMatching'),
            # layers=[LambdaBaseLayer, GenericLayer, AwsPandasLayer],
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to carry out 3 Way Matching',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'THREE_WAY_MATCHING_RESULTS_TABLE': DynamoDBMap['THREE_WAY_MATCHING_RESULTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_TABLE': DynamoDBMap['EXTRACTED_GRN_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_GRN_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'S3_BUCKET_NAME': S3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),
                'SQS_QUEUE_URL': SqsMap[env]['3WAYMATCHING_SQS'].format(ACCOUNT_ID),
                'JOB_TRACKING_TABLE': DynamoDBMap['JOB_TRACKING_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(50),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        GetNetsuiteDocuments = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-GetNetsuiteDocuments',
            function_name=f'{PROJECT_NAME}' + '-GetNetsuiteDocuments',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-GetNetsuiteDocuments'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to retrieve documents from NetSuite",
            role=AAPLambdaRole,
            environment={
                'NS_CREDENTIALS': f'{env}/netsuiteClientKey',
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME.upper()),
                'EXTRACTED_PO_TABLE': DynamoDBMap['EXTRACTED_PO_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_PO_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_PO_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_TABLE': DynamoDBMap['EXTRACTED_GRN_TABLE'].format(PROJECT_NAME),
                'EXTRACTED_GRN_LINE_ITEMS_TABLE': DynamoDBMap['EXTRACTED_GRN_LINE_ITEMS_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.minutes(10),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=1024
        )

        AdminGetJobStatus = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminGetJobStatus',
            function_name=f'{PROJECT_NAME}' + '-AdminGetJobStatus',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminGetJobStatus'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to Get Job Status for 3 Way Matching',
            role=AAPLambdaRole,
            environment={
                'JOB_TRACKING_TABLE': DynamoDBMap['JOB_TRACKING_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )


        AdminListReconciliationSourceRecords = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminListReconciliationSourceRecords',
            function_name=f'{PROJECT_NAME}' + '-AdminListReconciliationSourceRecords',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListReconciliationSourceRecords'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List AR Recon Source Records',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ES_ENDPOINT,
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'DOCUMENT_UPLOAD_TABLE': DynamoDBMap['DOCUMENT_UPLOAD_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        AdminListReconciliationMatchingResults = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminListReconciliationMatchingResults',
            function_name=f'{PROJECT_NAME}' + '-AdminListReconciliationMatchingResults',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminListReconciliationMatchingResults'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to List Reconciliation Matching Results',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'ES_DOMAIN': ES_ENDPOINT,
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(30),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )

        # AdminList3WayMatchingSourceRecords
        AdminList3WayMatchingSourceRecords = lambda_.Function(
            self, f'{PROJECT_NAME}' + 'AdminList3WayMatchingSourceRecords',
            function_name=f'{PROJECT_NAME}' + '-AdminList3WayMatchingSourceRecords',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminList3WayMatchingSourceRecords'),
            layers=[LambdaBaseLayer, GenericLayer],
            description='Function to carry out 3 Way Matching',
            role=AAPLambdaRole,
            environment={
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'MERCHANT_TABLE': DynamoDBMap['MERCHANT_TABLE'].format(PROJECT_NAME),
                'SMART_EYE_BUCKET': S3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env),

            },
            timeout=Duration.seconds(50),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=512
        )

        # AdminTriggerARReconciliation
        AdminTriggerARReconciliation = lambda_.Function(
            self, f'{PROJECT_NAME}' + '-AdminTriggerARReconciliation',
            function_name=f'{PROJECT_NAME}' + '-AdminTriggerARReconciliation',
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler='lambda_function.lambda_handler',
            code=lambda_.Code.from_asset(lambda_dir + 'AAP-AdminTriggerARReconciliation'),
            layers=[LambdaBaseLayer, GenericLayer],
            description="Function to trigger AR reconciliation jobs",
            role=AAPLambdaRole,
            environment={
                'JOB_TRACKING_TABLE': DynamoDBMap['JOB_TRACKING_TABLE'].format(PROJECT_NAME),
                'TRIGGER_RECONCILIATION_QUEUE_URL': SqsMap[env]['TRIGGER_RECONCILIATION_QUEUE_URL'].format(AccountMap[env]),  # Update this if needed
                'USER_TABLE': DynamoDBMap['USER_TABLE'].format(PROJECT_NAME),
                'USER_GROUP_TABLE': DynamoDBMap['USER_GROUP_TABLE'].format(PROJECT_NAME),
                'TIMELINE_TABLE': DynamoDBMap['TIMELINE_TABLE'].format(PROJECT_NAME),
            },
            timeout=Duration.seconds(60),
            tracing=lambda_.Tracing.ACTIVE,
            memory_size=256
        )


        # API Gateway
        # Create API Gateway
        AapAdminApi = apigateway.RestApi(
            self, f'{PROJECT_NAME}' + '-AdminApi',
            description=f'{PROJECT_NAME}' + 'Admin REST API',
            deploy=False,
            endpoint_configuration={
                "types": [apigateway.EndpointType.REGIONAL]
            }
        )
        
        AapAdminApiDeployment = apigateway.Deployment(
            self, f'{PROJECT_NAME}' + '-AdminApi-Deployment-{}'.format(now),
            api=AapAdminApi,
            retain_deployments=True,
            description=f'{PROJECT_NAME}' + '-AdminApi-Deployment-{}'.format(now)
        )

        AapAdminApiStage = apigateway.Stage(
            self, f'{PROJECT_NAME}' + '-AdminApi-Stage',
            deployment=AapAdminApiDeployment, 
            stage_name=env,
            description=f'{PROJECT_NAME}' + '-AdminApi-Stage',
            logging_level=apigateway.MethodLoggingLevel.ERROR,
            data_trace_enabled=True,
            tracing_enabled=True
        )

        # API /agents
        AdminAgentsApiResource = AapAdminApi.root.add_resource('agents')
        AdminAgentsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminAgentConfigMutationFunctionIntegration = apigateway.LambdaIntegration(AdminCreateOrUpdateAgentConfig)
        AdminAgentConfigMutationApiMethod = AdminAgentsApiResource.add_method(
            'POST', 
            AdminAgentConfigMutationFunctionIntegration, 
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        AdminAgentConfigQueryFunctionIntegration = apigateway.LambdaIntegration(AdminGetAgentConfig)
        AdminAgentConfigQueryApiMethod = AdminAgentsApiResource.add_method(
            'GET', 
            AdminAgentConfigQueryFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /agents/delete
        AdminDeleteAgentApiResource = AdminAgentsApiResource.add_resource('delete')
        AdminDeleteAgentApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminAgentConfigDeleteFunctionIntegration = apigateway.LambdaIntegration(AdminDeleteAgentConfig)
        AdminAgentConfigDeleteApiMethod = AdminDeleteAgentApiResource.add_method(
            'POST', 
            AdminAgentConfigDeleteFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /generates3uploadlink
        AdminGenerateS3UploadLinkApiResource = AapAdminApi.root.add_resource('generates3uploadlink')
        AdminGenerateS3UploadLinkApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGenerateS3UploadLinkFunctionIntegration = apigateway.LambdaIntegration(AdminGenerateS3UploadLink)
        AdminGenerateS3UploadLinkApiMethod = AdminGenerateS3UploadLinkApiResource.add_method(
            'POST', 
            AdminGenerateS3UploadLinkFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /prompts
        AdminPromptsApiResource = AapAdminApi.root.add_resource('prompts')
        AdminPromptsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGetPromptFunctionIntegration = apigateway.LambdaIntegration(AdminGetPrompt)
        AdminGetPromptApiMethod = AdminPromptsApiResource.add_method(
            'GET',
            AdminGetPromptFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )


        # API /services
        AdminServicesApiResource = AapAdminApi.root.add_resource('services')
        AdminServicesApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGetSettingsFunctionIntegration = apigateway.LambdaIntegration(AdminGetSettings)
        AdminGetSettingsApiMethod = AdminServicesApiResource.add_method(
            'GET', 
            AdminGetSettingsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )


        # API /documents
        AdminDocumentsApiResource = AapAdminApi.root.add_resource('documents')
        AdminDocumentsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGetDocumentFunctionIntegration = apigateway.LambdaIntegration(AdminGetDocument)
        AdminGetDocumentApiMethod = AdminDocumentsApiResource.add_method(
            'GET', 
            AdminGetDocumentFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /documents/list
        AdminListUploadedDocumentsApiResource = AdminDocumentsApiResource.add_resource('list')
        AdminListUploadedDocumentsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        AdminListUploadedDocumentsFunctionIntegration = apigateway.LambdaIntegration(AdminListExtractedDocuments)
        AdminListUploadedDocumentsApiMethod = AdminListUploadedDocumentsApiResource.add_method(
            'POST', 
            AdminListUploadedDocumentsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )


        # API /documents/approve
        AdminApproveDocumentApiResource = AdminDocumentsApiResource.add_resource('approve')
        AdminApproveDocumentApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminApproveDocumentFunctionIntegration = apigateway.LambdaIntegration(AdminApproveExtractedDocument)
        AdminApproveDocumentApiMethod = AdminApproveDocumentApiResource.add_method(
            'POST', 
            AdminApproveDocumentFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /documents/save
        AdminSaveDocumentApiResource = AdminDocumentsApiResource.add_resource('save')
        AdminSaveDocumentApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminSaveDocumentFunctionIntegration = apigateway.LambdaIntegration(AdminSaveExtractedDocument)
        AdminSaveDocumentApiMethod = AdminSaveDocumentApiResource.add_method(
            'POST', 
            AdminSaveDocumentFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /documents/delete
        AdminDeleteDocumentApiResource = AdminDocumentsApiResource.add_resource('delete')
        AdminDeleteDocumentApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminDeleteDocumentFunctionIntegration = apigateway.LambdaIntegration(AdminDeleteExtractedDocument)
        AdminDeleteDocumentApiMethod = AdminDeleteDocumentApiResource.add_method(
            'POST', 
            AdminDeleteDocumentFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /generates3downloadlink
        AdminGenerateS3DownloadLinkApiResource = AapAdminApi.root.add_resource('generates3downloadlink')
        AdminGenerateS3DownloadLinkApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGenerateS3DownloadLinkFunctionIntegration = apigateway.LambdaIntegration(AdminGenerateS3DownloadLink)
        AdminGenerateS3DownloadLinkApiMethod = AdminGenerateS3DownloadLinkApiResource.add_method(
            'GET', 
            AdminGenerateS3DownloadLinkFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /users
        AdminUsersApiResource = AapAdminApi.root.add_resource('users')
        AdminUsersApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /users/validate
        AdminUserValidateApiResource = AdminUsersApiResource.add_resource('validate')
        AdminUserValidateApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminUserValidateFunctionIntegration = apigateway.LambdaIntegration(AdminGetUserMerchantList)
        AdminUserValidateApiMethod = AdminUserValidateApiResource.add_method(
            'GET', 
            AdminUserValidateFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )
        
        AdminMutationUserFunctionIntegration = apigateway.LambdaIntegration(AdminCreateOrUpdateUser)
        AdminMutationUserApiMethod = AdminUsersApiResource.add_method(
            'POST', 
            AdminMutationUserFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        AdminGetUsersFunctionIntegration = apigateway.LambdaIntegration(AdminGetUsers)
        AdminGetUsersApiMethod = AdminUsersApiResource.add_method(
            'GET', 
            AdminGetUsersFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /users/delete
        AdminDeleteUserApiResource = AdminUsersApiResource.add_resource('delete')
        AdminDeleteUserApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminDeleteUserFunctionIntegration = apigateway.LambdaIntegration(AdminDeleteUser)
        AdminDeleteUserApiMethod = AdminDeleteUserApiResource.add_method(
            'POST', 
            AdminDeleteUserFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )


        # API /usermatrix
        AdminUserMatrixApiResource = AapAdminApi.root.add_resource('usermatrix')
        AdminUserMatrixApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /usermatrix/list
        AdminUserMatrixListApiResource = AdminUserMatrixApiResource.add_resource('list')
        AdminUserMatrixListApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        
        AdminListUserMatrixFunctionIntegration = apigateway.LambdaIntegration(AdminListUserMatrix)
        AdminListUserMatrixApiMethod = AdminUserMatrixListApiResource.add_method(
            'GET', 
            AdminListUserMatrixFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /usermatrix/detail
        AdminUserMatrixDetailApiResource = AdminUserMatrixApiResource.add_resource('detail')
        AdminUserMatrixDetailApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGetUserMatrixFunctionIntegration = apigateway.LambdaIntegration(AdminGetUserMatrix)
        AdminGetUserMatrixApiMethod = AdminUserMatrixDetailApiResource.add_method(
            'GET', 
            AdminGetUserMatrixFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )
        
        # API /roles
        AdminUserGroupApiResource = AapAdminApi.root.add_resource('roles')
        AdminUserGroupApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminMutationUserGroupFunctionIntegration = apigateway.LambdaIntegration(AdminCreateOrUpdateUserGroup)
        AdminMutationUserGroupApiMethod = AdminUserGroupApiResource.add_method(
            'POST', 
            AdminMutationUserGroupFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        AdminDeleteUserGroupFunctionIntegration = apigateway.LambdaIntegration(AdminDeleteUserGroup)
        AdminDeleteUserGroupApiMethod = AdminUserGroupApiResource.add_method(
            'DELETE', 
            AdminDeleteUserGroupFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        AdminGetUserGroupsFunctionIntegration = apigateway.LambdaIntegration(AdminGetUserGroups)
        AdminGetUserGroupsApiMethod = AdminUserGroupApiResource.add_method(
            'GET', 
            AdminGetUserGroupsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )


        # API /uploadeddocuments 
        AdminUploadedDocumentsResource = AapAdminApi.root.add_resource('uploadeddocuments')
        AdminUploadedDocumentsResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /uploadeddocuments/list
        AdminUploadedDocumentsListResource = AdminUploadedDocumentsResource.add_resource('list')
        AdminUploadedDocumentsListResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminListUploadedDocumentsFunctionIntegration = apigateway.LambdaIntegration(AdminListUploadedDocument)
        AdminListUploadedDocumentsApiMethod = AdminUploadedDocumentsListResource.add_method(
            'POST', 
            AdminListUploadedDocumentsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /audittrail 
        AdminAuditTrailResource = AapAdminApi.root.add_resource('audittrail')
        AdminAuditTrailResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /audittrail/list
        AdminAuditTrailListResource = AdminAuditTrailResource.add_resource('list')
        AdminAuditTrailListResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminListAuditTrailFunctionIntegration = apigateway.LambdaIntegration(AdminListAuditTrail)
        AdminListAuditTrailApiMethod = AdminAuditTrailListResource.add_method(
            'POST', 
            AdminListAuditTrailFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /export
        AdminExportApiResource = AapAdminApi.root.add_resource('export')
        AdminExportApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminTriggerExportCSVFunctionIntegration = apigateway.LambdaIntegration(AdminTriggerExportCSV)
        AdminTriggerExportCSVApiMethod = AdminExportApiResource.add_method(
            'POST', 
            AdminTriggerExportCSVFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /download
        AdminDownloadApiResource = AapAdminApi.root.add_resource('downloadjob')
        AdminDownloadApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGetDownloadJobStatusFunctionIntegration = apigateway.LambdaIntegration(AdminGetDownloadJobStatus)
        AdminGetDownloadJobStatusApiMethod = AdminDownloadApiResource.add_method(
            'GET', 
            AdminGetDownloadJobStatusFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /merchants
        AdminMerchantApiResource = AapAdminApi.root.add_resource('merchants')
        AdminMerchantApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        AdminGetMerchantFunctionIntegration = apigateway.LambdaIntegration(AdminGetMerchant)
        AdminGetMerchantApiMethod = AdminMerchantApiResource.add_method(
            'GET', 
            AdminGetMerchantFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        AdminCreateOrUpdateMerchantFunctionIntegration = apigateway.LambdaIntegration(AdminCreateOrUpdateMerchant)
        AdminCreateOrUpdateMerchantApiMethod = AdminMerchantApiResource.add_method(
            'POST', 
            AdminCreateOrUpdateMerchantFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /merchants/delete
        AdminDeleteMerchantApiResource = AdminMerchantApiResource.add_resource('delete')
        AdminDeleteMerchantApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminDeleteMerchantFunctionIntegration = apigateway.LambdaIntegration(AdminDeleteMerchant)
        AdminDeleteMerchantApiMethod = AdminDeleteMerchantApiResource.add_method(
            'POST', 
            AdminDeleteMerchantFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /grn
        AdminGRNApiResource = AapAdminApi.root.add_resource('grn')
        AdminGRNApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /grn/list
        AdminGRNListApiResource = AdminGRNApiResource.add_resource('list')
        AdminGRNListApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        AdminListExtractedGRNFunctionIntegration = apigateway.LambdaIntegration(AdminListExtractedGRN)
        AdminListExtractedGRNApiMethod = AdminGRNListApiResource.add_method(
            'POST', 
            AdminListExtractedGRNFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /po
        AdminPoApiResource = AapAdminApi.root.add_resource('po')
        AdminPoApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /po/list
        AdminPoListApiResource = AdminPoApiResource.add_resource('list')
        AdminPoListApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        AdminListExtractedPoFunctionIntegration = apigateway.LambdaIntegration(AdminListExtractedPo)
        AdminListExtractedPoApiMethod = AdminPoListApiResource.add_method(
            'POST', 
            AdminListExtractedPoFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /referralLetter
        AdminReferralLetterApiResource = AapAdminApi.root.add_resource('referralLetter')
        AdminReferralLetterApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /referralLetter/list
        AdminReferralLetterApiResource = AdminReferralLetterApiResource.add_resource('list')
        AdminReferralLetterApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        AdminListExtractedReferralLetterFunctionIntegration = apigateway.LambdaIntegration(AdminListExtractedReferralLetter)
        AdminListExtractedReferralLetterApiMethod = AdminReferralLetterApiResource.add_method(
            'POST', 
            AdminListExtractedReferralLetterFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        ## API /reconciliation
        AdminReconciliationApiResource = AapAdminApi.root.add_resource('reconciliation')
        AdminReconciliationApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /reconciliation/list
        AdminReconciliationSourceRecordsApiResource = AdminReconciliationApiResource.add_resource('list')
        AdminReconciliationSourceRecordsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        AdminListReconciliationSourceRecordsFunctionIntegration = apigateway.LambdaIntegration(AdminListReconciliationSourceRecords)
        AdminListReconciliationSourceRecordsApiMethod = AdminReconciliationSourceRecordsApiResource.add_method(
            'POST', 
            AdminListReconciliationSourceRecordsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /reconciliation/resultslist
        AdminReconciliationResultsApiResource = AdminReconciliationApiResource.add_resource('results')
        AdminReconciliationResultsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        
        AdminListReconciliationMatchingResultsFunctionIntegration = apigateway.LambdaIntegration(AdminListReconciliationMatchingResults)
        AdminListReconciliationMatchingResultsApiMethod = AdminReconciliationResultsApiResource.add_method(
            'POST', 
            AdminListReconciliationMatchingResultsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /reconciliation/trigger
        AdminReconciliationTriggerApiResource = AdminReconciliationApiResource.add_resource('trigger')
        AdminReconciliationTriggerApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        
        AdminReconciliationTriggerFunctionIntegration = apigateway.LambdaIntegration(AdminTriggerARReconciliation)
        AdminReconciliationTriggerApiMethod = AdminReconciliationTriggerApiResource.add_method(
            'POST',
            AdminReconciliationTriggerFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        
        # API /3waymatching
        Admin3WayMatchingApiResource = AapAdminApi.root.add_resource('3waymatching')
        Admin3WayMatchingApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        # API /3waymatching/resultslist
        Admin3WayMatchingResultsApiResource = Admin3WayMatchingApiResource.add_resource('results')
        Admin3WayMatchingResultsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )
        
        AdminList3WayMatchingResultsFunctionIntegration = apigateway.LambdaIntegration(AdminList3WayMatchingResults)
        AdminList3WayMatchingResultsApiMethod = Admin3WayMatchingResultsApiResource.add_method(
            'POST', 
            AdminList3WayMatchingResultsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /3waymatching/trigger
        Admin3WayMatchingTriggerApiResource = Admin3WayMatchingApiResource.add_resource('trigger')
        Admin3WayMatchingTriggerApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        Admin3WayMatchingTriggerFunctionIntegration = apigateway.LambdaIntegration(ThreeWayMatching)
        Admin3WayMatchingTriggerApiMethod = Admin3WayMatchingTriggerApiResource.add_method(
            'POST', 
            Admin3WayMatchingTriggerFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /3waymatching/netsuite-input-trigger
        Admin3WayMatchingNetsuiteInputTriggerApiResource = Admin3WayMatchingApiResource.add_resource('netsuite-input-trigger')
        Admin3WayMatchingNetsuiteInputTriggerApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        Admin3WayMatchingNetsuiteInputTriggerFunctionIntegration = apigateway.LambdaIntegration(GetNetsuiteDocuments)
        Admin3WayMatchingNetsuiteInputTriggerApiMethod = Admin3WayMatchingNetsuiteInputTriggerApiResource.add_method(
            'POST', 
            Admin3WayMatchingNetsuiteInputTriggerFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )

        # API /3waymatching/list
        Admin3WayMatchingSourceRecordsApiResource = Admin3WayMatchingApiResource.add_resource('list')
        Admin3WayMatchingSourceRecordsApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminList3WayMatchingSourceRecordsFunctionIntegration = apigateway.LambdaIntegration(AdminList3WayMatchingSourceRecords)
        AdminList3WayMatchingSourceRecordsApiMethod = Admin3WayMatchingSourceRecordsApiResource.add_method(
            'GET', 
            AdminList3WayMatchingSourceRecordsFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )        

        # API /jobstatus
        Admin3WayMatchingJobStatusApiResource = AapAdminApi.root.add_resource('jobstatus')
        Admin3WayMatchingJobStatusApiResource.add_cors_preflight(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_methods=apigateway.Cors.ALL_METHODS
        )

        AdminGetJobStatusFunctionIntegration = apigateway.LambdaIntegration(AdminGetJobStatus)
        AdminGetJobStatusApiMethod = Admin3WayMatchingJobStatusApiResource.add_method(
            'GET', 
            AdminGetJobStatusFunctionIntegration,
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO
        )