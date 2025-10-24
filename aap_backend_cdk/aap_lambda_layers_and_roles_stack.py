import os
import subprocess

from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_ssm as ssm,
    Tags
)
from constructs import Construct

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')

class AapBackendLambdaLayersAndRolesStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')

        # IAM Roles
        AapLambdaRole = iam.Role(
            self, f'{PROJECT_NAME}' + 'LambdaRoleMY',
            role_name=f'{PROJECT_NAME.title()}' + 'LambdaRoleMY' + self.region,
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            description='IAM Role to be used by Lambda Functions'
        )

        AapLambdaRole.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:*",
                    "kms:*",
                    "logs:*",
                    "secretsmanager:*",
                    "sqs:*",
                    "xray:*",
                    "lambda:*",
                    "es:*",
                    "rds-data:*",
                    "firehose:*",
                    "s3:*",
                    "cognito-idp:*",
                    "geo:*",
                    "sns:*",
                    "ses:*",
                    "states:*",
                    "textract:*",
                    "glue:*",
                    "athena:*",
                    "redshift-data:*",
                    "sts:*",
                    "route53:*",
                    "comprehend:*",
                    "apigateway:*",
                    "cloudfront:*",
                    "ec2:*",
                    "elasticache:*",
                    "iam:*",
                    "kendra:*",
                    "sagemaker:*",
                    "codebuild:*",
                    "amplify:*",
                    "quicksight:*",
                    "ssm:*",
                    "acm:*",
                    "events:*",
                    "execute-api:*",
                    "execute-api:ManageConnections",
                    "bedrock:*"
                ],
                resources=["*"]
            )
        )

        TextractAsyncRole = iam.Role(
            self, f'{PROJECT_NAME}' + 'TextractAsyncRoleMY',
            role_name=f'{PROJECT_NAME.title()}' + 'TextractAsyncRoleMY' + self.region,
            assumed_by=iam.ServicePrincipal('textract.amazonaws.com'),
            description='IAM Role to be used by Textract Async Functions'
        )

        TextractAsyncRole.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sns:Publish",
                ],
                resources=["*"]
            )
        )

        EmailAttachmentTextractRole = iam.Role(
            self, f'{PROJECT_NAME}' + 'EmailAttachmentTextractRoleMY',
            role_name=f'{PROJECT_NAME.title()}' + 'EmailAttachmentTextractRoleMY' + self.region,
            assumed_by=iam.ServicePrincipal('states.amazonaws.com'),
            description='IAM Role to be used by Presales Step Functions'
        )

        EmailAttachmentTextractRole.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "lambda:InvokeFunction",
                ],
                resources=[
                    "arn:aws:lambda:ap-southeast-5:582554346432:function:AAP-PreSalesExtractEmail",
                    "arn:aws:lambda:ap-southeast-5:582554346432:function:AAP-PresalesEmailAttachmentAnalysis",
                    "arn:aws:lambda:ap-southeast-5:582554346432:function:AAP-PreSalesExtractEmail:*",
                    "arn:aws:lambda:ap-southeast-5:582554346432:function:AAP-PresalesEmailAttachmentAnalysis:*",
                    "arn:aws:lambda:ap-southeast-5:582554346432:function:AAP-PresalesEmailStepFunctionCheck",
                    "arn:aws:lambda:ap-southeast-5:582554346432:function:AAP-PresalesEmailStepFunctionCheck:*"
                ]
            )
        )

        # Lambda Layers
        # delete the lambda/Layers/{PackageName}/python folder if you wish to install another version
        # some packages are imported directly from zip as it requires pre-compile packages, therefore not installed through 'pip'


        LambdaBaseLayer = lambda_.LayerVersion(
            self, 'LambdaBaseLayer',
            layer_version_name='{}-LambdaBaseLayer'.format(PROJECT_NAME.title()),
            code=self.create_dependencies_layer('./lambda/Layers/LambdaBase'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer with AwsLambdaPowerTools, SimpleJson, Requests, aws4auth and elasticsearch Dependency",
            removal_policy=RemovalPolicy.RETAIN
        )

        GenericLayer = lambda_.LayerVersion(
            self, 'GenericLayer',
            layer_version_name='{}-GenericLayer'.format(PROJECT_NAME.title()),
            code=lambda_.Code.from_asset('./lambda/Layers/Generic'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer for common code",
            removal_policy=RemovalPolicy.RETAIN
        )

        PyPDF2Layer = lambda_.LayerVersion(
            self, 'PyPDF2Layer',
            layer_version_name='{}-PyPDF2Layer'.format(PROJECT_NAME.title()),
            code=lambda_.Code.from_asset('./lambda/Layers/PyPDF2'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer for PyPDF2",
            removal_policy=RemovalPolicy.RETAIN
        )

        # AwsWranglerLayer = lambda_.LayerVersion.from_layer_version_arn(
        #     self, 'AwsWranglerLayer',
        #     "arn:aws:lambda:ap-southeast-5:336392948345:layer:AWSSDKPandas-Python312:16"
        # )

        PillowLayer = lambda_.LayerVersion(
            self, 'PillowLayer',
            layer_version_name='{}-PillowLayer'.format(PROJECT_NAME.title()),
            code=self.create_dependencies_layer('./lambda/Layers/PIL'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="PIL library for image stuff",
            removal_policy=RemovalPolicy.RETAIN
        )

        QRCodeLayer = lambda_.LayerVersion(
            self, 'QRCodeLayer',
            layer_version_name='{}-QRCodeLayer'.format(PROJECT_NAME.title()),
            code=self.create_dependencies_layer('./lambda/Layers/QRCode'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer with QRCode Dependency",
            removal_policy=RemovalPolicy.RETAIN
        )

        ExtractEmailMsgLayer = lambda_.LayerVersion(
            self, 'ExtractEmailMsgLayer',
            layer_version_name='{}-ExtractEmailMsgLayer'.format(PROJECT_NAME.title()),
            code=self.create_dependencies_layer('./lambda/Layers/ExtractEmailMsg'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer with ExtractEmailMsg Dependency",
            removal_policy=RemovalPolicy.RETAIN
        )

        PillowPymupdfLayer = lambda_.LayerVersion(
            self, 'PillowPymupdfLayer',
            layer_version_name='{}-PillowPymupdfLayer'.format(PROJECT_NAME.title()),
            code=self.create_dependencies_layer('./lambda/Layers/PillowPymupdfLayer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer with Pillow and Pymupdf Dependency",
            removal_policy=RemovalPolicy.RETAIN
        )

        # AwsPandasLayer = lambda_.LayerVersion(
        #     self, 'AwsPandasLayer',
        #     layer_version_name='{}-AwsPandasLayer'.format(PROJECT_NAME.title()),
        #     code=self.create_dependencies_layer('./lambda/Layers/Pandas'),
        #     compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
        #     description="Lambda Layer with Pillow and Pymupdf Dependency",
        #     removal_policy=RemovalPolicy.RETAIN
        # )

        AwsPandasLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'AwsPandasLayer',
            "arn:aws:lambda:ap-southeast-5:770693421928:layer:Klayers-p312-pandas:10"
        )

        # AwsWranglerLayer = lambda_.LayerVersion(
        #     self, 'AwsWranglerLayer',
        #     layer_version_name='{}-AwsWranglerLayer'.format(PROJECT_NAME.title()),
        #     code=self.create_dependencies_layer('./lambda/Layers/Pandas'),
        #     compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
        #     description="Lambda Layer with Pillow and Pymupdf Dependency",
        #     removal_policy=RemovalPolicy.RETAIN
        # )             

        AwsWranglerLayer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'AwsWranglerLayer',
            "arn:aws:lambda:ap-southeast-5:770693421928:layer:Klayers-p312-pandas:10"
        )

        OpenpyxlLayer = lambda_.LayerVersion(
            self, 'OpenpyxlLayer',
            layer_version_name='{}-OpenpyxlLayer'.format(PROJECT_NAME.title()),
            code=self.create_dependencies_layer('./lambda/Layers/Openpyxl'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Lambda Layer with Openpyxl Dependency",
            removal_policy=RemovalPolicy.RETAIN
        )

        # # SSM To Store Layer Version
        ssm.StringParameter(
            self, 'AwsPandasLayerArn',
            string_value=AwsPandasLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for AwsPandasLayer',
            parameter_name=f'{PROJECT_NAME}' + '-AwsPandasLayerArn'
        )


        ssm.StringParameter(
            self, 'LambdaBaseLayerArn',
            string_value=LambdaBaseLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for LambdaBaseLayer',
            parameter_name=f'{PROJECT_NAME}' + '-LambdaBaseLayerArn'
        )

        ssm.StringParameter(
            self, 'GenericLayerArn',
            string_value=GenericLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for GenericLayer',
            parameter_name=f'{PROJECT_NAME}' + '-GenericLayerArn'
        )

        ssm.StringParameter(
            self, 'PyPDF2LayerArn',
            string_value=PyPDF2Layer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for PyPDF2Layer',
            parameter_name=f'{PROJECT_NAME}' + '-PyPDF2LayerArn'
        )

        ssm.StringParameter(
            self, 'AwsWranglerLayerArn',
            string_value=AwsWranglerLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for AwsWranglerLayer',
            parameter_name=f'{PROJECT_NAME}' + '-AwsWranglerLayerArn'
        )

        ssm.StringParameter(
            self, 'PillowLayerArn',
            string_value=PillowLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for PillowLayer',
            parameter_name=f'{PROJECT_NAME}' + '-PillowLayerArn'
        )

        ssm.StringParameter(
            self, 'QRCodeLayerArn',
            string_value=QRCodeLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for QRCodeLayer',
            parameter_name=f'{PROJECT_NAME}' + '-QRCodeLayerArn'
        )

        ssm.StringParameter(
            self, 'ExtractEmailMsgLayerArn',
            string_value=ExtractEmailMsgLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for ExtractEmailMsgLayer',
            parameter_name=f'{PROJECT_NAME}' + '-ExtractEmailMsgLayerArn'
        )

        ssm.StringParameter(
            self, 'PillowPymupdfLayerArn',
            string_value=PillowPymupdfLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for PillowPymupdfLayer',
            parameter_name=f'{PROJECT_NAME}' + '-PillowPymupdfLayerArn'
        )

        ssm.StringParameter(
            self, 'OpenpyxlLayerArn',
            string_value=OpenpyxlLayer.layer_version_arn,
            type=ssm.ParameterType.STRING,
            description='Arn for OpenpyxlLayer',
            parameter_name=f'{PROJECT_NAME}' + '-OpenpyxlLayerArn'
        )

         # Output Exports
        CfnOutput(
            self, f'{PROJECT_NAME}' + 'LambdaRoleArn',
            export_name=f'{PROJECT_NAME}' + 'LambdaRoleArn',
            value=AapLambdaRole.role_arn
        )

        CfnOutput(
            self, f'{PROJECT_NAME}' + 'TextractAsyncRoleArn',
            export_name=f'{PROJECT_NAME}' + 'TextractAsyncRoleArn',
            value=TextractAsyncRole.role_arn
        )

    def create_dependencies_layer(self, localPath):
        main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        while localPath[0] == '.' or localPath[0] == '/':
            localPath = localPath[1:]

        layerPath = f'{main_dir}/{localPath}'

        if not os.path.exists(f'{layerPath}/python'):
            subprocess.check_call(f'pip install -r {layerPath}/requirements.txt -t {layerPath}/python', shell=True)
        return lambda_.Code.from_asset(layerPath)
