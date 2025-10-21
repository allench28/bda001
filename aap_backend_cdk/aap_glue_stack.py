import os
from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_ssm as ssm,
    Tags
)
from constructs import Construct
from aap_backend_cdk.environment import *

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'AAP')
ACCOUNT_ID = os.environ.get('ACCOUNT_ID', '')

class AapBackendGlueStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add('PROJECT_NAME', 'AI-AGENT-PLATFORM')

        scripts_dir = './glue/scripts'

        AapLambdaRole = iam.Role.from_role_arn(
            self, f'{PROJECT_NAME}' + 'LambdaRoleMY',
            f'arn:aws:iam::{ACCOUNT_ID}:role/{PROJECT_NAME.title()}LambdaRoleMY'
        )

        AAPGlueRole = iam.Role(
            self, f'{PROJECT_NAME}-GlueExecRoleMY',
            role_name=f'{PROJECT_NAME}-GlueExecRoleMY',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            description=f'IAM Role to be used by {PROJECT_NAME} Glue Functions'
        )

        AAPGlueRole.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:*",
                    "sqs:*",
                    "dynamodb:*",
                    "glue:*",
                    "logs:*"
                ],
                resources=["*"]
            )
        )

        AAPReconGlueScriptS3 = s3.Bucket.from_bucket_name(
            self, f'{PROJECT_NAME}-GlueAssets',
            s3Map[env]['SMART_EYE_BUCKET'].format(PROJECT_NAME.lower(), env)
        )

        GlueJobScriptsResources = s3_deploy.Source.asset(scripts_dir)
        
        s3_deploy.BucketDeployment(
            self, f'{PROJECT_NAME}-GlueScriptDeployment',
            destination_bucket=AAPReconGlueScriptS3,
            sources=[GlueJobScriptsResources],
            destination_key_prefix='glue/scripts',
            role=AapLambdaRole,
            memory_limit=512,
            prune=False
        )

        scriptLocationUrl = 's3://{}/glue/scripts'.format(AAPReconGlueScriptS3.bucket_name)

        AAPReconS3ProcessGlueJob = glue.CfnJob(
            self, f'{PROJECT_NAME}-ProcessS3ReconciliationDocument',
            name=f'{PROJECT_NAME}-ProcessS3ReconciliationDocument',
            role=AAPGlueRole.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f'{scriptLocationUrl}/process_s3_recon_document_job.py'
            ),
            default_arguments={
                '--job-language': 'python',
                '--enable-metrics': '',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': f's3://{AAPReconGlueScriptS3.bucket_name}/glue/sparkHistoryLogs/',
                '--enable-job-insights': 'true',
                '--enable-observability-metrics': 'true',
                '--TempDir': f's3://{AAPReconGlueScriptS3.bucket_name}/glue/temporary/',
                '--additional-python-modules': 'boto3,pandas'
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=50
            ),
            notification_property=glue.CfnJob.NotificationPropertyProperty(
                notify_delay_after=1
            ),
            glue_version='5.0',
            worker_type='G.1X',
            number_of_workers=10,
            timeout=480,  # 480 minutes = 8 hours
            max_retries=5,
            description='ETL job for data processing'
        )