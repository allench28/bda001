import os
from constructs import Construct
from .environment import *

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    RemovalPolicy,
    Tags,
    Duration
)

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'LITE_DEMO')

"""
Lite Demo S3 Bucket Stack
This stack creates S3 bucket for Lite Demo purposes.

Resources:
1. S3 Bucket with input/ and output/ folders structure
   - Versioning enabled
   - Server-side encryption
   - Lifecycle rules for cost optimization
"""

class LiteDemoS3BucketStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get bucket name from environment mapping
        s3_bucket_name = S3Map[env]['LITE_DEMO_BUCKET'].format(
            PROJECT_NAME.lower().replace('_', ''), 
            RegionMap[env]
        )

        # Create S3 Bucket
        lite_demo_bucket = s3.Bucket(
            self,
            f'{PROJECT_NAME}-LiteDemo-Bucket',
            bucket_name=s3_bucket_name,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id='DeleteOldVersions',
                    noncurrent_version_expiration=Duration.days(90),
                    enabled=True
                ),
                s3.LifecycleRule(
                    id='TransitionToIA',
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30)
                        )
                    ],
                    enabled=True
                )
            ],
            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.DELETE,
                        s3.HttpMethods.HEAD
                    ],
                    allowed_origins=['*'],
                    allowed_headers=['*'],
                    exposed_headers=[
                        'ETag',
                        'x-amz-server-side-encryption',
                        'x-amz-request-id',
                        'x-amz-id-2'
                    ],
                    max_age=3000
                )
            ]
        )

        # Add tags to all resources
        Tags.of(self).add('Project', PROJECT_NAME)
        Tags.of(self).add('Environment', env)
        Tags.of(self).add('Stack', 'LiteDemo-S3')
        Tags.of(self).add('ManagedBy', 'CDK')

        # Store reference
        self.bucket = lite_demo_bucket
        self.bucket_name = s3_bucket_name