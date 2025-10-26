from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3_deployment as s3deploy,
    aws_ssm as ssm,
    RemovalPolicy,
    CfnOutput,
    Tags
)
import os
from constructs import Construct

PROJECT_NAME = os.environ.get('PROJECT_NAME', 'LITE_DEMO')

class LiteDemoFrontendStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket for hosting React app
        frontend_bucket = s3.Bucket(
            self,
            f'{PROJECT_NAME}-FrontendBucket',
            website_index_document='index.html',
            website_error_document='index.html',
            public_read_access=True,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=False,
                block_public_policy=False,
                ignore_public_acls=False,
                restrict_public_buckets=False
            ),
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # CloudFront distribution
        distribution = cloudfront.Distribution(
            self,
            f'{PROJECT_NAME}-FrontendDistribution',
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3Origin(frontend_bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS
            ),
            default_root_object='index.html',
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path='/index.html'
                ),
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path='/index.html'
                )
            ]
        )

        # Deploy React build to S3
        s3deploy.BucketDeployment(
            self,
            f'{PROJECT_NAME}-DeployFrontend',
            sources=[s3deploy.Source.asset('lite.frontend.web/build')],
            destination_bucket=frontend_bucket,
            distribution=distribution,
            distribution_paths=['/*']
        )

        # Outputs
        CfnOutput(
            self,
            'CloudFrontURL',
            value=f'https://{distribution.distribution_domain_name}',
            description='Frontend CloudFront URL'
        )

        CfnOutput(
            self,
            'FrontendBucketName',
            value=frontend_bucket.bucket_name,
            description='Frontend S3 Bucket Name'
        )

        # Add tags
        Tags.of(self).add('Project', PROJECT_NAME)
        Tags.of(self).add('Stack', 'Frontend')
        Tags.of(self).add('ManagedBy', 'CDK')

        self.distribution = distribution
        self.bucket = frontend_bucket
