import os

import aws_cdk as cdk

# Removed AAP backend imports - only using lite_demo_cdk

from lite_demo_cdk.lite_demo_dynamodb_stack import LiteDemoDynamoDBStack
from lite_demo_cdk.lite_demo_apigateway_lambda_stack import LiteDemoApiGatewayLambdaStack
from lite_demo_cdk.lite_demo_s3_bucket_stack import LiteDemoS3BucketStack
from lite_demo_cdk.lite_demo_bda_project_stack import LiteDemoBDAProjectStack
from lite_demo_cdk.lite_demo_sns_stack import LiteDemoSNSStack
from lite_demo_cdk.lite_demo_sftp_stack import LiteDemoSftpStack
from lite_demo_cdk.environment import env, RegionMap

app = cdk.App()

# Force us-east-1 region
region = 'us-east-1'

# Lite Demo Stacks Only

lite_demo_dynamodb_stack = LiteDemoDynamoDBStack(
    app, 
    "LiteDemoDynamoDBStack-{}".format(env),
    env=cdk.Environment(region=region)
)
lite_demo_s3_stack = LiteDemoS3BucketStack(
    app,
    "LiteDemoS3BucketStack-{}".format(env),
    env=cdk.Environment(region=region)
)
lite_demo_bda_stack = LiteDemoBDAProjectStack(
    app,
    "LiteDemoBDAProjectStack-{}".format(env),
    s3_stack=lite_demo_s3_stack,
    env=cdk.Environment(region=region)
)
lite_demo_sns_stack = LiteDemoSNSStack(
    app,
    "LiteDemoSNSStack-{}".format(env),
    env=cdk.Environment(region=region)
)
LiteDemoApiGatewayLambdaStack(
    app, 
    "LiteDemoApiGatewayLambdaStack-{}".format(env),
    dynamodb_stack=lite_demo_dynamodb_stack,
    s3_stack=lite_demo_s3_stack,
    bda_stack=lite_demo_bda_stack,
    sns_stack=lite_demo_sns_stack,
    env=cdk.Environment(region=region)
)

LiteDemoSftpStack(
    app, 
    "LiteDemoSftpStack-{}".format(env),
    s3_stack=lite_demo_s3_stack,
    env=cdk.Environment(region=region)
)

app.synth()
