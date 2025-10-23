import os

import aws_cdk as cdk

from aap_backend_cdk.aap_lambda_layers_and_roles_stack import AapBackendLambdaLayersAndRolesStack
from aap_backend_cdk.aap_general_lambda_stack import AapBackendLambdaGeneralStack
from aap_backend_cdk.aap_dynamodb_lambda_stack import AapBackendLambdaDynamoDbStack
from aap_backend_cdk.aap_eventbridge_lambda_stack import AapBackendLambdaEventBridgeStack
from aap_backend_cdk.aap_s3_lambda_stack import AapBackendLambdaS3Stack
from aap_backend_cdk.aap_sqs_lambda_stack import AapBackendLambdaSqsStack
from aap_backend_cdk.aap_bda_lambda_stack import AapBackendBDALambdaStack
from aap_backend_cdk.aap_glue_stack import AapBackendGlueStack 
from aap_backend_cdk.aap_step_function_lambda_stack import AapBackendLambdaStepFunctionStack
from aap_backend_cdk.aap_cloudwatch_stack import AapCloudWatchStack
from aap_backend_cdk.environment import env

from aap_backendadmin_cdk.aap_apigateway_lambda_stack import AapBackendAdminLambdaApiGatewayStack

from lite_demo_cdk.lite_demo_dynamodb_stack import LiteDemoDynamoDBStack
from lite_demo_cdk.lite_demo_apigateway_lambda_stack import LiteDemoApiGatewayLambdaStack
from lite_demo_cdk.lite_demo_s3_bucket_stack import LiteDemoS3BucketStack
from lite_demo_cdk.environment import env as lite_demo_env

app = cdk.App()

AapBackendLambdaLayersAndRolesStack(app, "AapBackendLambdaLayersAndRolesStack-{}".format(env), env=cdk.Environment(region='us-east-1'))
# AapBackendLambdaGeneralStack(app, "AapBackendLambdaGeneralStack-{}".format(env))
# AapBackendLambdaDynamoDbStack(app, "AapBackendLambdaDynamoDbStack-{}".format(env))
# AapBackendLambdaEventBridgeStack(app, "AapBackendLambdaEventBridgeStack-{}".format(env))
# AapBackendLambdaS3Stack(app, "AapBackendLambdaS3Stack-{}".format(env))
# AapBackendLambdaSqsStack(app, "AapBackendLambdaSqsStack-{}".format(env))
# AapBackendGlueStack(app, "AapBackendGlueStack-{}".format(env)) 
# AapBackendBDALambdaStack(app, "AapBackendBDALambdaStack-{}".format(env), env=cdk.Environment(region=os.environ.get('BDA_REGION', 'us-east-1')))
# AapBackendLambdaStepFunctionStack(app, "AapBackendLambdaStepFunctionStack-{}".format(env))
# AapCloudWatchStack(app, "AapCloudWatchStack-{}".format(env))

# AapBackendAdminLambdaApiGatewayStack(app, "AapBackendAdminLambdaApiGatewayStack-{}".format(env))

# Lite Demo Stacks

lite_demo_dynamodb_stack = LiteDemoDynamoDBStack(
    app, 
    "LiteDemoDynamoDBStack-{}".format(lite_demo_env),
    env=cdk.Environment(region='us-east-1')
)
lite_demo_s3_stack = LiteDemoS3BucketStack(
    app,
    "LiteDemoS3BucketStack-{}".format(lite_demo_env),
    env=cdk.Environment(region='us-east-1')
)
LiteDemoApiGatewayLambdaStack(
    app, 
    "LiteDemoApiGatewayLambdaStack-{}".format(lite_demo_env),
    dynamodb_stack=lite_demo_dynamodb_stack,
    s3_stack=lite_demo_s3_stack,
    env=cdk.Environment(region='us-east-1')
)

app.synth()
