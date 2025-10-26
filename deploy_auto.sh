#!/bin/bash

# Zero-Configuration Deployment Script
# Auto-detects AWS account and region from your current AWS configuration

set -e

# Change to script directory
cd "$(dirname "$0")"

echo "🚀 Auto-Detecting AWS Configuration"
echo "===================================="

# Auto-detect AWS account, force us-east-1 region
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
REGION="us-east-1"

if [ -z "$ACCOUNT_ID" ]; then
    echo "❌ Unable to detect AWS account!"
    echo "📋 Please configure AWS CLI first:"
    echo "   aws configure"
    echo "   # OR use IAM role on EC2"
    exit 1
fi

# Set defaults (can be overridden by environment variables)
export PROJECT_NAME=${PROJECT_NAME:-LITE_DEMO}
export ENV=${ENV:-dev}

echo "✅ Configuration:"
echo "   Account ID: $ACCOUNT_ID (auto-detected)"
echo "   Region: $REGION (forced to us-east-1)"
echo "   Project: $PROJECT_NAME"
echo "   Environment: $ENV"
echo ""

# Setup Python environment
if [ ! -d ".venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv .venv
    echo "📦 Installing Python dependencies..."
    source .venv/bin/activate
    pip install -r requirements.txt
else
    echo "✅ Virtual environment exists"
    source .venv/bin/activate
fi

# Bootstrap CDK if needed
echo "🔧 Checking CDK bootstrap..."
if ! aws cloudformation describe-stacks --stack-name CDKToolkit --region $REGION >/dev/null 2>&1; then
    echo "🚀 Bootstrapping CDK..."
    cdk bootstrap aws://$ACCOUNT_ID/$REGION
fi

# Create required SSM parameters
echo "📝 Creating required SSM parameters..."
aws ssm put-parameter \
  --name "AAP-LambdaBaseLayerArn" \
  --value "none" \
  --type "String" \
  --region $REGION \
  --overwrite 2>/dev/null || echo "   Parameter already exists or created"

# Create placeholder build folder for CDK synth (will be replaced with real build)
echo "📝 Creating placeholder build folder..."
mkdir -p lite.frontend.web/build
echo "<!DOCTYPE html><html><body>Placeholder</body></html>" > lite.frontend.web/build/index.html

# Deploy backend stacks first (excludes frontend)
echo "📦 Deploying backend stacks..."
cdk deploy LiteDemoDynamoDBStack-${ENV} LiteDemoS3BucketStack-${ENV} LiteDemoBDAProjectStack-${ENV} LiteDemoSNSStack-${ENV} LiteDemoApiGatewayLambdaStack-${ENV} LiteDemoSftpStack-${ENV} --require-approval never

# Build frontend AFTER backend is deployed
echo "🎨 Building frontend with current API URL..."
./build-frontend.sh

# Deploy frontend stack with real build
echo "📦 Deploying frontend stack..."
cdk deploy LiteDemoFrontendStack-${ENV} --require-approval never

echo ""
echo "✅ Deployment completed successfully!"
echo ""

# Get outputs
echo "📋 Your Deployment:"
API_URL=$(aws ssm get-parameter --name "lite-demo-api-url-gw" --region $REGION --query "Parameter.Value" --output text 2>/dev/null || echo "Not available yet")
S3_BUCKET=$(aws ssm get-parameter --name "/${PROJECT_NAME}/LiteDemo/S3BucketName" --region $REGION --query "Parameter.Value" --output text 2>/dev/null || echo "Not available yet")
SNS_TOPIC=$(aws ssm get-parameter --name "/${PROJECT_NAME}/LiteDemo/SNSTopicArn" --region $REGION --query "Parameter.Value" --output text 2>/dev/null || echo "Not available yet")

# Get CloudFront URL from Frontend stack
CLOUDFRONT_URL=$(aws cloudformation describe-stacks --stack-name "LiteDemoFrontendStack-${ENV}" --region $REGION --query "Stacks[0].Outputs[?OutputKey=='CloudFrontURL'].OutputValue" --output text 2>/dev/null || echo "Not available yet")

# Get SFTP details from SFTP stack
SFTP_ENDPOINT=$(aws cloudformation describe-stacks --stack-name "LiteDemoSftpStack-${ENV}" --region $REGION --query "Stacks[0].Outputs[?OutputKey=='SftpEndpoint'].OutputValue" --output text 2>/dev/null || echo "Not available yet")
SFTP_USERNAME=$(aws cloudformation describe-stacks --stack-name "LiteDemoSftpStack-${ENV}" --region $REGION --query "Stacks[0].Outputs[?OutputKey=='SftpUsername'].OutputValue" --output text 2>/dev/null || echo "Not available yet")

echo "🪣 S3 Bucket: $S3_BUCKET"
echo "📧 SNS Topic: $SNS_TOPIC"
echo "☁️ CloudFront URL: $CLOUDFRONT_URL"
echo "💾 SFTP Endpoint: $SFTP_ENDPOINT"
echo "👤 SFTP Username: $SFTP_USERNAME"

echo ""
echo "🔧 Next Steps:"
echo "1. Access Web Portal:"
echo "   $CLOUDFRONT_URL"
echo ""
echo "2. Subscribe to SNS topic for notifications:"
echo "   aws sns subscribe --topic-arn $SNS_TOPIC --protocol email --notification-endpoint your-email@example.com --region $REGION"
echo ""
echo "3. Upload master data files via SFTP:"
echo "   sftp -i <key-file-path/key-name.pem> $SFTP_USERNAME@$SFTP_ENDPOINT"
echo "   put <file-path> buyer/buyer_master_data.xlsx"
echo "   put <file-path> product/productcatalog.xlsx"
echo ""
echo "4. Check CloudWatch Logs for Lambda execution details"