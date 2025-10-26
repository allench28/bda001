#!/bin/bash

# Zero-Configuration Deployment Script
# Auto-detects AWS account and region from your current AWS configuration

set -e

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

# Build frontend
echo "🎨 Building frontend..."
./build-frontend.sh

# Deploy stacks
echo "📦 Deploying stacks..."
cdk deploy --all --require-approval never

echo ""
echo "✅ Deployment completed successfully!"
echo ""

# Get outputs
echo "📋 Your Deployment:"
API_URL=$(aws ssm get-parameter --name "lite-demo-api-url-gw" --region $REGION --query "Parameter.Value" --output text 2>/dev/null || echo "Not available yet")
echo "🌐 API Gateway URL: $API_URL"

echo ""
echo "🔧 Next Steps:"
echo "1. Test API: python test_api.py $API_URL"
echo "2. Add SNS email subscription in AWS Console"
echo "3. Upload test documents to trigger processing"