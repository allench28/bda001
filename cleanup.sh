#!/bin/bash

# Cleanup Script - Destroys all deployed CDK resources
# WARNING: This will delete all resources created by the CDK deployment

set -e

echo "⚠️  WARNING: Resource Cleanup"
echo "=============================="
echo "This will DELETE all deployed resources including:"
echo "  - Lambda functions"
echo "  - API Gateway"
echo "  - DynamoDB tables"
echo "  - S3 buckets (and all contents)"
echo "  - SNS topics"
echo "  - CloudFront distribution"
echo "  - Frontend S3 bucket"
echo "  - IAM roles and policies"
echo "  - SSM parameters"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Cleanup cancelled"
    exit 0
fi

echo ""
read -p "Type 'DELETE' to confirm: " confirm2

if [ "$confirm2" != "DELETE" ]; then
    echo "❌ Cleanup cancelled"
    exit 0
fi

# Auto-detect configuration
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
REGION="us-east-1"
export PROJECT_NAME=${PROJECT_NAME:-LITE_DEMO}
export ENV=${ENV:-dev}

if [ -z "$ACCOUNT_ID" ]; then
    echo "❌ Unable to detect AWS account!"
    exit 1
fi

echo ""
echo "🗑️  Starting cleanup..."
echo "   Account: $ACCOUNT_ID"
echo "   Region: $REGION"
echo "   Project: $PROJECT_NAME"
echo ""

# Activate virtual environment if exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Empty S3 buckets before deletion (CDK can't delete non-empty buckets)
echo "📦 Emptying S3 buckets..."
BUCKET_NAME="${PROJECT_NAME,,}-lite-demo-bucket-${ENV}"
BUCKET_NAME=$(echo $BUCKET_NAME | tr '_' '-')

if aws s3 ls "s3://${BUCKET_NAME}" 2>/dev/null; then
    echo "   Emptying bucket: ${BUCKET_NAME}"
    aws s3 rm "s3://${BUCKET_NAME}" --recursive --region $REGION || true
fi

# Empty frontend bucket
echo "   Checking for frontend buckets..."
for bucket in $(aws s3 ls --region $REGION | grep "litedemofrontendstack" | awk '{print $3}'); do
    echo "   Emptying frontend bucket: ${bucket}"
    aws s3 rm "s3://${bucket}" --recursive --region $REGION || true
done

# Destroy CDK stacks
echo ""
echo "🔥 Destroying CDK stacks..."
cdk destroy --all --force

# Clean up SSM parameters
echo ""
echo "🧹 Cleaning up SSM parameters..."
aws ssm delete-parameter --name "AAP-LambdaBaseLayerArn" --region $REGION 2>/dev/null || true
aws ssm delete-parameter --name "lite-demo-api-url-gw" --region $REGION 2>/dev/null || true
aws ssm delete-parameter --name "/${PROJECT_NAME}/LiteDemo/S3BucketName" --region $REGION 2>/dev/null || true
aws ssm delete-parameter --name "/${PROJECT_NAME}/LiteDemo/SNSTopicArn" --region $REGION 2>/dev/null || true

echo ""
echo "✅ Cleanup completed!"
echo ""
echo "📋 Remaining manual cleanup (if needed):"
echo "   - CloudWatch Log Groups (will auto-expire)"
echo "   - CDK Bootstrap stack (if you want to remove CDK completely)"
echo "     Run: aws cloudformation delete-stack --stack-name CDKToolkit --region $REGION"
