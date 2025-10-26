#!/bin/bash

set -e

REGION="us-east-1"
# Parse argumen --region us-east-1
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --region)
      REGION="$2"
      shift # skip value
      ;;
  esac
  shift
done
PARAM_NAME="lite-demo-api-url-gw"

echo "Step 1: Fetching API Gateway URL from Parameter Store..."
API_GATEWAY_URL=$(aws ssm get-parameter --name $PARAM_NAME --region $REGION --query 'Parameter.Value' --output text)

if [ -z "$API_GATEWAY_URL" ]; then
  echo "Error: Could not fetch API Gateway URL from Parameter Store"
  exit 1
fi

echo "API Gateway URL: $API_GATEWAY_URL"

echo "Step 2: Creating .env file..."
echo "REACT_APP_API_BASE_URL=$API_GATEWAY_URL" > .env

echo "Step 3: Building React app..."
npm run build

echo "Step 4: Installing CDK dependencies..."
cd cdk && npm install

echo "Step 5: Deploying to S3 and CloudFront..."
cdk deploy --require-approval never

echo "Deployment complete!"
