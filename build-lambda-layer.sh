#!/bin/bash
set -e

echo "Building Lambda Layer with aws-lambda-powertools..."

# Create layer directory structure
mkdir -p layer/python

# Install dependencies
pip install -r lambda/Functions_LiteDemo/AAP-LiteDemoS3EventProcessor/requirements.txt -t layer/python/

# Create zip file
cd layer
zip -r ../lambda-layer.zip python/
cd ..

# Upload to S3 (using the lite-demo bucket)
BUCKET_NAME=$(aws ssm get-parameter --name /LITE_DEMO/LiteDemo/S3BucketName --query Parameter.Value --output text)
aws s3 cp lambda-layer.zip s3://${BUCKET_NAME}/layers/lambda-layer.zip

# Publish layer
LAYER_ARN=$(aws lambda publish-layer-version \
    --layer-name LITE_DEMO-PowertoolsLayer \
    --description "AWS Lambda Powertools and dependencies" \
    --content S3Bucket=${BUCKET_NAME},S3Key=layers/lambda-layer.zip \
    --compatible-runtimes python3.12 \
    --query LayerVersionArn --output text)

echo "Layer created: ${LAYER_ARN}"

# Update SSM parameter
aws ssm put-parameter \
    --name AAP-LambdaBaseLayerArn \
    --value "${LAYER_ARN}" \
    --type String \
    --overwrite

echo "SSM parameter updated with layer ARN"

# Cleanup
rm -rf layer lambda-layer.zip

echo "Done! Redeploy your Lambda stack to use the new layer."
