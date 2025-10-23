#!/bin/bash

source .venv/bin/activate
# Cara menjalankan script:
# 1. Pastikan sudah berada di direktori yang benar
# 2. Berikan permission execute: chmod +x deploy.sh
# 3. Jalankan dengan parameter environment, contoh:
#    ./deploy.sh dev
#    ./deploy.sh prod
cdk bootstrap
cdk deploy LiteDemoDynamoDBStack-$1 \
  LiteDemoS3BucketStack-$1 \
  LiteDemoBDAProjectStack-$1 \
  LiteDemoApiGatewayLambdaStack-$1 \
  --require-approval never
