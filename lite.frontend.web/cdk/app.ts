#!/usr/bin/env node

import * as cdk from 'aws-cdk-lib';
import { ReactCloudFrontStack } from './stack';
import * as dotenv from 'dotenv';
import * as fs from 'fs';

// Load .env file from parent directory
const envPath = '../.env';
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

const app = new cdk.App();
const region = process.env.REACT_APP_AWS_REGION;
new ReactCloudFrontStack(app, 'LiteFrontendStack', {
  env: { region }
});
