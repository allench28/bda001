import * as cdk from 'aws-cdk-lib';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import { Construct } from 'constructs';

export class ReactCloudFrontStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const apiGatewayUrl = ssm.StringParameter.valueForStringParameter(
      this,
      'lite-demo-api-url-gw'
    );

    const bucket = new s3.Bucket(this, 'ReactAppBucket', {
      websiteIndexDocument: 'index.html',
      websiteErrorDocument: 'index.html',
      publicReadAccess: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ACLS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const distribution = new cloudfront.CloudFrontWebDistribution(this, 'Distribution', {
      originConfigs: [{
        s3OriginSource: {
          s3BucketSource: bucket
        },
        behaviors: [{ isDefaultBehavior: true }]
      }],
      errorConfigurations: [{
        errorCode: 404,
        responseCode: 200,
        responsePagePath: '/index.html'
      }, {
        errorCode: 403,
        responseCode: 200,
        responsePagePath: '/index.html'
      }]
    });

    new s3deploy.BucketDeployment(this, 'DeployWebsite', {
      sources: [s3deploy.Source.asset('../build')],
      destinationBucket: bucket,
      distribution,
      distributionPaths: ['/*']
    });

    new cdk.CfnOutput(this, 'CloudFrontURL', {
      value: `https://${distribution.distributionDomainName}`,
      description: 'CloudFront Distribution URL'
    });

    new cdk.CfnOutput(this, 'ApiGatewayURL', {
      value: apiGatewayUrl,
      description: 'API Gateway URL from Parameter Store'
    });

    new cdk.CfnOutput(this, 'BucketName', {
      value: bucket.bucketName,
      description: 'S3 Bucket Name'
    });
  }
}
