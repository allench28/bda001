
import os

env = os.environ.get('ENV', 'dev')

# Force deployment to us-east-1 only
REGION = 'us-east-1'

AccountMap = {
    'dev': 'auto-detect',
    'staging': 'auto-detect', 
    'prod': 'auto-detect'
}

RegionMap = {
    'dev': REGION,
    'staging': REGION,
    'prod': REGION
}

S3Map = {
    'dev': {
        'LITE_DEMO_BUCKET': '{}-documents-bucket-{}',
    },
    'staging': {
        'LITE_DEMO_BUCKET': '{}-documents-bucket-{}',
    },
    'demo': {
        'LITE_DEMO_BUCKET': '{}-documents-bucket-{}',
    },
    'prod': {
        'LITE_DEMO_BUCKET': '{}-documents-bucket-{}'
    }
}

DynamoDBTableMap = {
    'dev': {
        'LITE_DEMO_DOCUMENTS': '{}-documents-table',
    },
    'staging': {
        'LITE_DEMO_DOCUMENTS': '{}-documents-table',
    },
    'demo': {
        'LITE_DEMO_DOCUMENTS': '{}-documents-table',
    },
    'prod': {
        'LITE_DEMO_DOCUMENTS': '{}-documents-table'
    }
}

# BDA Configuration - ARNs will be built dynamically in the stack using self.account and self.region
BDAMap = {
    'dev': {
        'PROJECT_ARN': 'auto-generated-by-stack',
        'PROFILE_ARN': 'auto-generated-by-stack'
    },
    'staging': {
        'PROJECT_ARN': 'auto-generated-by-stack',
        'PROFILE_ARN': 'auto-generated-by-stack'
    },
    'demo': {
        'PROJECT_ARN': 'auto-generated-by-stack',
        'PROFILE_ARN': 'auto-generated-by-stack'
    },
    'prod': {
        'PROJECT_ARN': 'auto-generated-by-stack',
        'PROFILE_ARN': 'auto-generated-by-stack'
    }
}
