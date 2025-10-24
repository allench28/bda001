
env = 'dev'

AccountMap = {
    'dev': '954986424675',
    'staging': '',
    'prod': ''
}

RegionMap = {
    'dev': 'us-east-1',
    'staging': 'ap-southeast-1',
    'prod': 'ap-southeast-1'
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

BDAMap = {
    'dev': {
        'PROJECT_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-project/4342d60f9f81',
        'PROFILE_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-profile/us.data-automation-v1'
    },
    'staging': {
        'PROJECT_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-project/your-project-id',
        'PROFILE_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-profile/your-profile-id'
    },
    'demo': {
        'PROJECT_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-project/your-project-id',
        'PROFILE_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-profile/your-profile-id'
    },
    'prod': {
        'PROJECT_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-project/your-project-id',
        'PROFILE_ARN': 'arn:aws:bedrock:us-east-1:954986424675:data-automation-profile/your-profile-id'
    }
}
