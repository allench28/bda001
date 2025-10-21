env = 'dev'

AccountMap = {
    'dev': '954986424675',
    'staging': '',
    'prod': ''
}

RegionMap = {
    'dev': 'ap-southeast-1',
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
