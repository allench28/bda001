env='dev'

AccountMap={
    'dev': '582554346432',
    'staging': '',
    'prod': ''
}

RegionMap={
    'dev': 'ap-southeast-5',
    'staging': 'ap-southeast-5',
    'prod': 'ap-southeast-5'
}

DynamoDBMap={
    'AGENT_CONFIGURATION_TABLE': '{}-AgentConfigurations',
    'MERCHANT_TABLE': '{}-Merchant',
    'USER_GROUP_TABLE': '{}-UserGroup',
    'USER_MATRIX_TABLE': '{}-UserMatrix',
    'USER_TABLE': '{}-User',
    'API_USER_MATRIX_TABLE': '{}-APIUserMatrix',
    'EXTRACTED_DOCUMENTS_TABLE': '{}-ExtractedDocuments',
    'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': '{}-ExtractedDocumentsLineItems',
    'INBOX_MONITORING_TABLE': '{}-InboxMonitoring',
    'DOCUMENT_UPLOAD_TABLE': '{}-DocumentUpload',
    'TIMELINE_TABLE': '{}-Timeline',
    'DOWNLOAD_JOB_TABLE': '{}-DownloadJob',
    'EXTRACTED_GRN_TABLE': '{}-ExtractedGrn',
    'EXTRACTED_GRN_LINE_ITEMS_TABLE': '{}-ExtractedGrnLineItems',
    'EXTRACTED_PO_TABLE': '{}-ExtractedPo',
    'EXTRACTED_PO_LINE_ITEMS_TABLE': '{}-ExtractedPoLineItems',
    'THREE_WAY_MATCHING_RESULTS_TABLE': '{}-ThreeWayMatchingResults',
    'THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE': '{}-ThreeWayMatchingLineItems',
    'JOB_TRACKING_TABLE': '{}-JobTracking',
    'EXTRACTED_REFERRAL_LETTER_TABLE': '{}-ExtractedReferralLetter',
    'SEQUENCE_NUMBER_GENERATOR_TABLE': '{}-SequenceNumberGenerator',
}
#3WAYTESTBUCKET is used for 3 way matching test bucket. To be unified into another bucket in the future.
S3Map={
    'dev': {
        'AGENT_CONFIGURATION_BUCKET': '{}-agentconfig-mappings-{}-my',
        'SMART_EYE_BUCKET': '{}-smarteye-documents-bucket-{}-my', 
    },
    'staging': {
        'AGENT_CONFIGURATION_BUCKET': '{}-agentconfig-mappings-{}-my',
        'SMART_EYE_BUCKET': '{}-smarteye-documents-bucket-{}-my',
    },
    'demo': {
        'AGENT_CONFIGURATION_BUCKET': '{}-agentconfig-mappings-{}-my',
        'SMART_EYE_BUCKET': '{}-smarteye-documents-bucket-{}-my',
    },
    'prod': {
        'AGENT_CONFIGURATION_BUCKET': '{}-agentconfig-mappings-{}-my',
        'SMART_EYE_BUCKET': '{}-smarteye-documents-bucket-{}-my'
    }
    
}

LambdaArnMap={
    'EMAIL_POLLING_LAMBDA_ARN': 'arn:aws:lambda:{}:{}:function:{}-EmailPolling',
    'CONVERT_TO_CSV_LAMBDA_ARN': 'arn:aws:lambda:{}:{}:function:{}-ExtractedDocumentToCsvSQS',
    'EXPORT_UPLOADED_DOCUMENTS_LAMBDA_ARN': 'arn:aws:lambda:{}:{}:function:{}-AdminExportUploadedDocumentsCSV',
    'EXPORT_EXTRACTED_DOCUMENTS_LAMBDA_ARN': 'arn:aws:lambda:{}:{}:function:{}-AdminExportExtractedDocumentsCSV',
    'EXPORT_EXTRACTED_LINE_ITEMS_LAMBDA_ARN': 'arn:aws:lambda:{}:{}:function:{}-AdminExportExtractedLineItemsCSV',
    'EXPORT_THREE_WAY_MATCHING_LAMBDA_ARN': 'arn:aws:lambda:{}:{}:function:{}-AdminExport3WayMatchingResults',
}

BedrockPromptManagementArnMap={
    'DEFAULT_INVOICE_EXTRACTION_PROMPT_ARN': ''
}

CognitoMap={
    'COGNITO_USER_POOL': 'ap-southeast-5_89dmkqbV5'
}

SqsMap={
    'dev': {
        '3WAYMATCHING_SQS': 'https://sqs.ap-southeast-1.amazonaws.com/{}/AnalyzeThreeWayMatchingQueue',
        'N8N_SQS_QUEUE': 'https://sqs.ap-southeast-1.amazonaws.com/{}/N8nErpQueue',
        'TRIGGER_RECONCILIATION_QUEUE_URL': 'https://sqs.ap-southeast-1.amazonaws.com/{}/TriggerReconciliationQueue'
    },
    'staging': {
        '3WAYMATCHING_SQS': 'https://sqs.ap-southeast-1.amazonaws.com/{}/AnalyzeThreeWayMatchingQueue',
        'N8N_SQS_QUEUE': ''
    },
    'demo': {
        '3WAYMATCHING_SQS': 'https://sqs.ap-southeast-1.amazonaws.com/{}/AnalyzeThreeWayMatchingQueue',
        'N8N_SQS_QUEUE': ''
    },
    'prod': {
        '3WAYMATCHING_SQS': 'https://sqs.ap-southeast-1.amazonaws.com/{}/AnalyzeThreeWayMatchingQueue',
        'N8N_SQS_QUEUE': ''
    }
    
}

ElasticSearchMap={
    'dev': {
        'domainName': '',
        'endpoint': 'search-aap-es-domain-dev-fz4asyjjebkcopfaquzs3ksz4e.ap-southeast-5.es.amazonaws.com'
    },
    'staging': {
        'domainName': '',
        'endpoint': ''
    },
    'demo': {
        'domainName': '',
        'endpoint': ''
    },
    'prod': {
        'domainName': '',
        'endpoint': ''
    }
}