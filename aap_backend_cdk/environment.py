env='dev'

AccountMap={
    'dev': '',
    'staging': '',
    'prod': ''
}

RegionMap={
    'dev': 'ap-southeast-5',
    'staging': 'ap-southeast-5',
    'prod': 'ap-southeast-5'
}

elasticacheMap={
    'dev': {
        'redisClusterId': '',
        'redisClusterEndpoint': ''
    },
    'staging': {
        'redisClusterId': '',
        'redisClusterEndpoint': ''
    },
    'prod': {
        'redisClusterId': '',
        'redisClusterEndpoint': ''
    }
}

elasticsearchMap={
    'dev': {
        'endpoint': 'https://search-aap-es-domain-dev-resp7npl7k4rgaef67liyhkyti.ap-southeast-5.es.amazonaws.com',
        'domainName': '{}-es-domain-dev',
        's3BackupRole': 'arn:aws:iam::{}:role/{}ElasticSearchS3AccessRoleMY',
        's3BackupBucket': '{}-elasticsearch-snapshot-dev-my',
        'SUPPLIER_INDEX': 'supplierfm',
        'LINE_ITEM_INDEX': 'supplieritemfm',
        'STORE_INDEX': 'storefm',
        
    },
    'staging': {
        'endpoint': 'https://search-aap-es-domain-dev-resp7npl7k4rgaef67liyhkyti.ap-southeast-5.es.amazonaws.com',
        'domainName': '{}-es-domain-staging',
        's3BackupRole': 'arn:aws:iam::{}:role/{}ElasticSearchS3AccessRoleMY',
        's3BackupBucket': '{}-elasticsearch-snapshot-staging-my',
        'SUPPLIER_INDEX': 'supplierfm',
        'LINE_ITEM_INDEX': 'supplieritemfm',
        'STORE_INDEX': 'storefm',
    },
    'prod': {
        'endpoint': '',
        'domainName': '{}-es-domain-prod',
        's3BackupRole': 'arn:aws:iam::{}:role/{}ElasticSearchS3AccessRoleMY',
        's3BackupBucket': '{}-elasticsearch-snapshot-prod-my',
        'SUPPLIER_INDEX': 'supplierfm',
        'LINE_ITEM_INDEX': 'supplieritemfm',
        'STORE_INDEX': 'storefm',
    }
}

glueMap={
    'dev': {
        'glueRole': 'arn:aws:iam::{}:role/{}-GlueRole',
        'processS3ReconcilationDocumentJob': '{}-ProcessS3ReconciliationDocument',
    }
}

s3Map={
    'dev': {
        'athenaResult': 's3://{}-datalake-dev/Athena-Result/',
        'SMART_EYE_BUCKET': '{}-smarteye-documents-bucket-{}-my',
        'AGENT_MAPPING_BUCKET': '{}-agentconfig-mappings-{}-my',

    },
    'staging': {
        'athenaResult': 's3://{}-datalake-staging/Athena-Result/',
        'SMART_EYE_BUCKET': '{}-smarteye-documents-bucket-{}-my',
        'AGENT_MAPPING_BUCKET': '{}-agentconfig-mappings-{}-my',
    },
    'prod': {
        'athenaResult': 's3://{}-datalake-prod/Athena-Result/',
        'SMART_EYE_BUCKET': '{}-smarteye-documents-bucket-{}-my',
        'AGENT_MAPPING_BUCKET': '{}-agentconfig-mappings-{}-my',
    }
}

DynamoDBMap={
    'USER_GROUP_TABLE': '{}-UserGroup',
    'USER_TABLE': '{}-User',
    'MERCHANT_TABLE': '{}-Merchant',
    'DOWNLOAD_JOB_TABLE': '{}-DownloadJob',
    'DOCUMENT_UPLOAD_TABLE': '{}-DocumentUpload',
    'EXTRACTED_DOCUMENTS_TABLE': '{}-ExtractedDocuments',
    'EXTRACTED_DOCUMENTS_LINE_ITEMS_TABLE': '{}-ExtractedDocumentsLineItems',
    'INBOX_MONITORING_TABLE': '{}-InboxMonitoring',
    'TIMELINE_TABLE': '{}-Timeline',
    'EXTRACTED_DOCUMENTS_TABLE': '{}-ExtractedDocuments',
    'EXTRACTED_GRN_TABLE': '{}-ExtractedGrn',
    'EXTRACTED_GRN_LINE_ITEMS_TABLE': '{}-ExtractedGrnLineItems',
    'EXTRACTED_PO_TABLE': '{}-ExtractedPo',
    'EXTRACTED_PO_LINE_ITEMS_TABLE': '{}-ExtractedPoLineItems',
    'THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE': '{}-ThreeWayMatchingLineItems',
    'THREE_WAY_MATCHING_RESULTS_TABLE': '{}-ThreeWayMatchingResults',
    'JOB_TRACKING_TABLE': '{}-JobTracking',
    'RECONCILIATION_RESULTS_TABLE': '{}-ReconciliationResults',
    'BANK_TRANSACTION_TABLE': '{}-BankStatement',
    'PAYMENT_GATEWAY_TABLE': '{}-PaymentGatewayReport',
    'PAYMENT_TRANSACTION_TABLE': '{}-PaymentTransaction',
    'PAYMENT_REPORT_ERP_TABLE': '{}-PaymentReportErp',
    'STORE_TABLE': '{}-Store',
    'SALES_ENTRY_TABLE': '{}-SalesStatement',
    'AGENT_CONFIGURATION_TABLE': '{}-AgentConfigurations',
    'SUPPLIER_TABLE': '{}-Supplier',
    'SUPPLIER_ITEM_TABLE': '{}-SupplierItem',
    'EXTRACTED_EMAIL_TABLE': '{}-ExtractedEmail',
    'ROUTE_CONTENT_TABLE': '{}-RouteContent',
    'SKILL_MATRIX_TABLE': '{}-SkillMatrix',
    'SEQUENCE_NUMBER_GENERATOR_TABLE': '{}-SequenceNumberGenerator',
    'EXTRACTED_REFERRAL_LETTER_TABLE': '{}-ExtractedReferralLetter',
    'EMAIL_ANALYSIS_RESULT_TABLE': '{}-EmailAnalysisResult',
    'FIXED_ASSET_TABLE': '{}-FixedAsset',
    'ACQUISITION_JOURNAL_TABLE': '{}-AcquisitionJournal',
}

N8nMap={
    'invoice': 'https://aliaarina.app.n8n.cloud/webhook/c0757462-3064-4e7f-a017-b6c69769216f',   
    'po': 'https://aliaarina.app.n8n.cloud/webhook/c0757462-3064-4e7f-a017-b6c69769216f'
}

SqsMap={
    'dev': {
        '3WayMatchingResultsSQS':'https://sqs.ap-southeast-5.amazonaws.com/{}/AnalyzeThreeWayMatchingQueue',
        'N8N_SQS_QUEUE': 'https://sqs.ap-southeast-5.amazonaws.com/{}/N8nErpQueue',
        'RouteContentSQS': 'https://sqs.ap-southeast-5.amazonaws.com/{}/RouteContentQueue',
        'CreateDocumentQueue': 'https://sqs.ap-southeast-5.amazonaws.com/{}/CreateDocumentQueue',
        'PresalesEmailAnalysisQueue': 'https://sqs.ap-southeast-5.amazonaws.com/{}/PresalesEmailDataAnalysisQueue',
        'ProcessConverseExtractionOutputQueue': 'https://sqs.ap-southeast-5.amazonaws.com/{}/ProcessConverseExtractionOutputQueue',
        'ProcessConverseExtractionOutputQueueBR': 'https://sqs.ap-southeast-5.amazonaws.com/{}/ProcessConverseExtractionOutputQueueBR',
    },
    'staging': {
        '3WayMatchingResultsSQS':'https://sqs.ap-southeast-5.amazonaws.com/{}/AnalyzeThreeWayMatchingQueue',
        'N8N_SQS_QUEUE': 'https://sqs.ap-southeast-5.amazonaws.com/{}/N8nErpQueue',
        'RouteContentSQS': 'https://sqs.ap-southeast-5.amazonaws.com/{}/RouteContentQueue',
        'CreateDocumentQueue': 'https://sqs.ap-southeast-5.amazonaws.com/{}/CreateDocumentQueue',
        'PresalesEmailAnalysisQueue': 'https://sqs.ap-southeast-5.amazonaws.com/{}/PresalesEmailDataAnalysisQueue',
        'ProcessConverseExtractionOutputQueue': 'https://sqs.ap-southeast-5.amazonaws.com/{}/ProcessConverseExtractionOutputQueue',
        'ProcessConverseExtractionOutputQueueBR': 'https://sqs.ap-southeast-5.amazonaws.com/{}/ProcessConverseExtractionOutputQueueBR',
    },
}

MERCHANT_ID_MAP = {
    'dev': {
        'GENTING': '',
        'BR': '6b8a78e2-95fe-403b-8008-e5f7c1a631fc',
        'VSTECS': '4f8219c3-b0ac-4cc5-b08b-4d1f98323b4e',
        'FM': '9f79aff1-ad5b-4d7f-8017-3d70e5f99cff',
    },
    'staging': {
        'GENTING': '073ea456-e3ac-4640-950d-f23cc0051bc2',
        'BR': '72b74810-3171-4be4-90fe-fc9bd6019b48',
        'VSTECS': 'd1aae641-75b1-4ec2-b12b-85d10f0e87bc',
        'FM': 'c8bf083a-79d9-4b83-a98d-2e122524515f',
    },
    'prod': {
        'GENTING': '',
        'BR': '',
        'VSTECS': '',
    }
}

BedrockModel={
    'dev': {
        'model-3.5':"anthropic.claude-3-5-sonnet-20240620-v1:0",
        'model-3.7': 'apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
        'claude-4': 'apac.anthropic.claude-sonnet-4-20250514-v1:0',
        'nova-pro': 'apac.amazon.nova-pro-v1:0',
    },
    'staging': {
        'model-3.5':"anthropic.claude-3-5-sonnet-20240620-v1:0",
        'model-3.7': 'apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
        'claude-4': 'apac.anthropic.claude-sonnet-4-20250514-v1:0',
        'nova-pro': 'apac.amazon.nova-pro-v1:0',
    },
    'prod': {
        'model-3.5':"anthropic.claude-3-5-sonnet-20240620-v1:0",
        'model-3.7': 'apac.anthropic.claude-3-7-sonnet-20250219-v1:0',
        'claude-4': 'apac.anthropic.claude-sonnet-4-20250514-v1:0',
        'nova-pro': 'apac.amazon.nova-pro-v1:0',
    },
}

stepFunctionMap={
    'dev': {
        'retryBDAPollingStepFunctionArn': 'arn:aws:states:us-east-1:{}:stateMachine:RetryBDAPollingStepFunctionMachine',
        'EmailAttachmentTextractStepFunctionArn': 'arn:aws:states:ap-southeast-5:{}:stateMachine:EmailAttachmentTextractStepFunctionMachine'
    },
    'staging': {
        'retryBDAPollingStepFunctionArn': 'arn:aws:states:us-east-1:{}:stateMachine:RetryBDAPollingStepFunctionMachine',
        'EmailAttachmentTextractStepFunctionArn': 'arn:aws:states:ap-southeast-5:{}:stateMachine:EmailAttachmentTextractStepFunctionMachine'
    },
    'prod': {
        'retryBDAPollingStepFunctionArn': 'arn:aws:states:us-east-1:{}:stateMachine:RetryBDAPollingStepFunctionMachine',
        'EmailAttachmentTextractStepFunctionArn': 'arn:aws:states:ap-southeast-5:{}:stateMachine:EmailAttachmentTextractStepFunctionMachine'
    }
}