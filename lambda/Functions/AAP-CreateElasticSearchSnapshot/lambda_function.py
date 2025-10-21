import os
import boto3
from requests_aws4auth import AWS4Auth
from datetime import datetime, timedelta
from aws_lambda_powertools import Logger, Tracer
from opensearchpy import OpenSearch, RequestsHttpConnection

S3_BUCKET = os.environ.get('S3_BUCKET')
ES_S3ACCESS_ROLE= os.environ.get('ES_S3ACCESS_ROLE')
ES_DOMAIN_ENDPOINT = os.environ.get('ES_DOMAIN_ENDPOINT')

ES_RESOURCES = {
    ES_DOMAIN_ENDPOINT: S3_BUCKET,  
}

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        today = datetime.now() + timedelta(hours=8)
        if today.weekday() >= 5:
            # print('Today is Weekend!')
            return True
        
        for es_endpoint, s3_bucket_name in ES_RESOURCES.items():
            # Repository and Snapshot Name
            repository = 'ECOM-ES-{}'.format(str(today.date()))
            snapshotFile = str(today.date())
            
            # Get Credentials
            credentials = boto3.Session().get_credentials()
            accessKey = credentials.access_key
            secretKey = credentials.secret_key
            awsauth = AWS4Auth(accessKey, secretKey, 'ap-southeast-5', 'es', session_token=credentials.token)
            
            # Elasticsearch Configuration
            es = OpenSearch(
                hosts=[{'host': es_endpoint, 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=100000
            )
            
            payload = {
                'type': 's3',
                'settings': {
                    'bucket': s3_bucket_name,
                    'role_arn': ES_S3ACCESS_ROLE
                }
            }
            
            # Verify Repository and Create if not exists
            if verifyRepo(es, repository) is False:
                result = es.snapshot.create_repository(repository, payload)

            payload = {
                'ignore_unavailable': True
            }
            
            # Create ES Snapshot
            result = es.snapshot.create(repository, snapshotFile, payload)
            print(result)
        
        return True
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}
    
@tracer.capture_method           
def verifyRepo(es, repository):
    try:
        result = es.snapshot.verify_repository(repository)
        return True
    except Exception as ex:
        return False
