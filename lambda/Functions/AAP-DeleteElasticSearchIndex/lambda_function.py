import os
import boto3
from requests_aws4auth import AWS4Auth
from aws_lambda_powertools import Logger, Tracer
from elasticsearch import Elasticsearch, RequestsHttpConnection

HOST = os.environ.get('HOST')

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        credentials = boto3.Session().get_credentials()
        accessKey = credentials.access_key
        secretKey = credentials.secret_key

        awsauth = AWS4Auth(accessKey, secretKey, 'ap-southeast-1', 'es', session_token=credentials.token)  

        es = Elasticsearch(
            hosts=[{'host': HOST, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=100000
        )
        
        indexList = [
            'product'
        ]
        
        for index in indexList:
            es.indices.delete(index=index)

    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}