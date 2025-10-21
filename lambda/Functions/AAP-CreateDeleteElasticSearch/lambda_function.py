import os
import json
import boto3
from datetime import datetime, timedelta
from aws_lambda_powertools import Logger, Tracer

ES_DOMAIN_NAME = os.environ.get('ES_DOMAIN_NAME')

ES_DOMAIN_NAME_LIST = [
    ES_DOMAIN_NAME
]

esClient = boto3.client('opensearch')

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        mode = event["mode"]
        
        today = datetime.now() + timedelta(hours=8)
        if today.weekday() >= 5:
            # print('Today is Weekend!')
            return True
        
        for es_endpoint in ES_DOMAIN_NAME_LIST:
            if mode == 'create':
                esCreate = esClient.create_domain(
                    DomainName=es_endpoint,
                    EngineVersion='OpenSearch_1.2',
                    ClusterConfig={
                        'InstanceType': 't3.medium.search',
                        'InstanceCount': 3
                    },
                    EBSOptions={
                        'EBSEnabled': True,
                        'VolumeType': 'gp2',
                        'VolumeSize': 10
                    },
                    TagList=[
                        {
                            'Key': 'PROJECT_NAME',
                            'Value': 'AI-AGENT-PLATFORM'
                        },
                    ]
                )
            elif mode == 'delete':
                esDelete = esClient.delete_domain(DomainName=es_endpoint)
        
        return True
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}