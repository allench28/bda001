import boto3
import time
import sys
from aws_lambda_powertools import Logger, Tracer

textract = boto3.client('textract')

logger = Logger()
tracer = Tracer()

def lambda_handler(event, context):
    
    logger.info(event)
    job_id = event['jobId']
    document = event['document']
    extractedEmailId = event['extractedEmailId']
    filepath = event['filepath']
    # # Format source file name
    # sourceFile = event['document'].split('/')[3]
    # sourceFile = sourceFile.replace(" ", "_")
    # prefix = '/'.join(document.split('/')[:3])

    completed_jobs = []
    while True:
        response = textract.get_document_text_detection(JobId=job_id)
        status = response['JobStatus']
        print(response)
        if status == 'SUCCEEDED':
            completed_jobs.append({
                "filepath": filepath,
                'jobId': job_id,
                'document': document,
                'extractedEmailId': extractedEmailId,
                'triggerType': 'attachment'
            })
            print(f"Payload size: {sys.getsizeof(completed_jobs)} bytes")
            break
        elif status == 'FAILED':
            raise Exception(f"Job {job_id} failed.")
        else:
            time.sleep(5)

    return {
        'Processed': completed_jobs,
        # 'bucket': bucket
    }
