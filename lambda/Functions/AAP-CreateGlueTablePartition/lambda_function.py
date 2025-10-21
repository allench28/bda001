import os
import time
import boto3
from datetime import datetime, timedelta, date
from aws_lambda_powertools import Logger, Tracer

GLUE_CLIENT = boto3.client('glue')
ATHENA_CLIENT = boto3.client('athena')
S3OUTPATH = os.environ.get('S3OUTPATH')

logger = Logger()
tracer = Tracer()

@tracer.capture_method     
def days_cur_month():
    m = (datetime.now() + timedelta(days=1)).month 
    y = (datetime.now() + timedelta(days=1)).year
    nextM = m + 1
    nextY = y
    if m == 12:
        nextM = 1
        nextY = y + 1
    ndays = (date(nextY, nextM, 1) - date(y, m, 1)).days
    d1 = date(y, m, 1)
    d2 = date(y, m, ndays)
    delta = d2 - d1

    return [(d1 + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days + 1)]

listOfDate = days_cur_month()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        listOfDatabaseName = ['ecom_database']
        
        for databaseName in listOfDatabaseName:
        
            glueResponse = GLUE_CLIENT.get_tables(
                DatabaseName=databaseName,
                MaxResults=200
            )
            
            for table in glueResponse['TableList']:
                if ('_auditlog' in table['Name'] or '_analytics' in table['Name']) and table['Name'] != 'extracteddocument_auditlog':
                    createPartition(table['Name'], databaseName)
                    time.sleep(1)
                
    except Exception as ex: 
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}

@tracer.capture_method
def createPartition(tableName, databaseName):
    for date in listOfDate:
        year = date.split("-")[0]
        month = date.split("-")[1]
        day = date.split("-")[2]
    
        query = "ALTER TABLE {} ADD IF NOT EXISTS PARTITION(year='{}', month='{}', day='{}')".format(tableName, year, month, day)
            
        ATHENA_CLIENT.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {
                'Database': databaseName
            },
            ResultConfiguration = {
                'OutputLocation': S3OUTPATH,
            }
        )
