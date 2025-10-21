import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta
import dateutil
import os
import requests
from requests_aws4auth import AWS4Auth
import csv
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import BadRequestException, ResourceNotFoundException
from zipfile import ZipFile

ES_DOMAIN_ENDPOINT = os.environ.get('ES_DOMAIN_ENDPOINT')
S3_BUCKET = os.environ.get('SMART_EYE_BUCKET')
DOWNLOAD_JOB_TABLE = os.environ.get('DOWNLOAD_JOB_TABLE')

CREDENTIALS = boto3.Session().get_credentials()
S3_CLIENT = boto3.client(
    's3',
    region_name='ap-southeast-5',
    endpoint_url='https://s3.ap-southeast-5.amazonaws.com'
)
DDB_RESOURCE = boto3.resource('dynamodb')

ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-5', 'es', session_token=CREDENTIALS.token)
DOWNLOAD_JOB_DDB_TABLE = DDB_RESOURCE.Table(DOWNLOAD_JOB_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        arguments = event.get('arguments')
        merchant_id = event.get('merchantId')
        itemIdList = arguments.get('itemIdList')
        jobId = event.get('jobId')
        sortField = 'createdAt'
        sortDirection = 'desc'
        if arguments.get('sort') is not None:
            sortField = arguments.get('sort').get('field')
            sortDirection = arguments.get('sort').get('direction')
            if sortField in ['invoiceNumber', 'supplierName', 'status']:
                sortField += '.keyword'

        filters = arguments.get('filter', {})
        extractedLineItem = getDataFromES(merchant_id, sortField, sortDirection, filters, itemIdList)
        header = [
            "Invoice Number",
            "Supplier Name",
            "Supplier Code",
            "Item Code",
            "Description",
            "Currency",
            "Unit Price",
            "Item UOM",
            "Quantity",
            "Total Price",
            "Uploaded At",
            "Uploaded By",
            "Updated At",
            "Updated By",
            "Item Status",
            "Issue Description"
        ]

        lineItemList = processLineItemList(extractedLineItem)
        currentDateTime = datetime.strftime((datetime.now()+timedelta(hours=8)), '%Y_%m_%d_%H_%M_%S')

        filename = 'ExtractedLineItemList{}.csv'.format(currentDateTime)
        zipFile = 'ExtractedLineItemList{}.zip'.format(currentDateTime)
        if os.path.exists('/tmp/' + filename):
            os.remove('/tmp/' + filename)

        with open('/tmp/' + filename, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(header)
            writer.writerows(lineItemList)


        with ZipFile('/tmp/' + zipFile, 'w') as zip:
            zip.write('/tmp/' + filename, filename)

        S3_CLIENT.upload_file('/tmp/' + zipFile, S3_BUCKET, 'export/extracted-lineitems/'+zipFile)

        objectPresignedURL = S3_CLIENT.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': 'export/extracted-lineitems/'+zipFile
            }
        )
        os.remove('/tmp/' + filename)
        os.remove('/tmp/' + zipFile)
        updateDownloadJobStatus(jobId, 'COMPLETED', 'Job Completed', S3_BUCKET+'export/extracted-lineitems/'+zipFile, objectPresignedURL)

        return True
    except (BadRequestException, ResourceNotFoundException) as ex:
        return {'status': False, 'message': str(ex)}
    except Exception as ex:
        updateDownloadJobStatus(jobId, 'FAILED', str(ex))
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}

def processLineItemList(lineItem):
    lineItemList = []
    for key in lineItem:
        item = key['_source']
        tempList = [
            item.get('invoiceNumber'),
            item.get('supplierName'),
            item.get('supplierCode'),
            item.get('itemCode'),
            item.get('description'),
            item.get('currency'),
            item.get('unitPrice'),
            item.get('itemUom'),
            item.get('quantity'),
            item.get('totalPrice'),
            (datetime.strptime(item.get('createdAt'), '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
            item.get('createdBy'),
            (datetime.strptime(item.get('updatedAt'), '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
            item.get('updatedBy'),
            item.get('status'),
            item.get('exceptionStatus')
        ]
        lineItemList.append(tempList)

    return lineItemList

def getDataFromES(merchantId, sortField, sortDirection, filters, selectedItems):
    filterConditionMap = {
            'eq': 'match_phrase',
            'match': 'match',
            'matchPhrase': 'match_phrase',
            'matchPhrasePrefix': 'match_phrase_prefix',
            'gt': 'gt',
            'gte': 'gte',
            'lt': 'lt',
            'lte': 'lte',
            'wildcard': 'wildcard',
            'regexp': 'regexp'
        }

    url = f'https://{ES_DOMAIN_ENDPOINT}/extracteddocumentslineitems/_doc/_search'

    if filters.get('and') and len(filters.get('and')) > 0   :
        filters['and'].append({'merchantId': {'eq': merchantId}})
    else: 
        filters['and'] = [{'merchantId': {'eq': merchantId}}]

    query = {'bool': {'must': []}}
    for andCondition in filters.get('and', []):
        if andCondition.get('or') is None:
            if andCondition.get('and'):
                for subAndCondition in andCondition.get('and', []):
                    filterField, filterConditionAndValue = list(subAndCondition.items())[0]
                    filterCondition, filterValue = list(filterConditionAndValue.items())[0]
                    if filterCondition == 'gt' or filterCondition == 'gte' or filterCondition == 'lt' or filterCondition == 'lte':
                        query['bool']['must'].append({"range":{filterField: {filterConditionMap[filterCondition]: filterValue}}})
                    elif filterCondition == 'exists':
                         query['bool']['must'].append({filterConditionMap[filterCondition]: {"field": filterField}})
                    else:
                        query['bool']['must'].append({filterConditionMap[filterCondition]: {filterField: filterValue}})
            else:        
                filterField, filterConditionAndValue = list(andCondition.items())[0]
                filterCondition, filterValue = list(filterConditionAndValue.items())[0]
                if filterCondition == 'gt' or filterCondition == 'gte' or filterCondition == 'lt' or filterCondition == 'lte':
                    query['bool']['must'].append({"range":{filterField: {filterConditionMap[filterCondition]: filterValue}}})
                elif filterCondition == 'exists':
                    query['bool']['must'].append({filterConditionMap[filterCondition]: {"field": filterField}})
                elif filterCondition == 'wildcard':
                    query['bool']['must'].append({
                        'bool': {
                            'should': [
                                {
                                    'wildcard': {
                                        filterField: {
                                            'value': filterValue.lower(),
                                            'case_insensitive': True,
                                            'rewrite': 'constant_score'
                                        }
                                    }
                                },
                                {
                                    'wildcard': {
                                        f'{filterField}.keyword': {
                                            'value': filterValue.lower(),
                                            'case_insensitive': True,
                                            'rewrite': 'constant_score'
                                        }
                                    }
                                }
                            ],
                            'minimum_should_match': 1
                        }
                    })
                else:
                    query['bool']['must'].append({filterConditionMap[filterCondition]: {filterField: filterValue}})
        else:
            orConditionQuery = {'bool': {'should': []}}
            for orCondition in andCondition.get('or', []):
                filterField, filterConditionAndValue = list(orCondition.items())[0]
                filterCondition, filterValue = list(filterConditionAndValue.items())[0]
                if filterCondition == 'gt' or filterCondition == 'gte' or filterCondition == 'lt' or filterCondition == 'lte':
                    orConditionQuery['bool']['should'].append({"range":{filterField: {filterConditionMap[filterCondition]: filterValue}}})
                elif filterCondition == 'exists':
                    orConditionQuery['bool']['should'].append({filterConditionMap[filterCondition]: {"field": filterField}})
                else:
                    orConditionQuery['bool']['should'].append({filterConditionMap[filterCondition]: {filterField: filterValue}})
            query['bool']['must'].append(orConditionQuery)
            
    payload = dict()
    payload['query'] = query
    payload['sort'] = {sortField: {'order': sortDirection}}
    payload['size'] = 10000

    if selectedItems:
        payload['query']['bool']['must'].append({'ids': {'values': selectedItems}})
    
    print(payload)
    payloadES = json.dumps(payload)
    headers = {
            'Content-Type': "application/json",
            'User-Agent': "PostmanRuntime/7.20.1",
            'Accept': "application/json, text/plain, */*",
            'Cache-Control': "no-cache",
            'Postman-Token': "1ae2b03c-ac6c-45f4-9b37-4f95b9b0102c,b678f18f-3ebe-458e-b63b-6ced7b74851f",
            'Host': ES_DOMAIN_ENDPOINT,
            'Accept-Encoding': "gzip, deflate, br",
            'Connection': "keep-alive",
            'cache-control': "no-cache",
        }
    response = requests.request("GET", url, data=payloadES, headers=headers, auth=AWSAUTH)
    responseText = json.loads(response.text)
    
    if 'error' in responseText:
        raise BadRequestException("Invalid query statement")
    totalResp = responseText.get('hits').get('total').get('value')
    currentTotalResp = len(responseText.get('hits').get('hits'))

    resultList = responseText.get('hits').get('hits')
    while totalResp > currentTotalResp:
        payload["from"] = str(currentTotalResp)
        payloadES = json.dumps(payload)
        response = requests.request("GET", url, data=payloadES, headers=headers, auth=AWSAUTH)
        responseText = json.loads(response.text)
        if 'error' in responseText:
            raise BadRequestException("Invalid query statement")
        currentTotalResp += len(responseText.get('hits').get('hits'))
        resultList += responseText.get('hits').get('hits')

    return resultList

@tracer.capture_method
def updateDownloadJobStatus(jobId, status, message, s3ObjectPath=None, objectPresignedURL=None):
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    DOWNLOAD_JOB_DDB_TABLE.update_item(
        Key={
            'downloadJobId': jobId
        },
        UpdateExpression='SET #st=:st, #msg=:msg, #ua=:ua, #opurl=:opurl, #s3b=:s3b, #s3op=:s3op',
        ExpressionAttributeNames={
            '#st': 'status',
            '#msg': 'message',
            '#ua': 'updatedAt',
            '#opurl': 'objectPresignedUrl',
            '#s3b': 's3Bucket',
            '#s3op': 's3ObjectPath'
        },
        ExpressionAttributeValues={
            ':st': status,
            ':msg': message,
            ':ua': now,
            ':opurl': objectPresignedURL,
            ':s3b': S3_BUCKET,
            ':s3op': s3ObjectPath
        }
    )

