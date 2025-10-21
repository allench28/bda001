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
S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb')

ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-1', 'es', session_token=CREDENTIALS.token)

DOWNLOAD_JOB_DDB_TABLE = DDB_RESOURCE.Table(DOWNLOAD_JOB_TABLE)

logger = Logger()
tracer = Tracer()

"""
Sample event
{
   "jobId":"e1885cbe-dbda-467b-98bd-9bd9cfcfc661",
   "arguments":{
      "resultType": "invoices",
      "module":"ReconciliationResults",
      "sort":{
         "field":"createdAt",
         "direction":"asc"
      },
      "filter": {"and": []}
   },
   "merchantId":"4d98df53-e473-445a-84e9-2681f1e82206"
}

"""

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        arguments = event.get('arguments')
        merchant_id = event.get('merchantId')
        # itemIdList = arguments.get('itemIdList')
        jobId = event.get('jobId')
        sortField = 'createdAt'
        sortDirection = 'asc'
        if arguments.get('sort') is not None:
            sortField = arguments.get('sort').get('field')
            sortDirection = arguments.get('sort').get('direction')
            if sortField in [
                    'merchantId',
                    'routeContentId',
                    'extractedEmailId',
                    'createdBy',
                    'updatedBy'
                ]:
                sortField += '.keyword'

        filters = arguments.get('filter', {})
        routingResults, extractedEmailContents = getDataFromES(merchant_id, sortField, sortDirection, filters)

        header = [
            "Primary - Presales",
            "CC - Presales",
            "Sender",
            "Reseller",
            "Date Received",
            "Date Sent Out",
            "Tender",
            "End User",
            "End User industry",
            "Vendor",
            "Product",
            "Product (MYR)",
            "Supp (MYR)",
            "Product (USD)",
            "Supp (USD)",
            "KU Service (MYR)",
            "Source File"
        ]
        
        processedResultCSVRows = processRoutingResultsList(routingResults, extractedEmailContents)
        
        if not processedResultCSVRows:
            logger.info("No data found for the given filters")
            updateDownloadJobStatus(jobId, 'COMPLETED', 'No data found')
            return {
                'status': True,
                'message': 'No data found',
            }
        
        currentDateTime = datetime.strftime((datetime.now()+timedelta(hours=8)), '%d-%m-%Y_%H:%M:%S')

        filename = 'RoutingResult{}.csv'.format(currentDateTime)
        zipFile = 'RoutingResult{}.zip'.format(currentDateTime)
        if os.path.exists('/tmp/' + filename):
            os.remove('/tmp/' + filename)

        with open('/tmp/' + filename, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(header)
            writer.writerows(processedResultCSVRows)


        with ZipFile('/tmp/' + zipFile, 'w') as zip:
            zip.write('/tmp/' + filename, filename)

        S3_CLIENT.upload_file('/tmp/' + zipFile, S3_BUCKET, 'presales/archive/4f8219c3-b0ac-4cc5-b08b-4d1f98323b4e/result-csv/' +zipFile)

        objectPresignedURL = S3_CLIENT.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': 'presales/archive/4f8219c3-b0ac-4cc5-b08b-4d1f98323b4e/result-csv/' +zipFile
            }
        )
        os.remove('/tmp/' + filename)
        os.remove('/tmp/' + zipFile)
        updateDownloadJobStatus(jobId, 'COMPLETED', 'Job Completed', S3_BUCKET+'presales/archive/4f8219c3-b0ac-4cc5-b08b-4d1f98323b4e/result-csv/' +zipFile, objectPresignedURL)

        return {
            'status': True,
            'message': 'Job Completed',
            'objectPresignedUrl': objectPresignedURL,
        }
    
    except (BadRequestException, ResourceNotFoundException) as ex:
        print("error: ", str(ex))
        return {'status': False, 'message': str(ex)}
    
    except Exception as ex:
        updateDownloadJobStatus(jobId, 'FAILED', str(ex))
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."}

@tracer.capture_method
def processRoutingResultsList(routingResults, extractedEmailContents):
    # Build a lookup for extractedemailContents by extractedEmailId
    extracted_email_map = {
        item['_source'].get('extractedEmailId'): item['_source']
        for item in extractedEmailContents
        if item.get('_source') and item['_source'].get('extractedEmailId')
    }
    routingResultsList = []
    for key in routingResults:
        document = key.get('_source', {})
        extracted_email_id = document.get('extractedEmailId')
        extracted_email_doc = extracted_email_map.get(extracted_email_id)

        # Only process if there is a matching extractedEmailId
        if not extracted_email_doc:
            continue

        # Parse and format dates
        def parse_date(date_str):
            try:
                return datetime.strptime(date_str, '%d-%m-%YT%H:%M:%S.%fZ').strftime('%d/%m/%Y')
            except Exception:
                return '-'

        dateReceived = document.get('dateReceived', '-')
        dateSentOut = document.get('dateSentOut', '-')

        isTender = "Yes" if document.get('isTender') is True else "No"
        primaryEmailIds = ', '.join(document.get('primaryEmailIds', '-')) if isinstance(document.get('primaryEmailIds'), list) else document.get('primaryEmailIds', '-')
        ccEmailIds = ', '.join(document.get('ccEmailIds', '-')) if isinstance(document.get('ccEmailIds'), list) else document.get('ccEmailIds', '-')
        productName = ', '.join(document.get('product', '-')) if isinstance(document.get('product'), list) else document.get('product', '-')
        vendor = ', '.join(document.get('vendor', '-')) if isinstance(document.get('vendor'), list) else document.get('vendor', '-')
        
        reseller = extracted_email_doc.get('reseller', '-')
        sourceFile = extracted_email_doc.get('sourceFile', '-')
        senderEmailAddress = extracted_email_doc.get('senderEmailAddress', '-')
        endUserName = ', '.join(extracted_email_doc.get('endUserName', '-')) if isinstance(extracted_email_doc.get('endUserName'), list) else extracted_email_doc.get('endUserName', '-')
        endUserIndustry = ', '.join(extracted_email_doc.get('industry', '-')) if isinstance(extracted_email_doc.get('industry'), list) else extracted_email_doc.get('industry', '-')
        productMYR = ', '.join(extracted_email_doc.get('productMYR', '-')) if isinstance(extracted_email_doc.get('productMYR'), list) else extracted_email_doc.get('productMYR', '-')
        suppMYR = ', '.join(extracted_email_doc.get('suppMYR', '-')) if isinstance(extracted_email_doc.get('suppMYR'), list) else extracted_email_doc.get('suppMYR', '-')
        productUSD = ', '.join(extracted_email_doc.get('productUSD', '-')) if isinstance(extracted_email_doc.get('productUSD'), list) else extracted_email_doc.get('productUSD', '-')
        suppUSD = ', '.join(extracted_email_doc.get('suppUSD', '-')) if isinstance(extracted_email_doc.get('suppUSD'), list) else extracted_email_doc.get('suppUSD', '-')
        kuServicesMYR = ', '.join(extracted_email_doc.get('kuServicesMYR', '-')) if isinstance(extracted_email_doc.get('kuServicesMYR'), list) else extracted_email_doc.get('kuServicesMYR', '-')

        payload = [
            primaryEmailIds,
            ccEmailIds,
            senderEmailAddress,
            reseller,
            dateReceived,
            dateSentOut,
            isTender,
            endUserName,
            endUserIndustry,
            vendor,
            productName,
            productMYR,
            suppMYR,
            productUSD,
            suppUSD,
            kuServicesMYR,
            sourceFile
        ]
        routingResultsList.append(payload)

    return routingResultsList

@tracer.capture_method
def getDataFromES(merchantId, sortField, sortDirection, filters):
    sortFieldRC = 'routeContentId.keyword'
    sortFieldEE = 'extractedEmailId.keyword'
    sortDirection = 'asc'
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

    def build_query(filters, merchantId):
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
                        if filterCondition in ['gt', 'gte', 'lt', 'lte']:
                            query['bool']['must'].append({"range": {filterField: {filterConditionMap[filterCondition]: filterValue}}})
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
                    filterField, filterConditionAndValue = list(andCondition.items())[0]
                    filterCondition, filterValue = list(filterConditionAndValue.items())[0]
                    if filterCondition in ['gt', 'gte', 'lt', 'lte']:
                        query['bool']['must'].append({"range": {filterField: {filterConditionMap[filterCondition]: filterValue}}})
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
                    if filterCondition in ['gt', 'gte', 'lt', 'lte']:
                        orConditionQuery['bool']['should'].append({"range": {filterField: {filterConditionMap[filterCondition]: filterValue}}})
                    elif filterCondition == 'exists':
                        orConditionQuery['bool']['should'].append({filterConditionMap[filterCondition]: {"field": filterField}})
                    else:
                        orConditionQuery['bool']['should'].append({filterConditionMap[filterCondition]: {filterField: filterValue}})
                query['bool']['must'].append(orConditionQuery)

        return query

    def run_es_query(index, sortField, sortDirection, filters, merchantId):
        url = f'https://{ES_DOMAIN_ENDPOINT}/{index}/_doc/_search'
        query = build_query(filters, merchantId)
        payload = {
            'query': query,
            'sort': {sortField: {'order': sortDirection}},
            'size': 10000
        }
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

    # Query routecontent index
    routingResults = run_es_query('routecontent', sortFieldRC, sortDirection, filters, merchantId)
    # Query extractedemail index
    extractedEmailContents = run_es_query('extractedemail', sortFieldEE, sortDirection, filters, merchantId)

    return routingResults, extractedEmailContents

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