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
THREE_WAY_MATCHING_RESULTS_TABLE = os.environ.get("THREE_WAY_MATCHING_RESULTS_TABLE")
THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE = os.environ.get('THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE')
ES_S3ACCESS_ROLE = os.environ.get('ES_S3ACCESS_ROLE')

CREDENTIALS = boto3.Session().get_credentials()
S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb')

ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-1', 'es', session_token=CREDENTIALS.token)

DOWNLOAD_JOB_DDB_TABLE = DDB_RESOURCE.Table(DOWNLOAD_JOB_TABLE)
THREE_WAY_MATCHING_RESULTS_DDB_TABLE = DDB_RESOURCE.Table(THREE_WAY_MATCHING_RESULTS_TABLE)
THREE_WAY_MATCHING_LINE_ITEMS_DDB_TABLE = DDB_RESOURCE.Table(THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE)

logger = Logger()
tracer = Tracer()

# Define header mapping for document level export
DOCUMENT_HEADER_MAPPING = {
    'matchId': 'Match ID',
    'matchingStatus': 'Matching Status',
    'timestampOfMatching': 'Timestamp of Matching',
    'confidenceScore': 'Confidence Score',
    'exceptionCategory': 'Exception Category',
    'exceptionDescription': 'Exception Description',
    'recommendedAction': 'Recommended Action',
    'invoiceDate': 'Invoice Date',
    'invoiceNumber': 'Invoice Number',
    'currency': 'Invoice Currency',
    'totalInvoiceAmount': 'Total Invoice Amount',
    'taxAmount': 'Tax Amount',
    'taxCode': 'Tax Code',
    'supplierCode': 'Supplier Code',
    'supplierName': 'Supplier Name',
    'poDate': 'PO Date',
    'poFileName': 'PO Filename',
    'purchaseOrderNo': 'PO Number',
    'totalPOAmount': 'Total PO Amount',
    'grnDate': 'GRN Date',
    'grnFileName': 'GRN Filename',
    'grnNumber': 'GRN Number',
    'totalGRNAmount': 'Total GRN Amount',
    'remarks': 'Remarks',
    'lastModifiedBy': 'Last Modified By',
    'createdAt': 'Created At',
    'updatedAt': 'Updated At'
}

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        arguments = event.get('arguments')
        merchant_id = event.get('merchantId')
        itemIdList = arguments.get('itemIdList')
        jobId = event.get('jobId')
        
        # Sort configuration
        sortField = 'createdAt'
        sortDirection = 'desc'
        if arguments.get('sort') is not None:
            sortField = arguments.get('sort').get('field')
            sortDirection = arguments.get('sort').get('direction')
            if sortField in [
                'matchId', 
                'purchaseOrderNo', 
                'invoiceNumber', 
                'merchantId',
                'supplierName',
                'createdBy',
                'updatedBy'
                ]:
                sortField += '.keyword'

        filters = arguments.get('filter', {})
        
        # Get data from ElasticSearch
        matchingResults = getDataFromES(merchant_id, sortField, sortDirection, filters, itemIdList)
    
        # Document header and processing (only this is needed)
        header = list(DOCUMENT_HEADER_MAPPING.values())
        processedResultCSVRows = process3WayMatchingResults(matchingResults)
        
        if not processedResultCSVRows:
            print("error: ", 'No data found')
            updateDownloadJobStatus(jobId, 'COMPLETED', 'No data found')
            return True
        
        # Generate files and export
        currentDateTime = datetime.strftime((datetime.now()+timedelta(hours=8)), '%Y-%m-%d_%H:%M:%S')
        filename = f'ThreeWayMatching{currentDateTime}.csv'
        zipFile = f'ThreeWayMatching{currentDateTime}.zip'
        
        if os.path.exists('/tmp/' + filename):
            os.remove('/tmp/' + filename)

        with open('/tmp/' + filename, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(header)
            writer.writerows(processedResultCSVRows)

        with ZipFile('/tmp/' + zipFile, 'w') as zip:
            zip.write('/tmp/' + filename, filename)

        S3_CLIENT.upload_file('/tmp/' + zipFile, S3_BUCKET, 'export/threewaymatching-results/'+zipFile)

        objectPresignedURL = S3_CLIENT.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': 'export/threewaymatching-results/'+zipFile
            }
        )
        
        os.remove('/tmp/' + filename)
        os.remove('/tmp/' + zipFile)
        updateDownloadJobStatus(jobId, 'COMPLETED', 'Job Completed', S3_BUCKET+'export/threewaymatching-results/'+zipFile, objectPresignedURL)

        return True
    
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
def process3WayMatchingResults(matchingResults):
    resultsList = []
    for key in matchingResults:
        document = key['_source']
        
        # Format dates if needed
        createdAt = formatTimestamp(document.get('createdAt'))
        updatedAt = formatTimestamp(document.get('updatedAt'))
        timestampOfMatching = formatTimestamp(document.get('timestampOfMatching'))
        
        row = [
            document.get('matchId', ''),
            document.get('matchingStatus', ''),
            timestampOfMatching,
            document.get('confidenceScore', ''),
            document.get('exceptionCategory', ''),
            document.get('exceptionDescription', ''),
            document.get('recommendedAction', ''),
            document.get('invoiceDate', ''),
            document.get('invoiceNumber', ''),
            document.get('invoiceCurrency', ''),
            document.get('totalInvoiceAmount', ''),
            document.get('taxAmount', ''),
            document.get('taxCode', ''),
            document.get('supplierCode', ''),
            document.get('supplierName', ''),
            document.get('poDate', ''),
            document.get('poFileName', ''),
            document.get('purchaseOrderNo', ''),
            document.get('totalPOAmount', ''),
            document.get('grnDate', ''),
            document.get('grnFileName', ''),
            document.get('grnNumber', ''),
            document.get('totalGRNAmount', ''),
            document.get('remarks', ''),
            document.get('lastModifiedBy', ''),
            createdAt,
            updatedAt
        ]
        resultsList.append(row)

    return resultsList


def formatTimestamp(timestamp):
    """Format timestamp string to readable date/time"""
    if not timestamp:
        return ''
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        return (dt + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError) as e: 
        logger.debug(f"Could not parse timestamp {timestamp}: {str(e)}")
        return timestamp
    

@tracer.capture_method
def getDataFromES(merchantId, sortField, sortDirection, filters, selectedItems):
    filterConditionMap = {
        'eq': 'term',
        'match': 'match',
        'matchPhrase': 'match_phrase',
        'matchPhrasePrefix': 'match_phrase_prefix',
        'gt': 'gt',
        'gte': 'gte',
        'lt': 'lt',
        'lte': 'lte',
        'wildcard': 'wildcard',
        'regexp': 'regexp',
        'exists': 'exists'
    }

    url = f'https://{ES_DOMAIN_ENDPOINT}/threewaymatchingresults/_doc/_search'

    if filters.get('and') and len(filters.get('and')) > 0:
        filters['and'].append({'merchantId': {'eq': merchantId}})
    else: 
        filters['and'] = [{'merchantId': {'eq': merchantId}}]

    query = {'bool': {'must': []}}
    
    def processConditions(conditions):
        processed_conditions = []
        
        for condition in conditions:
            if 'and' in condition:
                # Handle nested 'and' conditions - FIXED
                nested_conditions = processConditions(condition['and'])
                processed_conditions.extend(nested_conditions)
            elif 'or' in condition:
                # Handle 'or' conditions
                orConditionQuery = {'bool': {'should': []}}
                for orCondition in condition['or']:
                    filterField, filterConditionAndValue = list(orCondition.items())[0]
                    filterCondition, filterValue = list(filterConditionAndValue.items())[0]
                    
                    # Map field names correctly
                    if filterField == 'matchingStatus':
                        filterField = 'matchingStatus'
                    
                    if filterCondition in ['gt', 'gte', 'lt', 'lte']:
                        orConditionQuery['bool']['should'].append({
                            "range": {filterField: {filterConditionMap[filterCondition]: filterValue}}
                        })
                    elif filterCondition == 'exists':
                        orConditionQuery['bool']['should'].append({
                            filterConditionMap[filterCondition]: {"field": filterField}
                        })
                    elif filterCondition == 'eq':
                        if filterField in ['matchingStatus', 'merchantId']:
                            orConditionQuery['bool']['should'].append({
                                "term": {f"{filterField}.keyword": filterValue}
                            })
                        else:
                            orConditionQuery['bool']['should'].append({
                                "term": {filterField: filterValue}
                            })
                    else:
                        orConditionQuery['bool']['should'].append({
                            filterConditionMap[filterCondition]: {filterField: filterValue}
                        })
                
                orConditionQuery['bool']['minimum_should_match'] = 1
                processed_conditions.append(orConditionQuery)
            else:
                # Handle regular field conditions
                filterField, filterConditionAndValue = list(condition.items())[0]
                filterCondition, filterValue = list(filterConditionAndValue.items())[0]
                
                if filterField == 'matchingStatus':
                    filterField = 'matchingStatus'
                
                if filterCondition in ['gt', 'gte', 'lt', 'lte']:
                    processed_conditions.append({
                        "range": {filterField: {filterConditionMap[filterCondition]: filterValue}}
                    })
                elif filterCondition == 'exists':
                    processed_conditions.append({
                        filterConditionMap[filterCondition]: {"field": filterField}
                    })
                elif filterCondition == 'eq':
                    if filterField in ['matchingStatus', 'merchantId']:
                        processed_conditions.append({
                            "term": {f"{filterField}.keyword": filterValue}
                        })
                    else:
                        processed_conditions.append({
                            "term": {filterField: filterValue}
                        })
                else:
                    processed_conditions.append({
                        filterConditionMap[filterCondition]: {filterField: filterValue}
                    })
        
        return processed_conditions

    all_conditions = processConditions(filters.get('and', []))
    query['bool']['must'].extend(all_conditions)

    sort = {sortField: {'order': sortDirection}}
    payload = {
        "query": query,
        "sort": sort,
        "size": 10000
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(url, auth=AWSAUTH, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    results = response.json()
    hits = results.get('hits', {}).get('hits', [])
    total = results.get('hits', {}).get('total', {}).get('value', 0)
    
    # Handle pagination if needed
    if total > 10000:
        from_value = 10000
        while from_value < total:
            payload["from"] = from_value
            response = requests.post(url, auth=AWSAUTH, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            more_results = response.json()
            hits.extend(more_results.get('hits', {}).get('hits', []))
            from_value += 10000
    return hits

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