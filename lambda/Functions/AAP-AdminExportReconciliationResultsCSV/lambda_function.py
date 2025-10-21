import json
import boto3
from datetime import datetime, timedelta
from dateutil import parser
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
        jobId = event.get('jobId')
        itemIdList = arguments.get('itemIdList')
        reconciliationType = arguments.get('reconciliationType')
        reconciliationSubType = arguments.get('reconciliationSubType')
        output_type = arguments.get('outputType', 'csv')
        document_type = 'reconciliation' 

        sortField = 'createdAt'
        sortDirection = 'asc'
        if arguments.get('sort') is not None:
            sortField = arguments.get('sort').get('field')
            sortDirection = arguments.get('sort').get('direction')
            if sortField in [
                    'merchantId',
                    'reconciliationResultsId',
                    'createdBy',
                    'updatedBy'
                ]:
                sortField += '.keyword'

        filters = arguments.get('filter', {})

        if reconciliationType is not None:
            if filters.get('and') is None:
                filters['and'] = []

            filters['and'].append({
                'reconciliationType': {
                    'eq': reconciliationType
                }
            })
        
        if reconciliationSubType is not None:
            if filters.get('and') is None:
                filters['and'] = []

            filters['and'].append({
                'reconciliationSubType': {
                    'eq': reconciliationSubType
                }
            })

        reconciliationResults = getDataFromES(merchant_id, sortField, sortDirection, filters, itemIdList)

        mapping_config = getMappingConfig(merchant_id, document_type, output_type)
        
        section_key = buildSectionKey(reconciliationType, reconciliationSubType)
        
        section_config = mapping_config.get(section_key, mapping_config.get('exportDefault', {}))

        header = section_config.get('headers', [])

        processedResultCSVRows = processReconciliationResultsList(reconciliationResults, header, section_config)
        
        if not processedResultCSVRows:
            logger.info("No data found for the given filters")
            updateDownloadJobStatus(jobId, 'COMPLETED', 'No data found')
            return {
                'status': True,
                'message': 'No data found',
            }
        
        currentDateTime = datetime.strftime((datetime.now()+timedelta(hours=8)), '%Y-%m-%d_%H:%M:%S')

        reconTypeFormatted = section_key.replace('export', '')
        filename = 'ReconciliationResult_{}{}.csv'.format(reconTypeFormatted, currentDateTime)
        zipFile = 'ReconciliationResult_{}{}.zip'.format(reconTypeFormatted, currentDateTime)
        if os.path.exists('/tmp/' + filename):
            os.remove('/tmp/' + filename)

        with open('/tmp/' + filename, 'a', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(header)
            writer.writerows(processedResultCSVRows)

        with ZipFile('/tmp/' + zipFile, 'w') as zip:
            zip.write('/tmp/' + filename, filename)

        S3_CLIENT.upload_file('/tmp/' + zipFile, S3_BUCKET, 'export/reconciliation-results/'+zipFile)

        objectPresignedURL = S3_CLIENT.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': 'export/reconciliation-results/'+zipFile
            }
        )
        os.remove('/tmp/' + filename)
        os.remove('/tmp/' + zipFile)
        updateDownloadJobStatus(jobId, 'COMPLETED', 'Job Completed', S3_BUCKET+'export/reconciliation-results/'+zipFile, objectPresignedURL)

        return {
            'status': True,
            'message': 'Job Completed',
            'objectPresignedUrl': objectPresignedURL,
        }
    
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

@tracer.capture_method
def buildSectionKey(reconciliation_type, reconciliation_subtype=None):
    """
    Build section key based on reconciliation type and subtype
    
    Args:
        reconciliation_type (str): Main reconciliation type ('salesAmount', 'settlementAmount')
        reconciliation_subtype (str): Optional subtype ('foodMarketplace', 'creditCard')
    
    Returns:
        str: Section key for mapping lookup
    """
    if not reconciliation_type:
        return 'exportDefault'
    
    base_key = f'export{reconciliation_type[0].upper() + reconciliation_type[1:]}'
    
    if reconciliation_subtype:
        # Convert subtype to proper case (foodMarketplace -> FoodMarketplace)
        subtype_formatted = reconciliation_subtype[0].upper() + reconciliation_subtype[1:]
        section_key = f'{base_key}{subtype_formatted}'
    else:
        section_key = base_key
    
    return section_key

@tracer.capture_method
def getDataFromES(merchantId, sortField, sortDirection, filters, selectedItems):
    sortField = 'branchCode.keyword'
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

    url = f'https://{ES_DOMAIN_ENDPOINT}/reconciliationresults/_doc/_search'

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

    #sort by branchCode and transactionDate
    resultList = sorted(resultList, key=lambda x: (x['_source'].get('branchCode'), x['_source'].get('transactionDate')))
    return resultList

@tracer.capture_method
def processReconciliationResultsList(reconciliationResults, header, section_config):
    """Process the reconciliation results and format them for CSV export.
    
    Args:
        reconciliationResults (list): List of reconciliation results from Elasticsearch.
        header (list): List of header display names for the CSV file.
        section_config (dict): Section configuration containing field mappings and formats.
    Returns:
        list: List of lists, where each inner list represents a row in the CSV file."""
    
    reconciliationResultsList = []
    
    # Extract mappings and formats from section config
    document_fields = section_config.get('document_fields', {})
    field_formats = section_config.get('header_formats', {})
    date_parsing_config = section_config.get('date_parsing', {})
    
    for key in reconciliationResults:
        document = key['_source']
        
        # Process each header
        payload = []
        for header_name in header:
            # Get the actual document field name from mapping
            document_field = document_fields.get(header_name, header_name)
            value = document.get(document_field, '')
            
            # Apply formatting if specified
            format_pattern = field_formats.get(header_name)
            if format_pattern and value:
                value = applyFormat(value, format_pattern, date_parsing_config)
            
            payload.append(value)

        reconciliationResultsList.append(payload)

    return reconciliationResultsList

@tracer.capture_method
def applyFormat(value, format_pattern, date_parsing_config=None):
    """
    Apply formatting based on a pattern string.
    
    Args:
        value: The value to format
        format_pattern: Pattern like "{date:YYYY-MM-DD}"
        date_parsing_config: Optional configuration for date parsing
        
    Returns:
        Formatted string value
    """
    if value is None or value == '':
        return value
    
    import re
    pattern_match = re.match(r'^(.*?)\{(\w+):([^}]+)\}(.*?)$', format_pattern)
    if not pattern_match:
        return str(value)
    
    prefix, format_type, format_spec, suffix = pattern_match.groups()
    
    if format_type == 'date':
        formatted_value = formatDate(value, format_spec, date_parsing_config)
    else:
        formatted_value = str(value)
    
    return prefix + formatted_value + suffix

@tracer.capture_method
def formatDate(value, spec, date_parsing_config=None):
    """Format date with strftime specification"""
    if not value:
        return ''
    
    date_config = date_parsing_config or {}
    
    strftime_format = (spec
                      .replace('YYYY', '%Y')
                      .replace('MM', '%m')
                      .replace('DD', '%d')
                      .replace('HH', '%H')
                      .replace('mm', '%M')
                      .replace('ss', '%S'))
    
    try:
        parsed = parser.parse(
            str(value), 
            dayfirst=date_config.get('dayfirst', True),
            yearfirst=date_config.get('yearfirst', False)
        )
        
        if 'T' in str(value) or ':' in str(value):
            parsed += timedelta(hours=8)
        
        return parsed.strftime(strftime_format)
    except (parser.ParserError, ValueError, TypeError):
        return str(value)

@tracer.capture_method
def getMappingConfig(merchant_id, document_type, output_type):
    s3_key = f"mapping/{merchant_id}/{document_type}/output/{output_type}.json"
    response = S3_CLIENT.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=s3_key
    )
    if 'Contents' in response:
        response = S3_CLIENT.get_object(
            Bucket=S3_BUCKET,
            Key=s3_key
        )
        mapping_config = json.loads(response['Body'].read().decode('utf-8'))
    else:
        default_mapping_key = f"mapping/default/{document_type}/{output_type}_default.json"
        response = S3_CLIENT.get_object(
            Bucket=S3_BUCKET,
            Key=default_mapping_key
        )
        mapping_config = json.loads(response['Body'].read().decode('utf-8'))
    return mapping_config

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