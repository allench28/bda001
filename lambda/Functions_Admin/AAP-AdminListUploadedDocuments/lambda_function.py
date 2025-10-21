import os
import json
import boto3
import requests
import math
from requests_aws4auth import AWS4Auth
from datetime import datetime, timedelta
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.conditions import Key, Attr
from authorizationHelper import is_authenticated, Permission, has_permission, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException
from decimal import Decimal


CREDENTIALS = boto3.Session().get_credentials()
ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-5', 'es', session_token=CREDENTIALS.token)

MERCHANT_TABLE = os.environ.get("MERCHANT_TABLE")
EXTRACTED_DOCUMENTS_TABLE = os.environ.get("EXTRACTED_DOCUMENTS_TABLE")
EXTRACTED_GRN_TABLE = os.environ.get("EXTRACTED_GRN_TABLE")
EXTRACTED_PO_TABLE = os.environ.get("EXTRACTED_PO_TABLE")
EXTRACTED_REFERRAL_LETTER_TABLE = os.environ.get("EXTRACTED_REFERRAL_LETTER_TABLE")
ES_DOMAIN = os.environ.get("ES_DOMAIN")

DDBRESOURCE = boto3.resource('dynamodb')

MERCHANT_DDB_TABLE = DDBRESOURCE.Table(MERCHANT_TABLE)
EXTRACTED_DOCUMENTS_DDB_TABLE = DDBRESOURCE.Table(EXTRACTED_DOCUMENTS_TABLE)
EXTRACTED_GRN_DDB_TABLE = DDBRESOURCE.Table(EXTRACTED_GRN_TABLE)
EXTRACTED_PO_DDB_TABLE = DDBRESOURCE.Table(EXTRACTED_PO_TABLE)
EXTRACTED_REFERRAL_LETTER_DDB_TABLE = DDBRESOURCE.Table(EXTRACTED_REFERRAL_LETTER_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_id = user.get('userId')
        merchant_id = user.get('merchantId')
        user_group = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group, Permission.GET_ALL_DOCUMENTS.value)
        request_body = json.loads(event.get('body', '{}'))

        sortField = 'createdAt'
        sortDirection = 'asc'
        if request_body.get('sort') is not None: 
            sortField = request_body.get('sort').get('field')
            sortDirection = request_body.get('sort').get('direction')
        
        filters = request_body.get('filter')
        if not filters:
            filters = {}

        documentType = request_body.get('documentType')
        
        limit = request_body.get('limit')
        nextToken = request_body.get('nextToken')
        
        if not limit:
            limit = 10
        
        if not nextToken:
            nextToken = 0
        nextToken = int(nextToken)
        
        data, totalData = getDataFromES(merchant_id, sortField, sortDirection, filters, limit, nextToken, documentType) # childrenMerchantIdList
        data = formatResponse(data, documentType)


        return create_response(200, "Success", {
            "total": totalData,
            "nextToken": nextToken + limit,
            "items": data
        })

    except (AuthenticationException, AuthorizationException, BadRequestException) as ex:
        return create_response(400, ex.message)
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return create_response(500, "The server encountered an unexpected condition that prevented it from fulfilling your request.")
    
@tracer.capture_method
def query_extracted_documents(document_upload_id):
    response = EXTRACTED_DOCUMENTS_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents

@tracer.capture_method
def query_extracted_grn(document_upload_id):
    response = EXTRACTED_GRN_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents

@tracer.capture_method
def query_extracted_po(document_upload_id):
    response = EXTRACTED_PO_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents
        
@tracer.capture_method
def query_extracted_referral_letter(document_upload_id):
    response = EXTRACTED_REFERRAL_LETTER_DDB_TABLE.query(
        IndexName='gsi-documentUploadId',
        KeyConditionExpression=Key('documentUploadId').eq(document_upload_id),
    )

    extracted_documents = response.get('Items', [])

    for documents in extracted_documents:
        del documents['merchantId']

    return extracted_documents

@tracer.capture_method
def formatResponse(data, documentType):
    if documentType == 'grn':
        for item in data:
            extracted_grn = query_extracted_grn(item.get('documentUploadId'))
            item['documentList'] = extracted_grn
            del item['merchantId']

    elif documentType == 'po':
        for item in data:
            extracted_po = query_extracted_po(item.get('documentUploadId'))
            item['documentList'] = extracted_po
            del item['merchantId']

    elif documentType == 'medicalReferralLetter':
        for item in data:
            extracted_referral_letter = query_extracted_referral_letter(item.get('documentUploadId'))
            item['documentList'] = extracted_referral_letter
            del item['merchantId']

    else:
        for item in data:
            extracted_documets = query_extracted_documents(item.get('documentUploadId'))
            item['documentList'] = extracted_documets
            del item['merchantId']
        
    return data

@tracer.capture_method
def getDataFromES(merchantId, sortField, sortDirection, filters, limit, nextToken, documentType): #childrenMerchantIdList
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
            'regexp': 'regexp',
            'exists': 'exists'
        }

    url = 'https://' + ES_DOMAIN + "/documentupload/_doc/_search"
    if filters.get('and') and len(filters.get('and')) > 0   :
        filters['and'].append({'merchantId': {'eq': merchantId}})
    else: 
        filters['and'] = [{'merchantId': {'eq': merchantId}}]

    if documentType:
        filters['and'].append({'documentType': {'eq': documentType}})

    query = {
            'bool': {
                'must': []
            }
        }
    
    
    filterExpression = None
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
    payload['size'] = limit
    payload['from'] = nextToken
    payload['track_total_hits'] = True
    
    payloadES = json.dumps(payload)
    headers = {
            'Content-Type': "application/json",
            'User-Agent': "PostmanRuntime/7.20.1",
            'Accept': "application/json, text/plain, */*",
            'Cache-Control': "no-cache",
            'Postman-Token': "1ae2b03c-ac6c-45f4-9b37-4f95b9b0102c,b678f18f-3ebe-458e-b63b-6ced7b74851f",
            'Host': ES_DOMAIN,
            'Accept-Encoding': "gzip, deflate, br",
            'Connection': "keep-alive",
            'cache-control': "no-cache",
        }

    response = requests.request("GET", url, data=payloadES, headers=headers, auth=AWSAUTH)
    responseText = json.loads(response.text)
    if 'error' in responseText:
        raise BadRequestException("Invalid query statement")
    
    totalResp = responseText.get('hits').get('total').get('value')
    currentEsLimit = int(getCurrentESLimit())
    
    if totalResp > currentEsLimit:
        newLimit = math.ceil(totalResp / 100) * 100
        setCurrentESLimit(newLimit)
    eInvoiceList = [item.get('_source') for item in responseText.get('hits').get('hits')]
    return eInvoiceList, totalResp


def getCurrentESLimit():
    esUrl = f'https://{ES_DOMAIN}/documentupload/_settings'
    headers = {
            'Content-Type': "application/json",
            'User-Agent': "PostmanRuntime/7.20.1",
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Postman-Token': "1ae2b03c-ac6c-45f4-9b37-4f95b9b0102c,b678f18f-3ebe-458e-b63b-6ced7b74851f",
            'Host': ES_DOMAIN,
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }


    payload = { }
    
    response = requests.request("GET", esUrl, data=json.dumps(payload), headers=headers, auth=AWSAUTH)
    responseText = json.loads(response.text)
    maxResultWindow = responseText.get('documentupload').get('settings').get('index').get('max_result_window')
    if maxResultWindow is None:
        maxResultWindow = '10000'
    return maxResultWindow

def setCurrentESLimit(newLimit):
    esUrl = f'https://{ES_DOMAIN}/documentupload/_settings'
    headers = {
            'Content-Type': "application/json",
            'User-Agent': "PostmanRuntime/7.20.1",
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Postman-Token': "1ae2b03c-ac6c-45f4-9b37-4f95b9b0102c,b678f18f-3ebe-458e-b63b-6ced7b74851f",
            'Host': ES_DOMAIN,
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }


    payload = { 
        'max_result_window': newLimit
    }
    
    requests.request("PUT", esUrl, data=json.dumps(payload), headers=headers, auth=AWSAUTH)

@tracer.capture_method
def getMerchantFromDDB(merchantId):
    response = MERCHANT_DDB_TABLE.get_item(
        Key={'merchantId': merchantId}
    )
    return response.get('Item', {})

@tracer.capture_method
def getMerchantIdFromFilter(filters):
    if not filters:
        return []
    merchantIdList = []
    for key, value in filters.items():
        if key == 'merchantId':
            merchantId = value.get('eq')
            if merchantId:
                merchantIdList.append(merchantId)
        elif key == 'and' or key == 'or':
            for item in value:
                merchantIdList.extend(getMerchantIdFromFilter(item))
    return merchantIdList

@tracer.capture_method
def create_response(status_code, message, payload=None):
    if not payload:
        payload = {}

    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Content-Security-Policy': "default-src 'self'; script-src 'self'",
            'X-Content-Type-Options': 'nosniff',
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
            'Cache-control': 'no-store',
            'Pragma': 'no-cache',
            'X-Frame-Options':'SAMEORIGIN'
        },
        'body': json.dumps({
            "statusCode": status_code, 
            "message": message, 
            **payload
        },  cls=DecimalEncoder)
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)