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
import uuid


CREDENTIALS = boto3.Session().get_credentials()
ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-1', 'es', session_token=CREDENTIALS.token)

MERCHANT_TABLE = os.environ.get("MERCHANT_TABLE")
DOCUMENT_UPLOAD_TABLE = os.environ.get("DOCUMENT_UPLOAD_TABLE")
ES_DOMAIN = os.environ.get("ES_DOMAIN")

DDBRESOURCE = boto3.resource('dynamodb')

MERCHANT_DDB_TABLE = DDBRESOURCE.Table(MERCHANT_TABLE)
DOCUMENT_UPLOAD_DDB_TABLE = DDBRESOURCE.Table(DOCUMENT_UPLOAD_TABLE)

logger = Logger()
tracer = Tracer()

DOCUMENT_TYPE_INDEX = {
    "bankStatement": "bankstatement",
    "paymentGateway": "paymentgatewayreport",
    "salesStatement": "salesstatement",
}

SORT_FIELD = {
    "bankStatement": "bankRef.keyword",
    "paymentGateway": "transactionId.keyword",
    "salesStatement": "transactionId.keyword",
}

PAYMENT_METHOD_MAPPING = {
    "CREDIT_CARD": "Credit Card",
    "DEBIT_CARD": "Debit Card",
    "EWALLET": "E-Wallet",
    "CASH": "Cash",
    "TNG": "TnG"
}

REVENUE_CENTER_MAPPING = {
    "HOTEL": "Hotel",
    "CARPARK": "Carpark",
    "FOOD": "F&B"
}

FEE_TYPE_MAPPING = {
    "FX": "FX Fee",
    "PLATFORM": "Platform Fee",
    "MDR": "MDR"
}

STATUS_MAPPING = {
    "SUCCESS": "Success",
    "FAILED": "Failed"
}

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        user_id = user.get('userId')
        merchant_id = user.get('merchantId')
        user_group = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group, Permission.GET_RECONCILIATION_DOCUMENTS.value)
        request_body = json.loads(event.get('body', '{}'))

        sortField = 'createdAt'
        sortDirection = 'asc'
        if request_body.get('sort') is not None: 
            sortField = request_body.get('sort').get('field')
            sortDirection = request_body.get('sort').get('direction')
        
        filters = request_body.get('filter')
        if not filters:
            filters = {}
        
        limit = request_body.get('limit')
        nextToken = request_body.get('nextToken')
        documentType = request_body.get('documentType')
        
        if not limit:
            limit = 10
        
        if not nextToken:
            nextToken = 0
        nextToken = int(nextToken)
        
        data, totalData = getDataFromES(merchant_id, sortField, sortDirection, filters, limit, nextToken, documentType)
        formattedData = formatResponse(documentType, data)

        return create_response(200, "Success", {
            'items': formattedData,
            'nextToken': nextToken + limit,
            'total': totalData,
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
def formatResponse(documentType, data):
    formattedData = []
    for item in data:
        del item['merchantId']
    
    ## For payment gateway report & sales statement, return records in line items level instead of document level
    for item in data:
        for key, value in item.items():
            if value == "" or value == None:
                item[key] = "-"
        if documentType == 'paymentGateway':
            newItem = item.copy()
            newItem['paymentMethod'] = PAYMENT_METHOD_MAPPING[item.get('paymentMethod')] 
            newItem['revenueCenter'] = REVENUE_CENTER_MAPPING[item.get('revenueCenter')]
            newItem['status'] = STATUS_MAPPING[item.get('status')]
            if item.get('feeType'):
                newItem['feeType'] = FEE_TYPE_MAPPING[item.get('feeType')]
            else:
                newItem['feeType'] = "-"
            formattedData.append(newItem)
        elif documentType == 'salesStatement':
            newItem = item.copy()
            newItem['paymentMethod'] = PAYMENT_METHOD_MAPPING[item.get('paymentMethod')] 
            newItem['revenueCenter'] = REVENUE_CENTER_MAPPING[item.get('revenueCenter')]
            formattedData.append(newItem)
        else:
            formattedData.append(item)
    return formattedData

@tracer.capture_method
def getDataFromES(merchantId, sortField, sortDirection, filters, limit, nextToken, documentType):
    if documentType is not None and documentType in DOCUMENT_TYPE_INDEX:
        index = DOCUMENT_TYPE_INDEX[documentType]
        url = 'https://' + ES_DOMAIN + "/" + index + "/_doc/_search" 
        sortField = SORT_FIELD[documentType]
        sortDirection = "asc"
    else:
        raise BadRequestException("Invalid document type")

    print("url", url)

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

    if filters.get('and') and len(filters.get('and')) > 0   :
        filters['and'].append({'merchantId': {'eq': merchantId}})
    else: 
        filters['and'] = [{'merchantId': {'eq': merchantId}}]

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
    
    print("payload", payload)
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
    currentEsLimit = int(getCurrentESLimit(index))
    
    if totalResp > currentEsLimit:
        newLimit = math.ceil(totalResp / 100) * 100
        setCurrentESLimit(index, newLimit)
    responseList = [item.get('_source') for item in responseText.get('hits').get('hits')]
    return responseList, totalResp


def getCurrentESLimit(index):
    esUrl = f'https://{ES_DOMAIN}/{index}/_settings'
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
    maxResultWindow = responseText.get(index).get('settings').get('index').get('max_result_window')
    if maxResultWindow is None:
        maxResultWindow = '10000'
    return maxResultWindow

def setCurrentESLimit(index, newLimit):
    esUrl = f'https://{ES_DOMAIN}/{index}/_settings'
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
        }, cls=DecimalEncoder)
    }


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, Decimal):
            return str(obj)
        #️ otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)