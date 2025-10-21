import json
import redis
import os
import boto3
import requests
from datetime import datetime
from requests_aws4auth import AWS4Auth
from aws_lambda_powertools import Logger, Tracer

ES_ENDPOINT = os.environ.get('ES_ENDPOINT')
REDIS_ENDPOINT = os.environ.get('REDIS_ENDPOINT')
REDIS_PORT = os.environ.get('REDIS_PORT')

CREDENTIALS = boto3.Session().get_credentials()
ACCESS_KEY = CREDENTIALS.access_key
SECRET_ACCESS_KEY = CREDENTIALS.secret_key
AWSAUTH = AWS4Auth(ACCESS_KEY, SECRET_ACCESS_KEY, 'ap-southeast-1', 'es', session_token=CREDENTIALS.token)

redisClient = redis.Redis(
    host=REDIS_ENDPOINT,
    port=REDIS_PORT
)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        action = event['action']
        key = event.get('key', None)
        value = event.get('value', None)
        isJson = event.get('isJson', True)

        if action == 'getValue':
            return getValue(key, isJson)
        elif action == 'getPrefixKey':
            return getPrefixKey(key, isJson)
        elif action == 'setValue':
            return setValue(key, value, isJson)
        elif action == 'deleteValue':
            return deleteValue(key)
        elif action == 'batchDeleteValue':
            return batchDeleteValue(key)
        
        return False
        
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return False
    
@tracer.capture_method
def getValueFromES(key):
    url = 'https://{}/{}/_doc/{}'.format(ES_ENDPOINT, 'rediscache', key)
    response = requests.get(url, auth=AWSAUTH)
    if response.status_code == 200:
        data = response.json()
        data = data.get('_source', None)
        if not data:
            return None
        ttlValue = data.get('ttlTimestamp')
        # example ttlValue = 1728632528
        ttl = datetime.fromtimestamp(ttlValue)
        if ttl < datetime.now():
            deleteValueFromES(key)
            return None
        updateTtlValueFromES(key)
        return data.get('value')
    else:
        return None

@tracer.capture_method
def deleteValueFromES(key):
    url = 'https://{}/{}/_doc/{}'.format(ES_ENDPOINT, 'rediscache', key)
    response = requests.delete(url, auth=AWSAUTH)
    if response.status_code == 200:
        return True
    else:
        return False

@tracer.capture_method
def updateTtlValueFromES(key):
    url = 'https://{}/{}/_update/{}'.format(ES_ENDPOINT, 'rediscache', key)
    payload = {
        "doc": {
            "ttlTimestamp": int(datetime.now().strftime("%s")) + 86400
        }
    }
    response = requests.post(url, auth=AWSAUTH, json=payload)
    if response.status_code == 200:
        return True
    else:
        return False

@tracer.capture_method
def setValueToES(key, value):
    url = 'https://{}/{}/_doc/{}'.format(ES_ENDPOINT, 'rediscache', key)
    payload = {
        "ttlTimestamp": int(datetime.now().strftime("%s")) + 86400,
        "value": value
    }
    response = requests.put(url, auth=AWSAUTH, json=payload)
    if response.status_code == 200:
        return True
    else:
        return False

@tracer.capture_method
def getValue(key, isJson = False):
    try:
        value = redisClient.get(key)
        if isJson == True and value:
            return json.loads(value)
        else:
            return value
    except Exception as ex:
        value = getValueFromES(key)
        if isJson == True and value:
            return json.loads(value)
        else:
            return value

@tracer.capture_method
def setValue(key, value, isJson = False):
    try:
        if isJson == True:
            redisClient.set(key, json.dumps(value))
        else:
            redisClient.set(key, value) 
    except:
        if isJson == True:
            setValueToES(key, json.dumps(value))
        else:
            setValueToES(key, value)
    return True

@tracer.capture_method
def deleteValue(key):
    try:
        redisClient.delete(key)    
        return True  
    except:
        deleteValueFromES(key)    
        return True  

@tracer.capture_method
def getPrefixKey(prefixKey, isJson = False):
    firstScanResult = []
    prefix = prefixKey + '*'
    index, result = redisClient.scan(0, prefix, 1000)
    firstScanResult += result
    
    while index != 0:
        index, result = redisClient.scan(index, prefix, 1000)
        firstScanResult += result
    
    finalScanResult = list(set(firstScanResult))

    listOfKeys = []
    
    for row in finalScanResult:
        listOfKeys.append(str(row,'UTF-8'))

    return listOfKeys

@tracer.capture_method
def batchDeleteValue(keyList):
    for key in keyList:
        redisClient.delete(key)    
    return True 
        