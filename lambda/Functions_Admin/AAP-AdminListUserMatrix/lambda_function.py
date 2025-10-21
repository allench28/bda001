import os
import boto3
import json
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import AuthenticationException, AuthorizationException, BadRequestException

USER_MATRIX_TABLE = os.environ.get('USER_MATRIX_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

USER_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(USER_MATRIX_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, _, _ = is_authenticated(event)
        user = get_user(sub)
        merchantId = user.get('merchantId')
        userGroupId = user.get('userGroupId')
        user_group_name = get_user_group(userGroupId).get('userGroupName')

        # merchant = get_merchant_resp(merchantId)
                
        # cacheKey = 'merchant_' + merchantId + '_userGroup_' + userGroupId + '#UserMatrixList'
        # cachedItems = getCacheValue(cacheKey, True)
        # if cachedItems:
        #     userMatrixList = cachedItems
        # else:
        userMatrixResp = getUserMatrix(userGroupId)
        userMatrixList = formatUserMatrix(userMatrixResp)
        # setCacheValue(cacheKey, userMatrixList, True)
            
        return create_response(200, 'Successfully returned userMatrixList', {
            'userGroupName': user_group_name,
            'userMatrixList': userMatrixList
        })
        
    except (AuthenticationException, AuthorizationException, BadRequestException) as ex:
        logger.error(f"Custom error: {str(ex)}")
        return create_response(400, ex.message)
    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return create_response(
            500, 
            "The server encountered an unexpected condition that prevented it from fulfilling your request."
        )

@tracer.capture_method
def formatUserMatrix(userMatrixResp):
    userMatrixMapping = {}
    # process second level submodule first
    userMatrixResp = sorted(userMatrixResp, key=lambda x: x.get('parentUserMatrixId') or '')
    for userMatrix in userMatrixResp:
        module = userMatrix.get('module')
        payload = {
                'title': userMatrix.get('subModule'),
                'canAdd': userMatrix.get('canAdd'),
                'canDelete': userMatrix.get('canDelete'),
                'canEdit': userMatrix.get('canEdit'),
                'canList': userMatrix.get('canList'),
                'canView': userMatrix.get('canView'),
                'userMatrixId': userMatrix.get('userMatrixId')
            }
        
        # second level submodule
        if not userMatrix.get('parentUserMatrixId'):
            if module not in userMatrixMapping:
                userMatrixMapping[module] = [payload]
            else:
                userMatrixMapping[module].append(payload)
        # third level submodule
        else:
            parentUserMatrix = USER_MATRIX_DDB_TABLE.get_item(Key={'userMatrixId':userMatrix.get('parentUserMatrixId')}).get('Item')
            parentModule = parentUserMatrix.get('module')
            for submodule in userMatrixMapping[parentModule]:
                if submodule['title'] == userMatrix.get('module'):
                    if not submodule.get('children'):
                        submodule['children'] = [payload]
                    else:
                        submodule['children'].append(payload)
            
    userMatrixList = []
    for key, val in userMatrixMapping.items():
        userMatrixList.append({
            'module': key,
            'children': val
            })
    
    return userMatrixList

@tracer.capture_method
def get_user_group_resp(userGroupId):
    userGroup = USER_GROUP_DDB_TABLE.get_item(
        Key = {'userGroupId': userGroupId}
    ).get('Item')
    if not userGroup:
        raise BadRequestException('User Group Not Found')
    
    return userGroup
    
@tracer.capture_method
def getUserMatrix(userGroupId):
    userMatrixResp = USER_MATRIX_DDB_TABLE.query(
        IndexName='gsi-userGroupId',
        KeyConditionExpression=Key('userGroupId').eq(userGroupId)
    ).get('Items')
    if not userMatrixResp:
        raise BadRequestException('User Matrix Not Found')
    
    return userMatrixResp

@tracer.capture_method
def get_merchant_resp(merchantId):
    merchantResp = MERCHANT_DDB_TABLE.get_item(
        Key={'merchantId': merchantId}
    ).get('Item')

    if not merchantResp:
        raise BadRequestException('Merchant Not Found')
        
    return merchantResp

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
        })
    }