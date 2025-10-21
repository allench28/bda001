import os
import boto3
import json
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger, Tracer
from identityHelper import getUser
from custom_exceptions import BadRequestError, NotFoundError

USER_MATRIX_TABLE = os.environ.get('USER_MATRIX_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')

USER_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(USER_MATRIX_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # Extracting the body from the API Gateway event
        body = event.get('body')
        if body:
            body = json.loads(body)
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'status': False, 'message': 'Invalid request body.'})
            }

        arguments = body.get('arguments')
        identity = body.get('identity')
        if not identity:
            return {
                'statusCode': 400,
                'body': json.dumps({'status': False, 'message': 'Invalid User.'})
            }

        cognitoUsername = identity.get('username')
        user = getUser(cognitoUsername)
        merchantId = user.get('merchantId')

        userGroupId = arguments.get('userGroupId')
        if not userGroupId:
            raise BadRequestError('Missing UserGroupId Field.')

        userGroup = getUserGroupResp(userGroupId)

        cacheKey = 'merchant_' + merchantId + '_userGroup_' + userGroupId + '#UserMatrixList'
        cachedItems = getCacheValue(cacheKey, True)
        if cachedItems:
            userMatrixList = cachedItems
        else:
            userMatrixResp = getUserMatrix(userGroupId)
            userMatrixList = formatUserMatrix(userMatrixResp)
            setCacheValue(cacheKey, userMatrixList, True)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'userGroupId': userGroupId,
                'userGroupName': userGroup.get('userGroupName'),
                'userMatrixList': userMatrixList,
                'status': True,
                'message': 'Successfully return userMatrixList'
            })
        }

    except (BadRequestError, NotFoundError) as ex:
        return {
            'statusCode': 400,
            'body': json.dumps({'status': False, 'message': str(ex)})
        }
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {
            'statusCode': 500,
            'body': json.dumps({'status': False, 'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."})
        }

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
def getUserGroupResp(userGroupId):
    userGroup = USER_GROUP_DDB_TABLE.get_item(
        Key = {'userGroupId': userGroupId}
    ).get('Item')
    if not userGroup:
        raise NotFoundError('User Group Not Found')
    
    return userGroup
    
@tracer.capture_method
def getUserMatrix(userGroupId):
    userMatrixResp = USER_MATRIX_DDB_TABLE.query(
        IndexName='gsi-userGroupId',
        KeyConditionExpression=Key('userGroupId').eq(userGroupId)
    ).get('Items')
    if not userMatrixResp:
        raise NotFoundError('User Matrix Not Found')
    
    return userMatrixResp
