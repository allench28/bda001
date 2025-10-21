import os
import boto3
import json
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools import Logger, Tracer
from identityHelper import getUser, checkUserPermission
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
        body = json.loads(event.get('body', '{}'))
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
        merchantUserGroupId = user.get('userGroupId')

        if arguments.get('userGroupIdList'):
            checkUserPermission(merchantId, merchantUserGroupId, 'User', 'User Group', 'canDelete')
            checkUserGroup(arguments.get('userGroupIdList'))
            userMatrixIdList = deleteUserGroup(arguments.get('userGroupIdList'))
            deleteUserMatrix(userMatrixIdList)
                        
        if arguments.get('userMatrixIdList'):
            checkUserPermission(merchantId, merchantUserGroupId, 'User', 'User Group', 'canDelete')           
            deleteUserMatrix(arguments.get('userMatrixIdList'))
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': True,
                'message': 'Successfully deleted userGroup & userMatrix!'
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
            'body': json.dumps({
                'status': False,
                'message': "The server encountered an unexpected condition that prevented it from fulfilling your request."
            })
        }

@tracer.capture_method
def deleteUserGroup(userGroupIdList):
    for userGroupId in userGroupIdList:
        USER_GROUP_DDB_TABLE.delete_item(
            Key={'userGroupId': userGroupId}
        )
        userMatrixIdList = getUserMatrixIdList(userGroupId)
    return userMatrixIdList

@tracer.capture_method
def deleteUserMatrix(userMatrixIdList):
    for userMatrixId in userMatrixIdList:
        USER_MATRIX_DDB_TABLE.delete_item(
            Key={'userMatrixId': userMatrixId}
        )

@tracer.capture_method
def getUserMatrixIdList(userGroupId):
    userMatrixIdList = []
    userMatrixResp = USER_MATRIX_DDB_TABLE.query(
        IndexName='gsi-userGroupId',
        KeyConditionExpression = Key('userGroupId').eq(userGroupId)
    ).get('Items')
    for userMatrix in userMatrixResp:
        userMatrixIdList.append(userMatrix.get('userMatrixId'))
    
    return userMatrixIdList
    
@tracer.capture_method
def checkUserGroup(userGroupIdList):
    for userGroupId in userGroupIdList:
        userGroup = USER_GROUP_DDB_TABLE.get_item(
            Key={'userGroupId': userGroupId}
        ).get('Item')
        if userGroup and userGroup.get('totalUser') > 0:
            raise BadRequestError('Can not delete user group that still contains user.')
    return True