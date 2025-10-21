import os
import uuid
import boto3
import json
import decimal
from datetime import datetime
from aws_lambda_powertools import Logger, Tracer
from identityHelper import getUser, checkUserPermission
from custom_exceptions import BadRequestError, NotFoundError
from boto3.dynamodb.conditions import Key, Attr

SQS_URL = os.environ.get('SQS_URL')
USER_MATRIX_TABLE = os.environ.get('USER_MATRIX_TABLE')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
SQS = boto3.client('sqs')

USER_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(USER_MATRIX_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
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
        username = user.get('name')

        if arguments.get('userGroupId'):
            checkUserPermission(merchantId, merchantUserGroupId, 'User', 'User Group', 'canEdit')
            isNewUserGroupName = checkNewUserGroupName(arguments)
            updateUserGroup(arguments, username, now)
            updateUserMatrix(arguments, merchantId, username, now) 
            # if isNewUserGroupName:
            #     payload = {
            #         'merchantId': merchantId,
            #         'userGroupId': arguments.get('userGroupId'),
            #         'userGroupName': arguments.get('userGroupName')
            #     }
            #     sendToSQS(payload)
            
        else:
            checkUserPermission(merchantId, merchantUserGroupId, 'User', 'User Group', 'canAdd')           
            arguments['userGroupId'] = str(uuid.uuid4())
            createUserGroup(arguments, merchantId, username, now)
            createUserMatrix(arguments, merchantId, username, now) 
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': True,
                'message': 'Successfully created/updated userGroup & userMatrix!',
                'userGroupId': arguments.get('userGroupId')
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
def createUserGroup(arguments, merchantId, username, now):
    payload = {
        'userGroupId': arguments.get('userGroupId'),
        'merchantId': merchantId,
        'userGroupName': arguments.get('userGroupName'),
        'totalUser': 0,
        'createdAt': now,
        'createdBy': username,
        'updatedAt': now,
        'updatedBy': username,
    }
    USER_GROUP_DDB_TABLE.put_item(Item=payload)

@tracer.capture_method
def createUserMatrix(arguments, merchantId, username, now):
    with USER_MATRIX_DDB_TABLE.batch_writer() as batch:
        for userMatrix in arguments.get('userMatrixList'):
            # second level modules
            for submodule in userMatrix.get('children'):
                userMatrixId = str(uuid.uuid4())
                payload = {
                    'userMatrixId': userMatrixId,
                    'parentUserMatrixId': None,
                    'merchantId': merchantId,
                    'userGroupId': arguments.get('userGroupId'),
                    'module': userMatrix.get('module'),
                    'subModule': submodule.get('title'),
                    'canAdd': submodule.get('canAdd'),
                    'canDelete': submodule.get('canDelete'),
                    'canEdit': submodule.get('canEdit'),
                    'canList': submodule.get('canList'),
                    'canView': submodule.get('canView'),
                    'createdAt': now,
                    'createdBy': username,
                    'updatedAt': now,
                    'updatedBy': username,
                    }
                
                batch.put_item(Item=payload)
                
                # third level modules
                if submodule.get('children'):
                    for child in submodule.get('children'):
                        childPayload = {
                            'userMatrixId': str(uuid.uuid4()),
                            'parentUserMatrixId': userMatrixId,
                            'merchantId': merchantId,
                            'userGroupId': arguments.get('userGroupId'),
                            'module': submodule.get('title'),
                            'subModule': child.get('title'),
                            'canAdd': child.get('canAdd'),
                            'canDelete': child.get('canDelete'),
                            'canEdit': child.get('canEdit'),
                            'canList': child.get('canList'),
                            'canView': child.get('canView'),
                            'createdAt': now,
                            'createdBy': username,
                            'updatedAt': now,
                            'updatedBy': username,
                        }
                        batch.put_item(Item=childPayload)

@tracer.capture_method
def updateUserGroup(arguments, username, now):
    payload = {
        'userGroupName': arguments.get('userGroupName'),
        'updatedAt': now,
        'updatedBy': username,
    }
    updateExpression = "Set "
    expressionAttributesNames = {}
    expressionAttributesValues = {}

    for key, value in payload.items():
        updateExpression += ", "+ "#" + str(key) + " = :"+str(key) if updateExpression != "Set " else "#" + str(key)+" = :"+str(key)
        expressionAttributesNames['#'+str(key)] = str(key)
        expressionAttributesValues[':'+str(key)] = value

    USER_GROUP_DDB_TABLE.update_item(
        Key={
            'userGroupId': arguments.get('userGroupId')
        },
        UpdateExpression=updateExpression,
        ExpressionAttributeNames=expressionAttributesNames,
        ExpressionAttributeValues=expressionAttributesValues
    )

@tracer.capture_method
def updateUserMatrix(arguments, merchantId, username, now):
    oldUserMatrixIds = getOldUserMatrixIds(arguments.get('userGroupId'))
    deletedUserMatrixList(oldUserMatrixIds)
    createUserMatrix(arguments, merchantId, username, now)
        
@tracer.capture_method
def checkNewUserGroupName(arguments):
    userGroupId = arguments.get('userGroupId')
    newUserGroupName = arguments.get('userGroupName')
    
    userGroup = USER_GROUP_DDB_TABLE.get_item(Key={'userGroupId': userGroupId}).get('Item')
    
    if userGroup and userGroup.get('userGroupName') != newUserGroupName:
        return True
        
    return False

@tracer.capture_method
def sendToSQS(arguments):
    
    payload = json.dumps(arguments,cls=DecimalEncoder)
    
    sqsResponse = SQS.send_message(
        QueueUrl = SQS_URL,
        MessageBody = payload
    )

    return sqsResponse

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)
        
@tracer.capture_method
def getOldUserMatrixIds(userGroupId):
    userMatrixResp = USER_MATRIX_DDB_TABLE.query(
        IndexName='gsi-userGroupId',
        KeyConditionExpression=Key('userGroupId').eq(userGroupId)
    ).get('Items')
    
    return [
        item.get('userMatrixId') for item in userMatrixResp
    ]
    
@tracer.capture_method
def deletedUserMatrixList(oldUserMatrixIds):
    for userMatrixId in oldUserMatrixIds:
        USER_MATRIX_DDB_TABLE.delete_item(
            Key={'userMatrixId': userMatrixId}
        )