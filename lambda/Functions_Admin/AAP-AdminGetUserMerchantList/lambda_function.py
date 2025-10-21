import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools import Logger, Tracer
from identityHelper import getIdentity
from custom_exceptions import BadRequestError, NotFoundError

DDB_RESOURCE = boto3.resource('dynamodb')

USER_TABLE = os.environ.get('USER_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')

USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)
MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        identity = event.get('identity')
        if not identity:
            return {
                'statusCode': 400,
                'body': json.dumps({'status': False, 'message': "Invalid credentials"})
            }

        username = identity.get('username')
        userResponse = USER_DDB_TABLE.query(
            IndexName='gsi-cognitoUsername',
            KeyConditionExpression=Key('cognitoUsername').eq(username)
        )
        if len(userResponse.get('Items')) == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({'status': False, 'message': 'User not found'})
            }

        merchantList = []
        headers = event.get('requestContext', {}).get('headers', {})
        for item in userResponse.get('Items'):
            merchantInfo = getMerchantInfo(item.get('merchantId'))

            merchantDetail = {
                'merchantId': item.get('merchantId'),
                'name': merchantInfo.get('name'),
            }
            merchantList.append(merchantDetail)
            
        return {
            'statusCode': 200,
            'body': json.dumps({'items': [merchantList[0]]})
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
def getMerchantInfo(merchantId):
    merchantRes = MERCHANT_DDB_TABLE.get_item(
        Key={'merchantId': merchantId}
    ).get('Item')

    if not merchantRes:
        raise NotFoundError('Merchant not found!')

    return merchantRes
