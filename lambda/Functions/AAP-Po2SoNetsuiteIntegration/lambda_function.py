import boto3
import json
import os
import requests
from requests_oauthlib import OAuth1
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger, Tracer

NETSUITE_CREDENTIALS_TABLE = os.environ.get('NETSUITE_CREDENTIALS_TABLE')

dynamodb = boto3.resource('dynamodb')

NETSUITE_CREDENTIALS_DDB_TABLE = dynamodb.Table(NETSUITE_CREDENTIALS_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    merchant_id = event.get('merchantId')
    
    # Fetch credentials from DynamoDB
    response = NETSUITE_CREDENTIALS_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchant_id),
    )
    
    if not response.get('Items'):
        logger.error(f"No credentials found for merchant ID: {merchant_id}")
        return {
            'statusCode': 404,
            'body': json.dumps('Credentials not found')
        }
        
    creds = response.get('Items')[0]

    consumer_key = creds['consumerKey']
    consumer_secret = creds['consumerSecret']
    token_id = creds['tokenId']
    token_secret = creds['tokenSecret']
    account_id = creds['accountId']

    # Build NetSuite URL for retrieving items
    base_url = f'https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/'
    
    # Optional: Add query parameters to filter items
    # params = {
    #    'limit': 10,  # Limit the number of results
    #    'q': 'displayname IS NOT NULL'  # Example filter
    # }

    try:
        # Create OAuth1 auth object
        auth = get_oauth1_auth(account_id, consumer_key, consumer_secret, token_id, token_secret)
        
        # Send the request to get items
        response = requests.get(
            url=f'{base_url}inventoryitem',  # You can also use 'item' to get all item types
            auth=auth,
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
            # params=params  # Uncomment to use query parameters
        )

        # Check the response
        if response.status_code == 200:
            logger.info('Items retrieved successfully')
            return {
                'statusCode': 200,
                'body': json.dumps(response.json())
            }
        else:
            logger.error(f"Failed to retrieve items. Status Code: {response.status_code}, Response: {response.text}")
            return {
                'statusCode': response.status_code,
                'body': json.dumps(response.text)
            }
    except Exception as e:
        logger.exception(f"Error retrieving items: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

@tracer.capture_method
def get_oauth1_auth(account_id, consumer_key, consumer_secret, token_id, token_secret):
    """
    Create OAuth1 auth object for NetSuite REST API
    """
    # Add the realm (account ID) to the OAuth parameters
    oauth = OAuth1(
        client_key=consumer_key,
        client_secret=consumer_secret,
        resource_owner_key=token_id,
        resource_owner_secret=token_secret,
        realm=account_id
    )
    
    return oauth