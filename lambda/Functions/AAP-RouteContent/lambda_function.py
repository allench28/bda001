import os
import io
import csv
import uuid
import boto3
import json
import random
import string
import time
from typing import Dict
from decimal import Decimal
from typing import List
from dateutil import parser
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools import Logger, Tracer
from custom_exceptions import BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException
from bedrock_function import promptBedrock
import re

EXTRACTED_EMAIL_TABLE = os.environ.get('EXTRACTED_EMAIL_TABLE')
ROUTE_CONTENT_TABLE = os.environ.get('ROUTE_CONTENT_TABLE')
SKILL_MATRIX_TABLE = os.environ.get('SKILL_MATRIX_TABLE')
MODEL_ID = os.environ.get('MODEL_ID')
MERCHANT_ID = os.environ.get('MERCHANT_ID')

S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource("dynamodb")

EXTRACTED_EMAIL_DDB_TABLE = DDB_RESOURCE.Table(EXTRACTED_EMAIL_TABLE)
ROUTE_CONTENT_DDB_TABLE = DDB_RESOURCE.Table(ROUTE_CONTENT_TABLE)
SKILL_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(SKILL_MATRIX_TABLE)

logger = Logger()
tracer = Tracer()

ROUTE_EXCEPTION = {
    "DATA_CENTER_RUN_RATE_REQUEST": [
        "Za'im",
        "Voon Jian Wei",
        "YW Foo (Chester)"
    ],
    "SOTWARE_RUN_RATE_REQUEST": [
        "Clement Shu",
        "Andy Lai",
        "James Ngiam",
        "Ayden Wong",
        "Sern Hong"
    ],
    "DATA_CENTER_EXCEPTION_BRAND": [
        "HPE",
        "Dell",
        "Lenovo"
    ],
    "SOFTWARE_TEAM_EXCEPTION_BRAND": [
        "VMware",
        "Veeam",
        "Microsoft"
    ]
}


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    logger.info(event)
    try:
        for record in event.get('Records', []):
            # Get the message body
            message_body = record.get('body', '{}')
            if type(message_body) == str:
                message = json.loads(message_body)
            else:
                message = message_body

            productRelatedData = {}
            extractedEmailId = message.get('extractedEmailIds')
            extractedEmailData = queryExtractedEmailById(extractedEmailId)
            productRelatedData['team'] = extractedEmailData['team']
            productRelatedData['brand'] = extractedEmailData['brand']
            tender = extractedEmailData['tender']

            targetedPersonnelList = assignRouteTarget(
                productRelatedData, tender)
            contentRoutingResult = createContentRoutingResult(
                MERCHANT_ID, extractedEmailData, targetedPersonnelList)

        return {
            "status": 200,
            "body": "Success"
        }

    except (BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException) as ex:
        logger.error(f"Custom error: {str(ex)}")
        return {
            "status": 400,
            "body": str(ex)
        }

    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {
            "status": 500,
            'body': "The server encountered an unexpected condition that prevented it from fulfilling your request."
        }


@tracer.capture_method
def queryExtractedEmailById(extractedEmailId):
    """
    Query the DynamoDB table using the extractedEmailId.
    """
    results = []
    try:
        for emailId in extractedEmailId:
            response = EXTRACTED_EMAIL_DDB_TABLE.get_item(
                Key={'extractedEmailId': extractedEmailId[0]}
            ).get('Item', '')

        return response

    except Exception as ex:
        logger.exception({"message": str(ex)})
        raise BadRequestException(
            f"Failed to query extractedEmailId: {extractedEmailIds}. Error: {str(ex)}")


@tracer.capture_method
def querySkillMatrixByTeam(teams: List, brands: List) -> List[Dict]:
    """
    Query the SKILL_MATRIX_DDB_TABLE based on the given team.

    Parameters:
    - team: The team name to query the skill matrix for.

    Returns:
    - A list of dictionaries containing the skill matrix data for the given team.
    """

    results = []
    try:
        for team in teams:
            # Build filter expression: (primaryBrand contains any brand) OR (secondaryBrand contains any brand)
            filter_expr = None
            brand_filter = None
            for brand in brands:
                expr = (Attr('primaryBrand').contains(brand)) | (
                    Attr('secondaryBrand').contains(brand))
                brand_filter = expr if brand_filter is None else brand_filter | expr

            # Always include Team Lead and Assistant Team Lead regardless of brand
            role_filter = (Attr('roleName').eq('Team Lead')) | (
                Attr('roleName').eq('Assistant Team Lead'))
            filter_expr = brand_filter | role_filter if brand_filter is not None else role_filter

            if team == 'Software Team':
                response = SKILL_MATRIX_DDB_TABLE.query(
                    IndexName='gsi-team',
                    KeyConditionExpression=Key('team').eq(team)
                ).get('Items', [])
            else:
                response = SKILL_MATRIX_DDB_TABLE.query(
                    IndexName='gsi-team',
                    KeyConditionExpression=Key('team').eq(team),
                    FilterExpression=filter_expr
                ).get('Items', [])
            results.extend(response)
        return results
    except Exception as ex:
        logger.exception({"message": str(ex)})
        raise BadRequestException(
            f"Failed to query skill matrix for teams: {teams}. Error: {str(ex)}")


@tracer.capture_method
def assignRouteTarget(productRelatedData: Dict, tender: bool) -> Dict:
    """Normalize field names in Bedrock response"""
    sendToPersonnel = []
    ccToPersonnel = []

    if isinstance(productRelatedData, dict) and isinstance(tender, bool):
        brands = productRelatedData['brand']
        teams = productRelatedData['team']

        # Fetch the personnel based on specified team (team determined by brand)
        team_list = querySkillMatrixByTeam(teams, brands)

        # Categorize Primary and CC personnel
        for personnel in team_list:
            primary_brands = personnel.get('primaryBrand', [])
            secondary_brands = personnel.get('secondaryBrand', [])
            email = personnel['emailAddress']
            team = personnel['team']
            name = personnel['name']
            role = personnel['roleName']
            payload = {"emailAddress": email +
                       " (" + name + ")", "roleName": team + " " + role}

            # If any brand matches primaryBrand, add to sendToPersonnel
            if any(brand in primary_brands for brand in brands):
                if payload not in sendToPersonnel:
                    sendToPersonnel.append(payload)

            # If any brand matches secondaryBrand, add to ccToPersonnel and Team Lead or Assistant Team Lead always in ccToPersonnel
            # Handling Software Team exception case
            if (
                (
                    any(brand in secondary_brands for brand in brands) or
                    role in ['Team Lead', 'Assistant Team Lead']
                )
                or
                (
                    any(brand in ROUTE_EXCEPTION['SOFTWARE_TEAM_EXCEPTION_BRAND'] for brand in brands) and
                    team == 'Software Team' and
                    not tender
                )
            ):
                if payload not in ccToPersonnel:
                    ccToPersonnel.append(payload)

            # Remove personnel from ccToPersonnel if already existed in sendToPersonnel
            if payload in ccToPersonnel and payload in sendToPersonnel:
                ccToPersonnel.remove(payload)

            # Data Center tender exception
            if (
                team == 'Data Center' and
                tender and
                any(brand in ROUTE_EXCEPTION['DATA_CENTER_EXCEPTION_BRAND'] for brand in brands) and
                (
                    any(brand in secondary_brands for brand in brands) or
                    any(brand in primary_brands for brand in brands)
                ) and
                name in ROUTE_EXCEPTION['DATA_CENTER_RUN_RATE_REQUEST']
            ):
                if payload in sendToPersonnel:
                    sendToPersonnel.remove(payload)
                if payload in ccToPersonnel:
                    ccToPersonnel.remove(payload)
                continue

            # Software Team routing logic
            if team == 'Software Team':
                # Exception personnel only handle primary brand during tender
                if (
                    name in ROUTE_EXCEPTION['SOTWARE_RUN_RATE_REQUEST'] and
                    tender and
                    all(brand not in primary_brands for brand in brands)
                ):
                    if payload in ccToPersonnel:
                        ccToPersonnel.remove(payload)
                    continue

        response_data = {
            "sendToPersonnel": sendToPersonnel,
            "ccToPersonnel": ccToPersonnel
        }

    return response_data


@tracer.capture_method
def clean_analysisResult(analysis_data) -> Dict:
    try:
        json_patterns = [
            r'```(?:json)?\s*([\s\S]*?)\s*```',  # group(1) is the content
            # r'\{[\s\S]*"productName"|"requestType"|"endUserName"|"analysisStatus"[\s\S]*\}',
        ]

        json_str = None
        for pattern in json_patterns:
            json_match = re.search(pattern, analysis_data)
            if json_match:
                # Use group(1) for the first pattern, group(0) for the second
                if pattern.startswith('```'):
                    json_str = json_match.group(1)
                    json_str = json_str.strip()
                    # Remove leading 'json' if present
                    json_str = re.sub(r'^\s*json\s*', '',
                                      json_str, flags=re.IGNORECASE)
                else:
                    json_str = json_match.group(0)
                if json_str:
                    break

        if not json_str:
            start_idx = analysis_data.find('{')
            end_idx = analysis_data.rfind('}') + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = analysis_data[start_idx:end_idx]
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
            else:
                logger.exception(
                    {"message": "Could not locate valid JSON content by brackets"})

        if json_str:
            try:
                analysis_data = sanitizeAndParseJson(json_str)
                return analysis_data
            except json.JSONDecodeError as je:
                logger.exception({"message": f"JSON decode error: {str(je)}"})

        fallback_response = constructFallbackResponse(analysis_data)
        return fallback_response

    except Exception as e:
        logger.exception(
            {"message": f"Exception in clean_analysisResult: {str(e)}"})
        return constructFallbackResponse(analysis_data)


@tracer.capture_method
def createContentRoutingResult(
    merchantId: str,
    extractedEmailData: Dict,
    targetedPersonnelList: Dict
) -> Dict:
    """
    Create a routing result record for DynamoDB
    """
    routeContents = []
    # Generate unique ID for the routing result
    routeContentId = str(uuid.uuid4())
    now = datetime.now().strftime('%d-%m-%YT%H:%M:%S.%fZ')
    sendToPersonnelEmail = targetedPersonnelList['sendToPersonnel']
    ccToPersonnelEmail = targetedPersonnelList['ccToPersonnel']
    primaryEmailIds = [record['emailAddress']
                       for record in sendToPersonnelEmail if "emailAddress" in record and record["emailAddress"]]
    ccEmailIds = [record['emailAddress']
                  for record in ccToPersonnelEmail if "emailAddress" in record and record["emailAddress"]]

    extractedEmailId = extractedEmailData['extractedEmailId']
    isTender = extractedEmailData.get('tender', False)
    vendor = extractedEmailData.get('vendor', '-')
    productName = extractedEmailData.get('product', '-')
    brand = [record for record in extractedEmailData['brand'] if record]
    senderEmailAddress = extractedEmailData['senderEmailAddress']
    dateReceived = extractedEmailData.get('emailSentDate', '-')
    dateSentOut = now

    # Create the record dictionary
    record = {
        'routeContentId': routeContentId,
        'merchantId': MERCHANT_ID,
        'senderEmailAddress': senderEmailAddress,
        'extractedEmailId': extractedEmailId,
        'primaryEmailIds': primaryEmailIds,
        'ccEmailIds': ccEmailIds,
        'dateReceived': dateReceived,
        'dateSentOut': dateSentOut,
        'isTender': isTender,
        'vendor': vendor,
        'product': productName,
        'brand': brand,
        'createdAt': now,
        'createdBy': 'System',
        'updatedAt': now,
        'updatedBy': 'System',
    }

    # Convert all floats to Decimal for DynamoDB compatibility
    record = convertToDynamodbFormat(record)

    # Store the record in DynamoDB
    response = ROUTE_CONTENT_DDB_TABLE.put_item(Item=record)
    return routeContents


@tracer.capture_method
def constructFallbackResponse(result: str) -> Dict:
    """Construct fallback response when parsing fails"""
    return result

# Helper function to convert floats to Decimal for DynamoDB


@tracer.capture_method
def convertToDynamodbFormat(item):
    """Convert all floats in a dict to Decimal for DynamoDB storage"""
    if isinstance(item, dict):
        return {k: convertToDynamodbFormat(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convertToDynamodbFormat(i) for i in item]
    # elif isinstance(item, float) or isinstance(item, int):
    #     return Decimal(str(item))
    return item


@tracer.capture_method
def sanitizeAndParseJson(json_str):
    try:
        # First attempt to parse as is
        return json.loads(json_str)
    except json.JSONDecodeError:
        # If it fails, try to fix common issues

        # 1. Replace newlines in string values
        # This regex finds strings inside quotes and replaces newlines with spaces
        pattern = r'("(?:\\.|[^"\\])*")'

        def replace_newlines(match):
            return match.group(0).replace('\n', ' ')

        sanitized_str = re.sub(pattern, replace_newlines, json_str)

        # 2. Remove trailing commas in objects and arrays
        sanitized_str = re.sub(r',\s*}', '}', sanitized_str)
        sanitized_str = re.sub(r',\s*\]', ']', sanitized_str)

        try:
            # Try parsing the sanitized string
            return json.loads(sanitized_str)
        except json.JSONDecodeError as e:
            # If still failing, try a more brute force approach
            # Remove all newlines and excess whitespace
            compressed_str = re.sub(r'\s+', ' ', json_str).strip()

            try:
                return json.loads(compressed_str)
            except json.JSONDecodeError:
                # If all else fails, provide a more helpful error message
                raise ValueError(
                    f"Could not parse JSON even after sanitization. Original error: {str(e)}")
