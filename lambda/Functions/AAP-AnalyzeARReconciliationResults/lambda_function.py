import json
import boto3
import os
from datetime import datetime
from decimal import Decimal
from typing import Dict
from aws_lambda_powertools import Logger, Tracer
from bedrock_function import promptBedrock
import re

# Environment variables
RECONCILIATION_RESULTS_TABLE = os.environ.get('RECONCILIATION_RESULTS_TABLE')

# Initialize AWS clients
DDB_RESOURCE = boto3.resource('dynamodb')
SQS_CLIENT = boto3.client('sqs')

RECONCILIATION_RESULTS_DDB_TABLE = DDB_RESOURCE.Table(RECONCILIATION_RESULTS_TABLE)

"""
Sample Event
body
 {
    'merchantId': value,
    'reconciliationType': value,
    'reconciliationSubType': value,
    'reconciliationData': data,
}

"""

"""
ReconciliationResults Table Design

{
    'reconciliationResultsId': reconciliationResultId,
    'merchantId': MERCHANT_ID,
    'currency': "MYR",
    'branchCode': record.get('branchCode', ''),
    'branchName': record.get('branchName', ''),
    'salesMerchant': "Grab",
    'salesChannel': "GrabPay",
    'transactionDate': record.get('transactionDate', ''),
    'gatewayTransactionDate': record.get('transactionDate', ''),
    'gatewaySettlementDateTime': record.get('settlementDate', ''),
    'settlementId': record.get('settlementId', ''),
    'totalSalesAmount': totalSalesAmount,
    'totalTaxAmount': totalTaxAmount,
    'totalGatewaySalesAmount': totalGatewaySalesAmount,
    'totalGatewayTransactionAmount': totalGatewayTransactionAmount,
    'totalGatewayCreditAmount': totalGatewayCreditAmount,
    'totalGatewayProcessingFee': totalGatewayProcessingFee,
    'erpCreditAmount': erpCreditAmount,
    'erpDebitAmount': erpDebitAmount,
    'matchingStatus': status,
    'varianceAmount': record.get('varianceAmount', 0),
    'exceptionCategory': None,
    'exceptionDescription': exceptionDescription,
    'recommendedAction': None,
    'reconciliationType': reconciliationType,
    'remarks': None,
    'confidenceScore': 0,
    'createdAt': now,
    'createdBy': "System",
    'updatedAt': now,
    'updatedBy': "System",
    'branchUUID': paymentGatewayData.get('branchUUID', ''),
    'bankRef': bankStatementData.get('bankRef', ''),
    'vendorRef': bankStatementData.get('vendorRef', ''),
    'bankTransactionDate': bankStatementData.get('transactionDate', ''),
    'bankCreditAmount': bankStatementData.get('creditAmount', 0),
    'bankName': bankStatementData.get('bankName', ''),
}

"""

# Initialize logging and tracing
logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        
        # Process each SQS message (Lambda may receive up to 10 messages at once)
        for record in event.get('Records', []):
            
            # Get the message body
            message_body = record.get('body', '{}')
            if type(message_body) == str:
                message = json.loads(message_body)
            else:
                message = message_body
            
            # Extract data from the message
            merchantId = message.get('merchantId')
            reconciliationData = message.get('reconciliationData', {})
            reconciliationType = message.get('reconciliationType')
            reconciliationSubType = message.get('reconciliationSubType')  # NEW FIELD

            formattedData = formatDataForBedrock(reconciliationType, reconciliationSubType, reconciliationData)
            
            # Use Bedrock to analyze the match result
            bedrockResult = analyzeWithBedrock(formattedData, reconciliationType, reconciliationSubType)
            
            createReconciliationResultRecord(merchantId, reconciliationData, bedrockResult)
              
        
        # Return success response
        return {
            "status": 200,
            "body": 'Processing completed successfully',
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
def formatDataForBedrock(reconciliationType: str, reconciliationSubType: str, data: Dict) -> Dict:
    """
    Format data for Bedrock based on reconciliation type and sub-type
    """
    formattedData = {}
    if reconciliationType == 'salesAmount':
        if reconciliationSubType == 'foodMarketplace':
            # Food marketplace includes ERP data
            formattedData = {
                "Date": data.get("transactionDate"),
                "Store Name": data.get("branchName"),
                "Store Code": data.get("branchCode"),
                "Merchant": data.get("salesMerchant"),
                "Type": data.get("salesChannel"),
                "Transaction Net Sales": data.get("totalGatewaySalesAmount"),
                "Sales Report Total": data.get("totalSalesAmount"),
                "Odoo Credit": data.get("erpCreditAmount"),
                "Odoo Debit": data.get("erpDebitAmount"),
                "Variance": data.get("varianceAmount"),
                "Status": data.get("matchingStatus"),
                "Reconciliation Sub Type": "Food Marketplace"
            }
        elif reconciliationSubType == 'creditCard':
            # Credit card reconciliation without ERP data
            formattedData = {
                "Date": data.get("transactionDate"),
                "Store Name": data.get("branchName"),
                "Store Code": data.get("branchCode"),
                "Merchant": data.get("salesMerchant"),
                "Type": data.get("salesChannel"),
                "Transaction Net Sales": data.get("totalGatewaySalesAmount"),
                "Sales Report Total": data.get("totalSalesAmount"),
                "Variance": data.get("varianceAmount"),
                "Status": data.get("matchingStatus"),
                "Reconciliation Sub Type": "Credit Card"
            }
    elif reconciliationType == 'settlementAmount':
        formattedData = {
            "Date": data.get("transactionDate"),
            "Store Name": data.get("branchName"),
            "Store Code": data.get("branchCode"),
            "Merchant": data.get("salesMerchant"),
            "Type": data.get("salesChannel"),
            "Transaction Total": data.get("totalGatewayTransactionAmount"),
            "Transfer Net": data.get("totalGatewayCreditAmount"),
            "Bank": data.get("bankName"),
            "Bank Settlement Amount": data.get("bankCreditAmount"),
            "Variance": data.get("varianceAmount"),
            "Status": data.get("matchingStatus"),
            "Reconciliation Sub Type": reconciliationSubType
        }
    
    return formattedData

@tracer.capture_method
def analyzeWithBedrock(formattedData, reconciliationType, reconciliationSubType):
    try:
        # Construct prompt for Claude with exception categories based on sub-type
        if reconciliationType == 'salesAmount' and reconciliationSubType == 'foodMarketplace':
            prompt = """
            You are a reconciliation assistant for Food Marketplace transactions.
            Based on the input data from a food marketplace reconciliation report, generate the following fields:
            - Status Description – A short, clear narrative explaining the reconciliation result or discrepancy
            - Exception Type – A concise classification (e.g. Missing Data, Amount Mismatch, Late File)
            - Recommended Action – A practical step the finance team or accountant should take to resolve the issue
            - Variance Amount – The amount difference identified between the relevant fields in the input data

            Input will contain fields from Food Marketplace Sales Amount Reconciliation:
            1. Date
            2. Store Name
            3. Store Code
            4. Merchant (Grab)
            5. Type (GrabPay, GrabFood)
            6. Transaction Net Sales
            7. Sales Report Total
            8. Odoo Credit
            9. Odoo Debit
            10. Status

            Only generate outputs when Status is ❌ Mismatched or 📝 Adjusted Matched. If Status is ✅ Matched, return:
            Status Description: All values reconciled
            Exception Type: None
            Recommended Action: No action needed
            Variance Amount: The amount difference identified between any of the following pairs:
            - Transaction Net Sales and Sales Report Total
            - Transaction Net Sales and Odoo Credit
            - Sales Report Total and Odoo Debit

            Input Data:
            INPUT_DATA_PLACEHOLDER

            Format your response as a JSON object with the following structure:
            {{
            "documentLevelAnalysis": {{
                "confidenceScore": 0-100,
                "exceptionCategory": "Exception Type",
                "exceptionDescription": "Status Description",
                "recommendedAction": "Recommended Action",
                "varianceAmount": "Variance Amount"
                }},
            }}
            """
        elif reconciliationType == 'salesAmount' and reconciliationSubType == 'creditCard':
            prompt = """
            You are a reconciliation assistant for Credit Card transactions.
            Based on the input data from a credit card reconciliation report, generate the following fields:
            - Status Description – A short, clear narrative explaining the reconciliation result or discrepancy
            - Exception Type – A concise classification (e.g. Missing Data, Amount Mismatch, Late File)
            - Recommended Action – A practical step the finance team or accountant should take to resolve the issue
            - Variance Amount – The amount difference identified between the relevant fields in the input data

            Input will contain fields from Credit Card Sales Amount Reconciliation:
            1. Date
            2. Store Name
            3. Store Code
            4. Merchant (CreditCard)
            5. Type (CREDIT_CARD, QR, TNG, CIMB_BONUS_POINT)
            6. Transaction Net Sales
            7. Sales Report Total
            8. Status

            Note: Credit card reconciliation does not include ERP (Odoo) matching, only sales vs payment transaction matching.

            Only generate outputs when Status is ❌ Mismatched. If Status is ✅ Matched, return:
            Status Description: All values reconciled
            Exception Type: None
            Recommended Action: No action needed
            Variance Amount: The amount difference identified between Transaction Net Sales and Sales Report Total

            Input Data:
            INPUT_DATA_PLACEHOLDER

            Format your response as a JSON object with the following structure:
            {{
            "documentLevelAnalysis": {{
                "confidenceScore": 0-100,
                "exceptionCategory": "Exception Type",
                "exceptionDescription": "Status Description",
                "recommendedAction": "Recommended Action",
                "varianceAmount": "Variance Amount"
                }},
            }}
            """
        else:
            # Default prompt for settlement or other types
            prompt = """
            You are a reconciliation assistant.
            Based on the input data from a reconciliation report, generate the following fields:
            - Status Description – A short, clear narrative explaining the reconciliation result or discrepancy
            - Exception Type – A concise classification (e.g. Missing Data, Amount Mismatch, Late File)
            - Recommended Action – A practical step the finance team or accountant should take to resolve the issue
            - Variance Amount – The amount difference identified between the relevant fields in the input data

            Input Data:
            INPUT_DATA_PLACEHOLDER

            Format your response as a JSON object with the following structure:
            {{
            "documentLevelAnalysis": {{
                "confidenceScore": 0-100,
                "exceptionCategory": "Exception Type",
                "exceptionDescription": "Status Description",
                "recommendedAction": "Recommended Action",
                "varianceAmount": "Variance Amount"
                }},
            }}
            """

        json_data = json.dumps(formattedData, default=str)
        prompt = prompt.replace("INPUT_DATA_PLACEHOLDER", json_data)
        
        # Call Bedrock
        result, input_tokens, output_tokens = promptBedrock(prompt)
        
        # Handle Bedrock failure scenario or empty response
        if not result or result == "Bedrock Failure":
            return {
                "documentLevelAnalysis": {
                    "confidenceScore": 0,
                    "exceptionCategory": "Bedrock Failure",
                    "exceptionDescription": "Bedrock Failure",
                    "recommendedAction": "Manual review required",
                    "varianceAmount": 0
                },
                "lineItemAnalysis": []
            }
            
        
        # Try to extract JSON from text
        try:
            # First, try to find JSON in code blocks (enclosed in triple backticks)
            json_patterns = [
                r'```(?:json)?\s*([\s\S]*?)\s*```',  # JSON in code blocks with or without language spec
                r'\{[\s\S]*"documentLevelAnalysis"[\s\S]*\}',  # Find complete JSON object with required key
                r'\{[\s\S]*"lineItemAnalysis"[\s\S]*\}'  # Alternative key to look for
            ]
            
            json_str = None
            for pattern in json_patterns:
                json_match = re.search(pattern, result)
                if json_match:
                    json_str = json_match.group(0)
                    if not json_str.startswith('{'):
                        # If we got the inner content from backticks, ensure it's a JSON object
                        if re.match(r'\s*\{', json_str):
                            # It starts with whitespace then {, so strip and use it
                            json_str = json_str.strip()
                        else:
                            # Not a valid JSON object, keep looking
                            json_str = None
                    
                    if json_str:
                        break
            
            # If no JSON was found with regex, try to clean up the response and parse it
            if not json_str:
                # Check if response has line-by-line JSON (from the logs it appears this way)
                if '"documentLevelAnalysis"' in result and '{' in result and '}' in result:
                    # Try to extract just the JSON portion by finding common markers
                    start_idx = result.find('{')
                    end_idx = result.rfind('}') + 1
                    
                    if start_idx >= 0 and end_idx > start_idx:
                        json_str = result[start_idx:end_idx]
                        # Clean up any trailing commas which can cause parsing errors
                        json_str = re.sub(r',\s*}', '}', json_str)
                        json_str = re.sub(r',\s*]', ']', json_str)
                        
                    else:
                        print("Could not locate valid JSON content by brackets")
                else:
                    print("No JSON-like content detected in response")
            
            # Parse the JSON, whether found by regex or reconstructed
            if json_str:
                try:
                    analysis_data = sanitizeAndParseJson(json_str)
                    
                    # Normalize field names
                    analysis_data = normalizeBedrockResponse(analysis_data)
                    
                    return analysis_data
                except json.JSONDecodeError as je:
                    print(f"JSON decode error: {str(je)}")
            
            # If we get here, parsing failed - build a fallback response with any fragments we can find
            fallback_response = constructFallbackResponse(result)
            return fallback_response
                    
        except Exception as e:
            # Return simple Bedrock failure message
            return {
                "documentLevelAnalysis": {
                    "confidenceScore": 0,
                    "exceptionCategory": "Bedrock Failure",
                    "exceptionDescription": f"Failed to parse Bedrock response: {str(e)}",
                    "recommendedAction": "Manual review required",
                    "varianceAmount": 0
                },
                "lineItemAnalysis": []
            }
        
    except Exception as e:
        return {
            "documentLevelAnalysis": {
                "confidenceScore": 0,
                "exceptionCategory": "Bedrock Failure",
                "exceptionDescription": f"Error in analyzeWithBedrock function: {str(e)}",
                "recommendedAction": "Manual review required",
                "varianceAmount": 0
            },
            "lineItemAnalysis": []
        }

@tracer.capture_method
def constructFallbackResponse(result: str) -> Dict:
    """Construct fallback response when parsing fails"""
    return {
        "documentLevelAnalysis": {
            "confidenceScore": 0,
            "exceptionCategory": "Processing Error",
            "exceptionDescription": "Failed to parse Bedrock response",
            "recommendedAction": "Manual review required",
            "varianceAmount": 0
        },
        "lineItemAnalysis": []
    }

@tracer.capture_method
def normalizeBedrockResponse(response_data: Dict) -> Dict:
    """Normalize field names in Bedrock response"""
    if isinstance(response_data, dict):
        # Document level normalization
        if 'documentLevelAnalysis' in response_data:
            doc_analysis = response_data['documentLevelAnalysis']
            # Normalize status field names
            if 'matchingStatus' in doc_analysis and 'matchStatus' not in doc_analysis:
                doc_analysis['matchStatus'] = doc_analysis.pop('matchingStatus')
            
        # Line item normalization
        if 'lineItemAnalysis' in response_data:
            for item in response_data['lineItemAnalysis']:
                if 'matchingStatus' in item and 'matchStatus' not in item:
                    item['matchStatus'] = item.pop('matchingStatus')
                
    return response_data

@tracer.capture_method
def createReconciliationResultRecord(merchantId: str, reconciliationData: Dict, bedrockResponse: Dict) -> Dict:
    """
    Create a reconciliation result record for DynamoDB
    """
    
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Check for duplicates based on uniqueKey before inserting
    unique_key = reconciliationData.get('uniqueKey')
    if unique_key:
        try:
            # Check if this record already exists
            response = RECONCILIATION_RESULTS_DDB_TABLE.query(
                IndexName='gsi-uniqueKey',
                KeyConditionExpression=boto3.dynamodb.conditions.Key('uniqueKey').eq(unique_key),
                Limit=1
            )
            
            if response.get('Items'):
                logger.info(f"Duplicate record found with uniqueKey: {unique_key}, skipping insert")
                # Return the existing record to maintain function consistency
                return response['Items'][0]
                
        except Exception as e:
            logger.error(f"Error checking for duplicates: {str(e)}")
    
    # Create the record dictionary
    documentLevelAnalysis = bedrockResponse.get('documentLevelAnalysis', {})
    exceptionCategory = documentLevelAnalysis.get('exceptionCategory')
    if exceptionCategory == "None":
        exceptionCategory = ""

    for fields in reconciliationData:
        if isinstance(reconciliationData[fields], float):
            reconciliationData[fields] = Decimal(str(reconciliationData[fields]))

    record = {
        **reconciliationData,
        'exceptionCategory': exceptionCategory,
        'exceptionDescription': documentLevelAnalysis.get('exceptionDescription'),
        'recommendedAction': documentLevelAnalysis.get('recommendedAction'),
        'confidenceScore': documentLevelAnalysis.get('confidenceScore'),
        'varianceAmount': Decimal(str(documentLevelAnalysis.get('varianceAmount'))),
        'createdAt': now,
        'updatedAt': now,
    }
    
    RECONCILIATION_RESULTS_DDB_TABLE.put_item(Item=record)
    return record

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
                raise ValueError(f"Could not parse JSON even after sanitization. Original error: {str(e)}")
