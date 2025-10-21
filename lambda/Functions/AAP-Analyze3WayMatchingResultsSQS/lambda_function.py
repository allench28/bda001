import json
import boto3
import os
import uuid
import re
import traceback
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Any
from botocore.config import Config
from aws_lambda_powertools import Logger, Tracer
from bedrock_function import promptBedrock  

logger = Logger()
tracer = Tracer()

S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs')
DDB_RESOURCE = boto3.resource('dynamodb', region_name='ap-southeast-1')

THREE_WAY_MATCHING_RESULTS_TABLE = os.environ.get('THREE_WAY_MATCHING_RESULTS_TABLE')
THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE = os.environ.get('THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')
MODEL_ID = os.environ.get('MODEL_ID')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
JOB_TRACKING_TABLE = os.environ.get('JOB_TRACKING_TABLE')
MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
AGENT_MAPPING_BUCKET = os.environ.get('AGENT_MAPPING_BUCKET')


MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
JOB_TRACKING_DDB_TABLE = DDB_RESOURCE.Table(JOB_TRACKING_TABLE)
THREE_WAY_MATCHING_RESULTS_DDB_TABLE = DDB_RESOURCE.Table(THREE_WAY_MATCHING_RESULTS_TABLE)
THREE_WAY_MATCHING_RESULTS_LINE_ITEM_DDB_TABLE = DDB_RESOURCE.Table(THREE_WAY_MATCHING_RESULTS_LINE_ITEM_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)


MATCH_STATUS_MAP = {
    'Matched': ['No Exception'],
    'Partial Matched': ['Partial Delivery and Split Invoice'],
    'Mismatched': [
        'Quantity Discrepancies',
        'Price Discrepancies', 
        'Item-Level Discrepancies',
        'Amount Discrepancies',
        'Currency Discrepancy',
        'Reference & Document Discrepancies',
        'Supplier Mismatch',
        'Date Discrepancies',
        'Approval or Policy Violations',
        'Missing Key Fields',
        'Duplicate Doc'
    ]
}

def determineMatchStatus(exception_category: str) -> str:
    """
    Determine match status based on exception category
    """
    for status, categories in MATCH_STATUS_MAP.items():
        if exception_category in categories:
            return status
    return 'Mismatched'  # Default to mismatched if category unknown

def validateMatchStatus(analysis_result: Dict) -> Dict:
    """
    Ensure consistent match status between code logic and Bedrock analysis
    """
    doc_analysis = analysis_result['documentLevelAnalysis']
    
    # Handle both field name variations
    current_status = doc_analysis.get('matchStatus', 
                    doc_analysis.get('matchingStatus', 'Unknown'))
    
    # Get expected status from exception category
    expected_status = determineMatchStatus(doc_analysis['exceptionCategory'])
    
    # Add note about GRN total amount
    if 'exceptionDescription' in doc_analysis and 'totalAmount' in doc_analysis['exceptionDescription'] and 'GRN' in doc_analysis['exceptionDescription']:
        doc_analysis['exceptionDescription'] += " (Note: GRN total amount should not be considered for matching)"
    
    # Update if mismatch
    if current_status != expected_status:
        print(f"Correcting match status from {current_status} to {expected_status}")
        doc_analysis['matchStatus'] = expected_status
        
    # Update line items
    for item in analysis_result.get('lineItemAnalysis', []):
        item_status = item.get('matchStatus', 
                     item.get('matchingStatus', 'Unknown'))
        expected_item_status = determineMatchStatus(item['exceptionCategory'])
        
        if item_status != expected_item_status:
            print(f"Correcting line item match status from {item_status} to {expected_item_status}")
            item['matchStatus'] = expected_item_status
            
    return analysis_result

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

THREE_WAY_MATCHING_PROMPT = """
You are a 3-way matching expert responsible for verifying invoice accuracy by comparing data across:
- Purchase Order (PO)
- Goods Receipt Note (GRN)
- Invoice

Your job is to:
1. Match each invoice line item with the PO and GRN for same supplier.
2. Determine if the quantity, price, currency, tax, and unit of measure are consistent across the documents.
3. Detect scenarios like partial deliveries, split invoices, or duplicate entries.
4. Explain your reasoning for each line item.
5. Recommend actions: Approve or Hold for human review.

Purchase Order Data:
{poData}

Goods Receipt Note Data:
{grnData}

Invoice Data:
{invoiceData}

Matching Result:
{matchResult}
        
Return a detailed explanation per line item and an overall recommendation.
1. Match status per item (Matched, Mismatched, Partial Matched)
2. Exception category
3. Exception description
3. Confidence score (0-100)
4. Final recommendation (Approve or Hold for Human Review)

Standardise item codes and description (remove extra spaces, casing, typos etc)
For any UOM (Unit of Measure) fields that are missing or empty, use "EA" (Each) as the default value.
Handle near-matching item descriptions or slight differences in naming for items
    
RULES FOR SUCCESSFUL MATCHING where the matchStatus showed "Matched"

1. **PO Number Consistency**  
   - The same PO number must appear on the Invoice and the GRN.
   - If the PO number is missing or incorrect, mark the invoice as "Mismatched".

2. **Item Code Match**
   - The Item Code on the Invoice must match the PO and GRN for each line item.
   - If it differs or is missing in any document, mark as "Mismatched".

3. **Quantity Alignment**
   - **Invoice Quantity** must not exceed the **GRN Quantity** (received).
   - **GRN Quantity** must not exceed the **PO Ordered Quantity**.
   - If an Invoice is billed before full delivery, and delivery is still ongoing, it's a "Partial Match".
   - If quantities do not align and are not reconcilable, mark as "Mismatched".

4. **Unit Price Match**
   - The unit price on the Invoice must match the PO.
   - If the price differs beyond allowed tolerance, mark as "Mismatched".
   - GRN unit price should not be considered for matching.

5. **UOM (Unit of Measure) Match**
   - Units of measure must be the same across PO, GRN, and Invoice.
   - Mismatched UOMs cause valuation inconsistencies → "Mismatched".

6. **Currency Match**
   - Invoice currency must match PO currency.
   - Any deviation is a "Mismatched" unless explicitly approved.

7. **Tax Consistency**
   - Tax rate and tax amount must align with PO or organizational tax policy.
   - Any mismatch in tax rate or calculation → "Mismatched".

8. **Date Validations**
   - Invoice Date cannot be before PO Date or GRN Date.
   - A delayed invoice (e.g., submitted 30+ days after GRN) may be flagged for review.

9. **Invoice and GRN Linking**
   - For split deliveries or invoices, all must share the same PO.
   - You must track cumulative quantities for matching.
   - Only when total invoiced = total delivered = total ordered → "Matched".

10. **Approval Status**
   - PO must be marked as "Approved".
   - If linked to an unapproved PO, the invoice must be "Escalated".

MATCHING OUTCOME DEFINITION for matchStatus:

- Matched: All fields meet the above rules across all documents.
- Partial Matched: Only some goods/services delivered or invoiced, but traceable to PO. Only when the Exception Category show "Partial Delivery and Split Invoice"
- Mismatched: The fields didnt meet any of the above rules and fall into the Exception Category (1-11)below
  If you dont know what to show for the matchStatus, just showed "Require Human Validation". Dont put any values such as "Matched/ Partial Matched/Mismatched"

        Use ONLY the following categories for exception classification of specified scenarios,
        if any of the scenarios below (1-10) happens, please ensure the matchStatus field showing "Mismatched", not "Successful Matching" 
        if scenario 11 happens, please ensure the matchStatus field showing "Partial Matched", not "Successful Matching", not "Mismatched"
        The matchStatus can only show "Matched": when the exceptionCategory is No Exception, else it will be either Partial Matched or Mismatched.

        1. Quantity Discrepancies
        If Invoice Quantity more than GRN Quantity, this is wrong and cannot be approved for payment.
        If Invoice Quantity more than PO Quantity, , this is wrong and cannot be approved for payment.
        If GRN Quantity more than PO Quantity, this is wrong and cannot be approved for payment.
        
        2. Price Discrepancies
        Unit Price Variance: Invoice unit price doesn't match PO unit price, this is wrong and cannot be approved for payment.
        Unexpected Charges: Invoice includes extra charges not in PO.

        3. Item-Level Discrepancies
        Wrong or Mismatched Item Codes/Descriptions: Item code and/or descriptions don't align across PO, GRN, Invoice.
        Missing Line Items: Items present in PO/GRN not found on the invoice.
        UOM Mismatch: Different units of measure used between PO and Invoice/GRN resulting in inconsistent valuation.

        4. Amount Discrepancies
        Invoice Amount Mismatch: Invoice totals differs from PO totals.

        5. Currency Discrepancy
        Currency Mismatch: Invoice currency differs from PO currency.

        6. Reference & Document Discrepancies
        Missing or Incorrect PO Number: Invoice references invalid/wrong PO.
        GRN Not Available: No record of received goods.
        
        7. Supplier Mismatch
        Supplier ID/Name Mismatch: Supplier details differ across Invoice, PO, and GRN.

        8. Date Discrepancies
        Invoice Date Before PO/GRN Date: Invoice raised before PO creation or goods receipt.

        9. Approval or Policy Violations
        Unapproved PO: Invoice linked to PO that hasn't been approved.

        10. Missing Key Fields
        PO: PO Number, PO Date, Supplier Name, Supplier Address, Currency, Total invoice Amount, Item Code, Description, Unit Price, Quantity, UOM  
        GRN: GRN Number, GRN Date, PO Number, Supplier Name, Item Code, Description, Unit Price, Quantity, UOM
        Invoice: Invoice Number, Invoice Date, PO Number, PO Date, Supplier Name, Supplier Address, Currency, Total invoice Amount, Item Code, Description, Unit Price, Quantity, UOM, Tax Amount, Tax Category
        
        11. Duplicate Doc
        Same Invoice/GRN Number already processed.

        12. Partial Delivery and Split Invoice
        For scenario related to the partial delivery and split invoices, there will be more than 1 GRN or more 1 invoice having the same PO number.
        - **Partial Delivery**: GRN quantity is less than the PO quantity. Expect partial invoice.
        - **Split Invoice**: Same PO is invoiced in multiple documents. Check total invoiced quantity.
        Clearly track split transactions with linked references between PO, Invoice, and GRN.
        
        Given the following data for a purchase order and its corresponding invoice and goods receipt:
        PO: PO number 123 with PO date 1st Jan 2025, 100 units of Item A at $10 each 
        Invoice 1: PO number 123 with Invoice date 5th Jan 2025,60 units of Item A at $10 each
        Invoice 2: PO number 123 with Invoice date 10rd Jan 2025, 40 units of Item A at $10 each
        GRN 1: PO number 123 with GRN date 3rd Jan 2025 , 60 units received
        GRN 2: PO number 123 with GRN date 8th Jan 2025, 40 units received
        Generate a human-readable explanation of the match status. Explain whether this is a partial delivery, split invoicing, and whether there's a mismatch."
        
        This purchase order was split into two deliveries: one for 60 units and another for 40 units, matching the total PO quantity of 100 units, where all 3 documents having same PO number 123.
        When performing matching when receive Invoice 1 on 5th Jan 2025 which related to goods receipt GRN 1, the status should be "partial match".
        Invoice 1 Matching Status (as of 5th Jan 2025):
        PO: 100 units ordered
        GRN available at the time: Only GRN 1 (60 units received)
        Invoice 1: 60 units of Item A billed
        The invoiced quantity matches the first GRN.
        Since only partial delivery and billing are completed at this point:
        Match Status: Partial Matched
        Exception Category: Partial Delivery and Split Invoice
        Exception Description: Partial delivery and split invoice against the PO
        Recommendation: Hold (pending full delivery and final invoice)
        
        When performing matching when receive Invoice 2 on 10th Jan 2025 where the remaining 40 units are delivered, the status of PO, GRN 1, GRN 2, Invoice 1 and Invoice 2 would all be "matched" as they are having same PO number 123.
        Invoice 2 Matching Status (as of 10th Jan 2025):
            PO: Still PO 123 
            GRNs available: GRN 1 (60) + GRN 2 (40) = 100 units received
            Invoices available: Invoice 1 (60) + Invoice 2 (40) = 100 units billed
            All items have been delivered and billed correctly:
            Match Status (Invoice 2): Matched
            Match Status (PO): Matched
            Match Status (Invoice 1, GRN 1, GRN 2): Matched
            Exception Category: No Exception
            Exception Description: No exception as all matched
            Recommendation: Approve
        
        matchingStatus is "Mismatched" when the exception category is any of the following:
            - Quantity Discrepancies
            - Price Discrepancies
            - Item-Level Discrepancies
            - Amount Discrepancies
            - Currency Discrepancy
            - Reference & Document Discrepancies
            - Supplier Mismatch
            - Date Discrepancies
            - Approval Violation
            - Missing Key Fields

        matchingStatus is "Partial Matched" when the exception category is "Partial Delivery and Split Invoice".

        
        OUTPUT FORMAT:
        Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
        DO NOT remove any keys from the input JSON object.
         {{
          "documentLevelAnalysis": {{
            "matchingStatus": "Matched/Partial Matched/Mismatched",
            "confidenceScore": 0-100,
            "exceptionCategory": "ONE OF THE EXACT CATEGORIES ABOVE",
            "exceptionDescription": "Detailed explanation of any issues",
            "recommendedAction": "Approve/Hold with reason"
          }},
          "lineItemAnalysis": [
            {{
              "lineItemNumber": 1,
              "itemCode": "Extract from source data",
              "description": "Extract from source data",
              "poQuantity": "Extract actual PO quantity from source data",
              "grnQuantity": "Extract actual GRN quantity from source data",
              "invoiceQuantity": "Extract actual Invoice quantity from source data",
              "matchingStatus": "Matched/Partial Matched/Mismatched", 
              "confidenceScore": 0-100,
              "exceptionCategory": "ONE OF THE EXACT CATEGORIES ABOVE", 
              "exceptionDescription": "Detailed explanation of any issues",
              "recommendedAction": "Approve/Hold with reason"
            }},
            // repeat for each line item
          ]
        }}
        """

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """
    AWS Lambda handler for analyzing three-way matching results
    
    This function:
    1. Processes SQS messages with matching results
    2. Uses Bedrock to analyze matching results
    3. Stores results in DynamoDB
    4. Records timeline events
    """
    try:
        print(f"Starting analysis of three-way matching results at {datetime.now().isoformat()}")
        
        # Process each SQS message (Lambda may receive up to 10 messages at once)
        successful_messages = []
        failed_messages = []
        
        # Track successful and failed messages by jobTrackingId
        job_tracking_updates = {}
        
        for record in event.get('Records', []):
            try:
                # Get the message body
                message_body = record.get('body', '{}')
                if type(message_body) == str:
                    message = json.loads(message_body)
                else:
                    message = message_body

                if isinstance(message, list) and len(message) > 0:
                    message = message[0]
                
                print(f"Processing message for invoice {message.get('matchResult', {}).get('invoiceNumber')}")
                
                # Extract data from the message
                merchantId = message.get('merchantId')
                poData = message.get('poData')
                grnData = message.get('grnData', [])
                invoiceData = message.get('invoiceData', [])
                matchResult = message.get('matchResult', {})
                poKey = message.get('poKey')
                poFilename = message.get('poFilename')
                grnFilename = message.get('grnFilename')
                
                #Get merchant configuration
                merchant_config = getMerchantConfiguration(merchantId)
                
                # Get the invoice number from the match result
                invoiceNumber = matchResult.get('invoiceNumber', 'Unknown')
                
                print(f"Analyzing match result for invoice {invoiceNumber}")
                
                # UPDATED: Use Bedrock to analyze with merchant config
                bedrock_result = analyzeWithBedrock(poData, grnData, invoiceData, matchResult, merchant_config)
                
                # Normalize Bedrock response
                bedrock_result = normalizeBedrockResponse(bedrock_result)
                
                # Validate and correct match status
                bedrock_result = validateMatchStatus(bedrock_result)
                
                # Store the result in DynamoDB
                storeResultsInDynamoDb([matchResult], poData, grnData, invoiceData, 
                                         merchantId, poKey, poFilename, grnFilename, bedrock_result)
                
                # Add message to successful list
                successful_messages.append(record['messageId'])
                
                # Track success by jobTrackingId
                jobTrackingId = message.get('jobTrackingId')
                if jobTrackingId:
                    if jobTrackingId not in job_tracking_updates:
                        job_tracking_updates[jobTrackingId] = {'completed': 0, 'failed': 0}
                    job_tracking_updates[jobTrackingId]['completed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing message {record.get('messageId')}: {str(e)}")
                logger.error(traceback.format_exc())
                
                # Track failure by jobTrackingId
                jobTrackingId = message.get('jobTrackingId')
                if jobTrackingId:
                    if jobTrackingId not in job_tracking_updates:
                        job_tracking_updates[jobTrackingId] = {'completed': 0, 'failed': 0}
                    job_tracking_updates[jobTrackingId]['failed'] += 1
                
                # Add message to failed list
                failed_messages.append(record['messageId'])
        
        # Update job tracking records
        for jobTrackingId, stats in job_tracking_updates.items():
            updateJobTracking(
                jobTrackingId, 
                completed=stats['completed'],
                failed=stats['failed']
            )
        
        # Return processing results
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Analysis of three-way matching results completed',
                'successful': len(successful_messages),
                'failed': len(failed_messages),
                'timestamp': datetime.now().isoformat()
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error in analyze_matching_results lambda: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error in analyze_matching_results lambda: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })
        }

@tracer.capture_method
def analyzeWithBedrock(poData, grnData, invoiceData, matchResult, merchant_config):
    """
    Analyze matching results using Amazon Bedrock with Claude
    Enhanced with merchant-specific Python prompts and better error handling
    """
    try:
        # Get default prompt
        default_prompt = THREE_WAY_MATCHING_PROMPT
        
        # Get prompt path from merchant config (same field as PBEO.py)
        prompt_paths = merchant_config.get('promptPaths', {})
        three_way_matching_prompt_path = prompt_paths.get('threeWayMatchingPrompt')
        
        # Fetch custom prompt or use default
        prompt_template = fetchPythonPrompt(three_way_matching_prompt_path, default_prompt)
        
        prompt = prompt_template.format(
            poData=poData if poData else {},
            grnData=grnData if grnData else [],
            invoiceData=invoiceData if invoiceData else [],
            matchResult=matchResult if matchResult else {}
        )
        
        # Call Bedrock
        result, input_tokens, output_tokens = promptBedrock(prompt)
        
        # Debug the raw response
        print(f"Raw Bedrock response type: {type(result)}")
        print(f"Raw Bedrock response preview: {result[:100]}..." if result else "Empty response")
        
        # Handle Bedrock failure scenario or empty response
        if not result or result == "Bedrock Failure":
            logger.error("Bedrock returned empty response or explicit failure")
            return {
                "documentLevelAnalysis": {
                    "matchingStatus": "Unknown",
                    "confidenceScore": 0,
                    "exceptionCategory": "Bedrock Failure",
                    "exceptionDescription": "Bedrock Failure",
                    "recommendedAction": "Manual review required"
                },
                "lineItemAnalysis": []
            }
        
        # Try to extract JSON from text
        try:
            # Try multiple regex patterns to handle different formats
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
                        print(f"Found JSON using pattern: {pattern[:30]}...")
                        break
            
            # If no JSON was found with regex, try to clean up the response and parse it
            if not json_str:
                # Check if response has line-by-line JSON (from the logs it appears this way)
                if '"documentLevelAnalysis"' in result and '{' in result and '}' in result:
                    print("Attempting to reconstruct JSON from line-by-line format")
                    # Try to extract just the JSON portion by finding common markers
                    start_idx = result.find('{')
                    end_idx = result.rfind('}') + 1
                    
                    if start_idx >= 0 and end_idx > start_idx:
                        json_str = result[start_idx:end_idx]
                        # Clean up any trailing commas which can cause parsing errors
                        json_str = re.sub(r',\s*}', '}', json_str)
                        json_str = re.sub(r',\s*]', ']', json_str)
                        
                        print(f"Reconstructed JSON, preview: {json_str[:100]}...")
                    else:
                        logger.error("Could not locate valid JSON content by brackets")
                else:
                    logger.error("No JSON-like content detected in response")
            
            # Parse the JSON, whether found by regex or reconstructed
            if json_str:
                try:
                    analysis_data = json.loads(json_str)
                    print(f"Successfully parsed JSON from Bedrock response")
                    
                    # Normalize field names
                    analysis_data = normalizeBedrockResponse(analysis_data)
                    
                    # Validate and normalize match statuses
                    analysis_data = validateMatchStatus(analysis_data)
                    
                    return analysis_data
                except json.JSONDecodeError as je:
                    logger.error(f"JSON decode error: {str(je)}")
            
            # If we get here, parsing failed - build a fallback response with any fragments we can find
            logger.error("All JSON parsing methods failed, constructing fallback response")
            fallback_response = constructFallbackResponse(result)
            return fallback_response
                    
        except Exception as e:
            logger.error(f"Error parsing Bedrock JSON response: {str(e)}")
            logger.error(f"Response text preview: {result[:500]}..." if result else "Empty response")
            
            # Return simple Bedrock failure message
            return {
                "documentLevelAnalysis": {
                    "matchStatus": "Unknown",
                    "confidenceScore": 0,
                    "exceptionCategory": "Bedrock Failure",
                    "exceptionDescription": f"Failed to parse Bedrock response: {str(e)}",
                    "recommendedAction": "Manual review required"
                },
                "lineItemAnalysis": []
            }
        
    except Exception as e:
        logger.error(f"Error in analyzeWithBedrock: {str(e)}")
        return {
            "documentLevelAnalysis": {
                "matchStatus": "Unknown",
                "confidenceScore": 0,
                "exceptionCategory": "Bedrock Failure",
                "exceptionDescription": f"Error in analyzeWithBedrock function: {str(e)}",
                "recommendedAction": "Manual review required"
            },
            "lineItemAnalysis": []
        }

def constructFallbackResponse(result: str) -> Dict:
    """Construct fallback response when parsing fails"""
    return {
        "documentLevelAnalysis": {
            "matchStatus": "Unknown",  # Use consistent field name
            "confidenceScore": 0,
            "exceptionCategory": "Processing Error",
            "exceptionDescription": "Failed to parse Bedrock response",
            "recommendedAction": "Manual review required"
        },
        "lineItemAnalysis": []
    }

# Helper function to convert floats to Decimal for DynamoDB
def convertToDynamoDbFormat(item):
    """Convert all floats in a dict to Decimal for DynamoDB storage"""
    if isinstance(item, dict):
        return {k: convertToDynamoDbFormat(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convertToDynamoDbFormat(i) for i in item]
    elif isinstance(item, float) or isinstance(item, int):
        return Decimal(str(item))
    return item

def safeDecimalConversion(value, default='0'):
    """
    Safely convert any value to Decimal with error handling
    """
    try:
        # Handle None or empty values
        if value is None or value == '':
            return Decimal(default)
        
        # Handle nested objects or lists
        if isinstance(value, (dict, list)):
            return Decimal(default)
        
        # Convert to string and clean
        str_value = str(value).strip()
        
        # Handle empty string after stripping
        if not str_value:
            return Decimal(default)
        
        # Remove any non-numeric characters except decimal point and minus
        import re
        cleaned_value = re.sub(r'[^\d.-]', '', str_value)
        
        # If nothing left after cleaning, use default
        if not cleaned_value:
            return Decimal(default)
        
        return Decimal(cleaned_value)
        
    except (ValueError, TypeError, InvalidOperation) as e:
        logger.error(f"Error converting value '{value}' to Decimal: {str(e)}")
        return Decimal(default)

@tracer.capture_method
def storeResultsInDynamoDb(matching_results, poData_list, grnData_list, invoiceData_list, merchantId, poKey, poFilename, grnFilename, bedrock_result):
    try:
        # Normalize poData_list to ensure it's always a list of dictionaries
        if poData_list is None:
            poData_list = []
        elif isinstance(poData_list, str):
            # Try to parse string as JSON if needed
            try:
                poData_list = []  # Default to empty list if parsing fails
            except Exception as e:
                logger.error(f"Error parsing poData_list: {str(e)}")
                poData_list = []
        elif isinstance(poData_list, dict):
            # Convert single dictionary to list
            poData_list = [poData_list]
            
        # Safe lookup that handles all data types
        for result in matching_results:
            poReference = result.get('poReference') or result.get('poNumber')
            poData = {}
            
            # Safely iterate only if poData_list is actually a list
            if isinstance(poData_list, list):
                for po in poData_list:
                    if isinstance(po, dict) and poReference and str(po.get('poNumber', '')).strip() == str(poReference).strip():
                        poData = po
                        break
            
            current_timestamp = datetime.now().isoformat()
            
            matchId = str(uuid.uuid4())
            threeWayMatchingResultsId = str(uuid.uuid4())
            threeWayMatchingLineItemsId = str(uuid.uuid4())
            
            print(f"Looking up GRN data for PO reference: {poReference}")
            
            invoiceNumber = result.get('invoiceNumber')
            print(f"Looking up invoice data for number: {invoiceNumber}")
            invoiceData = next((inv for inv in invoiceData_list if any([
                str(inv.get('invoice_number', '')).strip() == str(invoiceNumber).strip(),
                str(inv.get('invoiceNumber', '')).strip() == str(invoiceNumber).strip(),
                str(inv.get('extractedDocumentsId', '')).strip() == str(invoiceNumber).strip()
            ])), {})
            
            if not invoiceData:
                print(f"No invoice data found for number {invoiceNumber}")
            else:
                print(f"Found invoice data: {json.dumps({k: v for k, v in invoiceData.items() if k != 'lineItems'}, default=str)}")
                if 'lineItems' in invoiceData:
                    print(f"Invoice has {len(invoiceData['lineItems'])} line items")
            
            grnData = next((grn for grn in grnData_list if any(
                lineItem.get('item_code') == grnLineItems.get('item_code')
                for grnLineItems in grn.get('lineItems', [])
                for lineItem in invoiceData.get('lineItems', [])
            )), {})
            if not grnData:
                print(f"No GRN data found matching item codes in invoice {invoiceNumber}")

            print("grnData: ", grnData)
            
            data_issues = []
            
            if not poData:
                data_issues.append("PO data not found")
            elif not poData.get('lineItems') or len(poData.get('lineItems', [])) == 0:
                data_issues.append("PO line items missing")
            elif poData.get('totalAmount', 0) == 0:
                data_issues.append("PO amount is zero")
                
            if not grnData:
                data_issues.append("GRN data not found")
            elif not grnData.get('lineItems') or len(grnData.get('lineItems', [])) == 0:
                data_issues.append("GRN line items missing")
            elif grnData.get('receivedQuantity', 0) == 0:
                data_issues.append("GRN quantity is zero")
                
            if not invoiceData:
                data_issues.append("Invoice data not found")
            elif not invoiceData.get('lineItems') or len(invoiceData.get('lineItems', [])) == 0:
                data_issues.append("Invoice line items missing")
            elif invoiceData.get('totalAmount', 0) == 0:
                data_issues.append("Invoice amount is zero")
            
            if data_issues:
                print(f"Data issues for match {matchId}: {', '.join(data_issues)}")
            
            doc_analysis = bedrock_result.get("documentLevelAnalysis", {})
            
            if data_issues and 'exceptionDescription' in doc_analysis:
                issues_text = f" Note: {', '.join(data_issues)}."
                if issues_text not in doc_analysis['exceptionDescription']:
                    doc_analysis['exceptionDescription'] += issues_text
                    
                if doc_analysis.get('exceptionCategory', '') == 'No Exception' or not doc_analysis.get('exceptionCategory'):
                    doc_analysis['exceptionCategory'] = 'Missing Key Fields'
                    doc_analysis['matchStatus'] = 'Mismatched'

            if 'exceptionDescription' in doc_analysis and 'totalAmount' in doc_analysis['exceptionDescription'] and 'GRN' in doc_analysis['exceptionDescription']:
                if "GRN total amount should not be considered for matching" not in doc_analysis['exceptionDescription']:
                    doc_analysis['exceptionDescription'] += " (Note: GRN total amount should not be considered for matching)"
            
            print("grnData: ", grnData)
            document_record = {
                'threeWayMatchingResultsId': threeWayMatchingResultsId,
                'matchId': matchId,
                'createdAt': current_timestamp,
                'updatedAt': current_timestamp,
                'timestampOfMatching': current_timestamp,
                
                'invoiceNumber': invoiceNumber,
                'invoiceDate': invoiceData.get('invoiceDate', ''),
                'invoiceCurrency': invoiceData.get('currency', poData.get('currency', '')),
                'totalInvoiceAmount': Decimal(str(invoiceData.get('totalAmount', 0))),
                
                'purchaseOrderNo': poData.get('poNumber', poReference if poReference else ''),
                'poDate': poData.get('poDate', ''),
                'poFileName': poFilename,
                'totalPOAmount': formatPriceAmount(poData.get('totalAmount', 0)),
                
                'grnNumber': grnData.get('grnNumber', ''),
                'grnDate': grnData.get('grnDate', ''),
                'grnFileName': grnFilename if grnFilename else grnData.get('source_file', ''),
                'totalGRNAmount': formatPriceAmount(grnData.get('totalAmount', 0)),
                
                'matchingStatus': doc_analysis.get('matchStatus', doc_analysis.get('matchingStatus', 'Unknown')),
                'confidenceScore': Decimal(str(doc_analysis.get('confidenceScore', 50))),
                'exceptionCategory': doc_analysis.get('exceptionCategory', ''),
                'exceptionDescription': doc_analysis.get('exceptionDescription', ''),
                'recommendedAction': doc_analysis.get('recommendedAction', ''),
                
                'merchantId': merchantId,
                'supplierCode': invoiceData.get('supplierCode', poData.get('supplierCode', '')),
                'supplierName': invoiceData.get('supplierName', poData.get('supplierName', '')),
                'taxCode': invoiceData.get('tax_code', invoiceData.get('taxType', poData.get('taxDetails', ''))),
                'taxAmount': Decimal(str(invoiceData.get('taxAmount', poData.get('taxAmount', 0)))),
                'remarks': data_issues[0] if data_issues else '',
                'lastModifiedBy': 'system'
            }

            
            print(f"Match record data summary:")
            critical_fields = ['invoiceNumber', 'purchaseOrderNo', 'grnNumber', 'totalInvoiceAmount', 
                               'totalPOAmount', 'totalGRNAmount', 'matchingStatus', 'exceptionCategory']
            for key in critical_fields:
                print(f"  {key}: {document_record.get(key, 'Not set')}")

            document_record = convertToDynamoDbFormat(document_record)
            print("document_record: ", document_record)
            
            print(f"Storing document record for invoice {invoiceNumber}")
            try:
                THREE_WAY_MATCHING_RESULTS_DDB_TABLE.put_item(Item=document_record)
                print(f"Successfully stored match record with ID {matchId}")
                
                if 'lineItemAnalysis' in bedrock_result and bedrock_result['lineItemAnalysis']:
                    print(f"Processing {len(bedrock_result['lineItemAnalysis'])} line item analyses")
                    
                    for idx, item_analysis in enumerate(bedrock_result['lineItemAnalysis']):
                        line_item_record = {
                            'threeWayMatchingLineItemsId': threeWayMatchingLineItemsId,
                            'matchId': matchId,
                            'lineItemNumber': idx + 1,
                            'itemCode': item_analysis.get('itemCode', ''),
                            'description': item_analysis.get('description', ''),
                            'poQuantity': safeDecimalConversion(item_analysis.get('poQuantity'), '0'),
                            'grnQuantity': safeDecimalConversion(item_analysis.get('grnQuantity'), '0'),
                            'invoiceQuantity': safeDecimalConversion(item_analysis.get('invoiceQuantity'), '0'),
                            'matchStatus': item_analysis.get('matchStatus', item_analysis.get('matchingStatus', 'Unknown')),
                            'confidenceScore': safeDecimalConversion(item_analysis.get('confidenceScore'), '50'),
                            'exceptionCategory': item_analysis.get('exceptionCategory', ''),
                            'exceptionDescription': item_analysis.get('exceptionDescription', ''),
                            'recommendedAction': item_analysis.get('recommendedAction', ''),
                            'createdAt': current_timestamp,
                            'merchantId': merchantId
                        }
                        
                        line_item_record = convertToDynamoDbFormat(line_item_record)
                        
                        try:
                            THREE_WAY_MATCHING_RESULTS_LINE_ITEM_DDB_TABLE.put_item(Item=line_item_record)
                            logger.error(f"Stored line item {idx+1} of {len(bedrock_result['lineItemAnalysis'])}")
                        except Exception as line_item_error:
                            logger.error(f"Error storing line item record: {str(line_item_error)}")
                else:
                    print("No line item analyses to store")
                
                try:
                    timeline_record = {
                        'timelineId': str(uuid.uuid4()),
                        'merchantId': merchantId,
                        'timelineForId': threeWayMatchingResultsId,
                        'title': 'processed',
                        'type': 'Three Way Matching',
                        'description': f"3-way matching performed with status: {document_record['matchingStatus']}",
                        'createdAt': current_timestamp,
                        'createdBy': "System",
                        "updatedAt": current_timestamp,
                        "updatedBy": "System",
                        'invoiceNumber': invoiceNumber,
                        'supplierName': document_record['supplierName']
                    }
                    
                    TIMELINE_DDB_TABLE.put_item(Item=timeline_record)
                    print(f"Created timeline record for match {threeWayMatchingResultsId}")
                except Exception as timeline_error:
                    logger.error(f"Error creating timeline record: {str(timeline_error)}")
                    
            except Exception as db_error:
                logger.error(f"Error storing document record: {str(db_error)}")
                logger.error(traceback.format_exc())

    except Exception as e:
        logger.error(f"Error in storeResultsInDynamoDb: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@tracer.capture_method
def updateJobTracking(jobTrackingId, completed=0, failed=0):
    """Update job tracking progress using atomic counters"""
    if not jobTrackingId:
        print("No jobTrackingId provided, skipping update")
        return
        
    try:

        timestamp = datetime.now().isoformat()
        
        # Use atomic counters to update completed/failed counts
        response = JOB_TRACKING_DDB_TABLE.update_item(
            Key={'jobTrackingId': jobTrackingId},
            UpdateExpression='ADD totalCompletedRecords :c, totalFailedRecords :f SET updatedAt = :u, updatedBy = :ub',
            ExpressionAttributeValues={
                ':c': completed,
                ':f': failed,
                ':u': timestamp,
                ':ub': 'System'
            },
            ReturnValues='UPDATED_NEW'  # Return the updated values
        )
        
        # Get the updated counts
        updated_item = response.get('Attributes', {})
        new_completed = int(updated_item.get('totalCompletedRecords', 0))
        new_failed = int(updated_item.get('totalFailedRecords', 0))
        
        # Get the total invoices
        job_response = JOB_TRACKING_DDB_TABLE.get_item(Key={'jobTrackingId': jobTrackingId})
        if 'Item' not in job_response:
            return
            
        total_invoices = int(job_response['Item'].get('totalInvoices', 0))
        
        # If all processed, update status to COMPLETED in a separate operation
        if new_completed + new_failed >= total_invoices:
            JOB_TRACKING_DDB_TABLE.update_item(
                Key={'jobTrackingId': jobTrackingId},
                UpdateExpression='SET #status = :s',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={':s': 'COMPLETED'}
            )
        
        print(f"Updated job {jobTrackingId}: completed={new_completed}, failed={new_failed}, total={total_invoices}")
        
    except Exception as e:
        logger.error(f"Error updating job tracking: {str(e)}")

@tracer.capture_method
def getMerchantConfiguration(merchantId):
    """
    Get merchant configuration once and return structured data
    """
    try:
        response = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchantId})
        merchant = response.get('Item', {})
        
        # Extract all necessary fields
        custom_logics = merchant.get('customLogics', {})
        mappingPrompts = merchant.get('mappingPrompts', {})
        
        merchant_config = {
            'merchantId': merchantId,
            'customLogics': custom_logics,
            'mappingPaths': {
                'supplierMapping': merchant.get('supplierMapping'),
                'itemMapping': merchant.get('itemMapping'),
                'storeMapping': merchant.get('storeMapping')
            },
            'promptPaths': mappingPrompts
        }
        
        return merchant_config
        
    except Exception as e:
        logger.error(f"Error fetching merchant configuration: {str(e)}")
        # Return default configuration
        return {
            'merchantId': merchantId,

            'customLogics': {
                'overrideQuantityFromUom': False,
                'useCustomerRefAsPO': False,
                'invoiceToPO': False,
                'useStoreMapping': False,
                'enableExceptionFields': False
            },
            'mappingPaths': {
                'supplierMapping': None,
                'itemMapping': None,
                'storeMapping': None
            },
            'promptPaths': {
                'vendorMappingPrompt': None,
                'itemMappingPrompt': None,
                'storeMappingPrompt': None,
                'exceptionCheckingPrompt': None,
                'threeWayMatchingPrompt': None
            }
        }

@tracer.capture_method
def fetchPythonPrompt(prompt_path, default_prompt):
    """
    Fetch custom Python prompt from S3 or return default prompt
    Enhanced with better error handling and validation
    """
    if not prompt_path:
        logger.info("No prompt path provided, using default prompt")
        return default_prompt
    
    try:   
        response = S3_CLIENT.get_object(Bucket=AGENT_MAPPING_BUCKET, Key=prompt_path)
        python_content = response['Body'].read().decode('utf-8')
        
        # Execute the Python file to get the prompt
        prompt_namespace = {}
        exec(python_content, prompt_namespace)
        
        # Look for THREE_WAY_MATCHING_PROMPT variable in the executed Python
        if 'THREE_WAY_MATCHING_PROMPT' in prompt_namespace:
            custom_prompt = prompt_namespace['THREE_WAY_MATCHING_PROMPT']
            
            logger.info(f"Using custom 3-way matching prompt from: {prompt_path}")
            return custom_prompt
        else:
            logger.warning(f"No THREE_WAY_MATCHING_PROMPT variable found in {prompt_path}. Using default prompt.")
            return default_prompt
            
    except Exception as e:
        logger.warning(f"Failed to fetch custom 3-way matching prompt from {prompt_path}: {str(e)}. Using default prompt.")
        return default_prompt

def formatPriceAmount(amount):
    """
    Format currency amount to 2 decimal places with proper rounding
    Handles floating point precision issues
    """
    if amount is None:
        return Decimal('0.00')
    
    try:
        # Convert to float first to handle any string/Decimal inputs
        float_amount = float(amount)
        # Round to 2 decimal places
        rounded_amount = round(float_amount, 2)
        # Convert to Decimal with exactly 2 decimal places
        return Decimal(f"{rounded_amount:.2f}")
    except (ValueError, TypeError, InvalidOperation):
        return Decimal('0.00')

