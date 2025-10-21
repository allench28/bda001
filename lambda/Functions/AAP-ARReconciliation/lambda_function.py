import os
import uuid
import boto3
import json
from decimal import Decimal
from datetime import datetime, timedelta
from copy import deepcopy
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.conditions import Key, Attr

SALES_STATEMENT_TABLE = os.environ.get("SALES_STATEMENT_TABLE")
BANK_STATEMENT_TABLE = os.environ.get("BANK_STATEMENT_TABLE")
PAYMENT_GATEWAY_REPORT_TABLE = os.environ.get("PAYMENT_GATEWAY_REPORT_TABLE")
PAYMENT_TRANSACTION_TABLE = os.environ.get("PAYMENT_TRANSACTION_TABLE")
STORE_TABLE = os.environ.get("STORE_TABLE")
PAYMENT_REPORT_ERP_TABLE = os.environ.get("ODOO_PAYMENT_TABLE")
RECONCILIATION_RESULTS_TABLE = os.environ.get("RECONCILIATION_RESULTS_TABLE")
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
JOB_TRACKING_TABLE = os.environ.get('JOB_TRACKING_TABLE')
TIMELINE_TABLE = os.environ.get('TIMELINE_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
LAMBDA_CLIENT = boto3.client('lambda')
S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs')

SALES_STATEMENT_DDB_TABLE = DDB_RESOURCE.Table(SALES_STATEMENT_TABLE)
BANK_STATEMENT_DDB_TABLE = DDB_RESOURCE.Table(BANK_STATEMENT_TABLE)
PAYMENT_GATEWAY_REPORT_DDB_TABLE = DDB_RESOURCE.Table(PAYMENT_GATEWAY_REPORT_TABLE)
PAYMENT_TRANSACTION_DDB_TABLE = DDB_RESOURCE.Table(PAYMENT_TRANSACTION_TABLE)
STORE_DDB_TABLE = DDB_RESOURCE.Table(STORE_TABLE)
PAYMENT_REPORT_ERP_DDB_TABLE = DDB_RESOURCE.Table(PAYMENT_REPORT_ERP_TABLE)
RECONCILIATION_RESULTS_DDB_TABLE = DDB_RESOURCE.Table(RECONCILIATION_RESULTS_TABLE)
JOB_TRACKING_DDB_TABLE = DDB_RESOURCE.Table(JOB_TRACKING_TABLE)
TIMELINE_DDB_TABLE = DDB_RESOURCE.Table(TIMELINE_TABLE)

logger = Logger()
tracer = Tracer()

# Merchant id
MERCHANT_ID = "6b8a78e2-95fe-403b-8008-e5f7c1a631fc"

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        # Handle SQS triggered processing
        if "Records" in event:
            # Process each SQS record/message
            records = event["Records"]
            for record in records:
                try:
                    # Get the message from SQS
                    message_body = record.get('body', '{}')
                    if isinstance(message_body, str):
                        message = json.loads(message_body)
                    else:
                        message = message_body
                    
                    # Extract the parameters for this chunk
                    job_id = message.get('jobId')
                    merchant_id = message.get('merchantId', MERCHANT_ID)
                    start_date = message.get('startDate')
                    end_date = message.get('endDate')
                    reconciliation_type = message.get('reconciliationType', 'salesAmount')
                    reconciliation_sub_type = message.get('reconciliationSubType')
                    payment_method = message.get('paymentMethod')
                    
                    # Process just this chunk
                    processed_records = processReconciliationChunk(
                        merchant_id, start_date, end_date, reconciliation_type, 
                        reconciliation_sub_type, payment_method
                    )
                    
                    createTimelineRecord(
                        job_id, merchant_id, 'system',
                        'Reconciliation Chunk Processing Completed',
                        f"Successfully processed {len(processed_records)} records for {payment_method} from {start_date} to {end_date}",
                        'reconciliation', reconciliation_type, reconciliation_sub_type
                    )
                    
                    # Update job progress if job_id is provided
                    if job_id:
                        updateJobProgress(job_id)
                        
                except Exception as ex:
                    logger.error(f"Error processing SQS message: {str(ex)}")
                    
                    if 'job_id' in locals():
                        createTimelineRecord(
                            job_id, merchant_id, 'system',
                            'Reconciliation Chunk Processing Failed',
                            f"Error processing reconciliation chunk: {str(ex)}",
                            'reconciliation', 'error', 'chunk_processing'
                        )
                    # Don't re-raise the exception to allow the Lambda to process other records
            
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'SQS processing complete'})
            }
            
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        # Log critical error
        createTimelineRecord(
            'unknown', MERCHANT_ID, 'system',
            'Reconciliation Critical Error',
            f"Critical error in reconciliation processing: {str(ex)}",
            'reconciliation', 'error', 'critical'
        )
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'The server encountered an unexpected condition'})
        }

@tracer.capture_method
def updateJobStatus(jobId, status, additionalData=None):
    """Update job status in the tracking table"""
    if not JOB_TRACKING_TABLE:
        return
        
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    update_expression = "set #status = :status, updatedAt = :updatedAt"
    expression_values = {
        ":status": status,
        ":updatedAt": timestamp
    }
    
    expression_names = {
        "#status": "status"
    }
    
    # Add any additional data to update
    if additionalData:
        for key, value in additionalData.items():
            update_expression += f", #{key} = :{key}"
            expression_values[f":{key}"] = value
            expression_names[f"#{key}"] = key
    
    # Update the job record
    JOB_TRACKING_DDB_TABLE.update_item(
        Key={'jobTrackingId': jobId},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
        ExpressionAttributeNames=expression_names
    )


@tracer.capture_method
def getSalesStatementData(merchantId, startDate, endDate, paymentMethod):
    """
    Get Sales Statement data from DynamoDB
    """
    # Ensure dates are in the correct format for comparison
    if 'T' not in startDate:
        startDate = f"{startDate}T00:00:00.000Z"
    if 'T' not in endDate:
        endDate = f"{endDate}T23:59:59.999Z"
    
    posRecords = []
    try:
        response = SALES_STATEMENT_DDB_TABLE.query(
            IndexName='gsi-merchantId-invoiceNumber',
            KeyConditionExpression=Key('merchantId').eq(merchantId),
            FilterExpression=Attr('orderDateTime').between(startDate, endDate),
        )
        posRecords.extend(response.get('Items', []))

        while 'LastEvaluatedKey' in response:
            response = SALES_STATEMENT_DDB_TABLE.query(
                IndexName='gsi-merchantId-invoiceNumber',
                KeyConditionExpression=Key('merchantId').eq(merchantId),
                FilterExpression=Attr('orderDateTime').between(startDate, endDate),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            posRecords.extend(response.get('Items', []))

        # Filter SalesStatement by salesStatus = "PAID", salesType = "SALES" & paymentMethod
        filtered_records = [
            record for record in posRecords 
            if (record.get('salesStatus') == 'PAID' and 
                record.get('salesType') == 'SALES' and 
                record.get('paymentMethod') == paymentMethod)
        ]
        
        return filtered_records
        
    except Exception as e:
        logger.error(f"Error querying sales statement data: {str(e)}")
        return []


@tracer.capture_method
def getPaymentTransactionData(merchantId, startDate, endDate, paymentMethod):
    """
    Get Payment Transaction data from DynamoDB
    """
    # Ensure dates are in the correct format for comparison
    # Convert dates to full datetime strings if they're just dates
    if 'T' not in startDate:
        startDate = f"{startDate}T00:00:00.000Z"
    if 'T' not in endDate:
        endDate = f"{endDate}T23:59:59.999Z"
    
    paymentTransactionData = []
    try:
        response = PAYMENT_TRANSACTION_DDB_TABLE.query(
            IndexName='gsi-merchantId-transactionId',
            KeyConditionExpression=Key('merchantId').eq(merchantId),
            FilterExpression=Attr('paymentDateTime').between(startDate, endDate),
        )
        paymentTransactionData.extend(response.get('Items', []))

        while 'LastEvaluatedKey' in response:
            response = PAYMENT_TRANSACTION_DDB_TABLE.query(
                IndexName='gsi-merchantId-transactionId',
                KeyConditionExpression=Key('merchantId').eq(merchantId),
                FilterExpression=Attr('paymentDateTime').between(startDate, endDate),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            paymentTransactionData.extend(response.get('Items', []))
            
        # Filter PaymentTransaction by gatewayTransactionType, transactionType, and status
        # For GRABFOOD payment method, include both GRABFOOD and GRABMART records
        filtered_data = []
        for record in paymentTransactionData:
            gateway_type = record.get('gatewayTransactionType', '')
            transaction_type = record.get('transactionType', '')
            status = record.get('status', '')
            
            # Consolidate GRABMART with GRABFOOD
            if paymentMethod == "GRABFOOD":
                gateway_match = gateway_type in ["GRABFOOD", "GRABMART"]
            else:
                gateway_match = gateway_type == paymentMethod
            
            if (gateway_match and 
                transaction_type == 'SALE' and 
                status == 'SUCCESS'):
                filtered_data.append(record)
        
        return filtered_data
        
    except Exception as e:
        logger.error(f"Error querying payment transaction data: {str(e)}")
        return []


@tracer.capture_method
def getPaymentGatewayData(merchantId, settlementId):
    """
    Get Payment Gateway data from DynamoDB
    """
    response = PAYMENT_GATEWAY_REPORT_DDB_TABLE.query(
        IndexName='gsi-merchantId-settlementId',
        KeyConditionExpression=Key('merchantId').eq(merchantId) & Key('settlementId').eq(settlementId),
    ).get('Items', [])
    if response:
        return response[0]
    else:
        return None


@tracer.capture_method
def getBankStatementData(merchantId, vendorRef):
    """
    Get Bank Statement data from DynamoDB
    """
    response = BANK_STATEMENT_DDB_TABLE.query(
        IndexName='gsi-merchantId-bankRef',
        KeyConditionExpression=Key('merchantId').eq(merchantId),
        FilterExpression=Attr('vendorRef').eq(vendorRef),
    ).get('Items', [])
    if response:
        return response[0]
    else:
        return None


@tracer.capture_method
def getPaymentReportErpData(merchantId, branchCode, paymentTransactionDate):
    """
    Get Payment Report ERP data from DynamoDB
    """
    paymentErpRecords = []
    filteredPaymentErpRecords = []

    response = PAYMENT_REPORT_ERP_DDB_TABLE.query(
        IndexName='gsi-merchantId',
        KeyConditionExpression=Key('merchantId').eq(merchantId),
        FilterExpression=Attr('branchCode').eq(branchCode),
    )
    paymentErpRecords.extend(response.get('Items', []))

    while 'LastEvaluatedKey' in response:
        response = PAYMENT_REPORT_ERP_DDB_TABLE.query(
            IndexName='gsi-merchantId',
            KeyConditionExpression=Key('merchantId').eq(merchantId),
            FilterExpression=Attr('branchCode').eq(branchCode),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        paymentErpRecords.extend(response.get('Items', []))
    
    for record in paymentErpRecords:
        transactionDateTime = record.get('transactionDateTime', '')
        transactionDate = transactionDateTime.split('T')[0]

        reportDateTime = record.get('reportDateTime', '')
        reportDate = reportDateTime.split('T')[0]

        if transactionDate == paymentTransactionDate and record.get('amountType') == 'CREDIT':
            filteredPaymentErpRecords.append(record)
        if reportDate == paymentTransactionDate and record.get('amountType') == 'DEBIT':
            filteredPaymentErpRecords.append(record)

    return filteredPaymentErpRecords


@tracer.capture_method
def matchSalesStatementWithPaymentTransaction(salesStatementData, paymentTransactionData):
    """
    Match Sales Statement with Payment Transaction
    """
    matchedRecords = []
    unmatchedRecords = []
    salesStatementMap = {}
    for salesData in salesStatementData:
        branchCode = salesData.get('branchCode', '')
        branchName = salesData.get('branchName', '')
        orderDateTime = salesData.get('orderDateTime', '')
        orderDate = orderDateTime.split('T')[0]
        totalPayableAmount = float(salesData.get('totalPayableAmount', 0))

        if branchCode not in salesStatementMap:
            salesStatementMap[branchCode] = {}
        if orderDate not in salesStatementMap[branchCode]:
            salesStatementMap[branchCode][orderDate] = {
                'totalPayableAmount': float(0),
                'branchName': branchName,
                'salesRecords': []
            }
        salesStatementMap[branchCode][orderDate]['totalPayableAmount'] += totalPayableAmount
        salesStatementMap[branchCode][orderDate]['salesRecords'].append(salesData)

    # Get Payment Transaction data
    for branchKey, salesData in salesStatementMap.items():
        for orderDate, salesRecords in salesData.items():
            totalSalesNetAmount = round(float(salesRecords.get('totalPayableAmount')), 2)
            paymentTransactionRecords = [payment for payment in paymentTransactionData if payment.get('branchCode') == branchKey and payment.get('paymentDateTime').split('T')[0] == orderDate]
            if not paymentTransactionRecords:
                unmatchedRecords.append({
                    'transactionDate': orderDate,
                    'settlementId': None,
                    'branchCode': branchKey,
                    'branchName': salesRecords.get('branchName', ''),
                    'totalSalesNetAmount': totalSalesNetAmount,
                    'varianceAmount': totalSalesNetAmount,  # Full amount as variance since no matching payment
                    'mismatchDetails': {
                        'error': f'No matching Payment Transaction record for {branchKey} on {orderDate}',
                        'expected': f'Payment Transaction record for {branchKey} on {orderDate}',
                        'found': None,
                    },
                    'posData': salesRecords.get('salesRecords', []),
                    'paymentRecord': None,
                    'gatewayRecord': None
                })
                continue

            totalTransactionSalesNetAmount = round(sum([float(payment.get('salesNetAmount', 0)) for payment in paymentTransactionRecords]), 2)
            varianceAmount = round(totalSalesNetAmount - totalTransactionSalesNetAmount, 2)

            # Compare Sales Statement with Payment Transaction
            if totalSalesNetAmount == totalTransactionSalesNetAmount:
                # Matched
                matchedRecords.append({
                    'transactionDate': orderDate,
                    'settlementId': None,
                    'branchCode': branchKey,
                    'branchName': salesRecords.get('branchName', ''),
                    'totalSalesNetAmount': totalSalesNetAmount,
                    'varianceAmount': varianceAmount,
                    'matchDetails': {
                        'transactionDate': orderDate,
                        'settlementId': None,
                        'branchCode': branchKey,
                        'paymentTransactionMatched': True,
                        'paymentGatewayMatched': False,
                    },
                    'posData': salesRecords.get('salesRecords', []),
                    'paymentRecord': paymentTransactionRecords,
                    'gatewayRecord': None
                })
            else:
                # Not Matched
                unmatchedRecords.append({
                    'transactionDate': orderDate,
                    'settlementId': None,
                    'branchCode': branchKey,
                    'branchName': salesRecords.get('branchName', ''),
                    'totalSalesNetAmount': totalSalesNetAmount,
                    'varianceAmount': varianceAmount,
                    'mismatchDetails': {
                        'error': f'Mismatch between Sales Statement total amount and Payment Transaction total amount. Expected: {totalSalesNetAmount}, Found: {totalTransactionSalesNetAmount}',
                        'expected': str(salesRecords['totalPayableAmount']),
                        'found': str(totalTransactionSalesNetAmount),
                    },
                    'posData': salesRecords.get('salesRecords', []),
                    'paymentRecord': paymentTransactionRecords,
                    'gatewayRecord': None
                })
    return matchedRecords, unmatchedRecords
    

@tracer.capture_method
def matchPaymentTransactionWithPaymentGateway(paymentTransactionData):
    """
    Match Payment Transaction with Payment Gateway
    """
    matchedRecords = []
    unmatchedRecords = []
    paymentTransactionMap = {}
    for paymentData in paymentTransactionData:
        settlementId = paymentData.get('settlementId', '')
        paymentDateTime = paymentData.get('paymentDateTime', '')
        paymentDate = paymentDateTime.split('T')[0]
        salesNetAmount = float(paymentData.get('salesNetAmount', 0))
        creditAmount = float(paymentData.get('creditAmount', 0))

        if settlementId not in paymentTransactionMap:
            paymentTransactionMap[settlementId] = {}
        if paymentDate not in paymentTransactionMap[settlementId]:
            paymentTransactionMap[settlementId][paymentDate] = {
                'totalSalesNetAmount': float(0),
                'totalCreditAmount': float(0),
                'settlementId': paymentData.get('settlementId', ''),
                'paymentRecords': []
            }
        paymentTransactionMap[settlementId][paymentDate]['totalSalesNetAmount'] += salesNetAmount
        paymentTransactionMap[settlementId][paymentDate]['totalCreditAmount'] += creditAmount
        paymentTransactionMap[settlementId][paymentDate]['paymentRecords'].append(paymentData)

    # Get Payment Gateway data
    for settlementKey, transactionData in paymentTransactionMap.items():
        for paymentDate, transactionRecord in transactionData.items():
            settlementId = transactionRecord['settlementId']
            paymentGatewayData = getPaymentGatewayData(MERCHANT_ID, settlementId)
            branchCode = transactionRecord['paymentRecords'][0].get('branchCode', '')
            if not paymentGatewayData:
                unmatchedRecords.append({
                    'transactionDate': paymentDate,
                    'settlementDate': None,
                    'settlementId': settlementId,
                    'branchCode': branchCode,
                    'mismatchDetails': {
                        'error': f'No matching Transfer record for {branchCode} on {paymentDate} for Settlement ID {settlementId}',
                        'expected': f'Transfer record for {settlementId} on {paymentDate}',
                        'found': None,
                    },
                    'paymentRecord': transactionRecord,
                    'gatewayRecord': None
                })
                continue

            settlementDateTime = paymentGatewayData.get('settlementDateTime', '')
            settlementDate = settlementDateTime.split('T')[0]
            totalTransactionCreditAmount = round(float(transactionRecord.get('totalCreditAmount')), 2)
            totalGatewayCreditAmount = round(float(paymentGatewayData.get('totalCreditAmount')), 2)
            # Compare Payment Transaction with Payment Gateway
            if totalTransactionCreditAmount == totalGatewayCreditAmount:
                # Matched
                matchedRecords.append({
                    'transactionDate': paymentDate,
                    'settlementDate': settlementDate,
                    'settlementId': settlementId,
                    'branchCode': branchCode,
                    'matchDetails': {
                        'transactionDate': paymentDate,
                        'settlementDate': settlementDate,
                        'settlementId': settlementId,
                        'branchCode': branchCode,
                        'paymentTransactionMatched': True,
                        'paymentGatewayMatched': True,
                    },
                    'paymentRecord': transactionRecord,
                    'gatewayRecord': paymentGatewayData
                })
            else:
                # Not Matched
                unmatchedRecords.append({
                    'transactionDate': paymentDate,
                    'settlementDate': settlementDate,
                    'branchCode': branchCode,
                    'mismatchDetails': {
                        'error': f'Mismatch betwee Transfer record Net Total amount and Transaction record Total amount. Expected: {totalTransactionCreditAmount}, Found: {totalGatewayCreditAmount}',
                        'expected': str(transactionRecord['totalCreditAmount']),
                        'found': str(paymentGatewayData['totalCreditAmount']),
                    },
                    'paymentRecord': transactionRecord,
                    'gatewayRecord': None
                })

    return matchedRecords, unmatchedRecords


@tracer.capture_method
def matchPaymentGatewayWithBank(matchedTransferRecords):
    """
    Match Payment Gateway with Bank Statement
    """
    initialMatchedRecords = deepcopy(matchedTransferRecords)
    matchedRecords = []
    unmatchedRecords = []
    for record in initialMatchedRecords:
        paymentGatewayData = record.get('gatewayRecord', {})
        vendorRef = paymentGatewayData.get('vendorRef', '')
        branchCode = record.get('branchCode', '')
        transactionDate = record.get('transactionDate', '')
        bankRecord = getBankStatementData(MERCHANT_ID, vendorRef)
        if not bankRecord:
            record['matchDetails']['bankMatched'] = False
            record['mismatchDetails'] = {
                'error': f'No matching CIMB-i bank statement record for {branchCode} on {transactionDate}',
                'expected': f'Bank statement for {vendorRef}',
                'found': None,
            }
            unmatchedRecords.append(record)
            continue

        totalCreditAmount = round(float(paymentGatewayData.get('totalCreditAmount', 0)), 2)
        transactionAmount = round(float(bankRecord.get('transactionAmount', 0)), 2)
        if totalCreditAmount == transactionAmount:
            record['matchDetails']['bankMatched'] = True
            record['bankStatementData'] = bankRecord
            matchedRecords.append(record)
        else:
            record['matchDetails']['bankMatched'] = False
            record['mismatchDetails'] = {
                'error': f'Mismatch in total amounts between Transfer record and CIMB-i bank statement record. Expected: {totalCreditAmount}, Found: {transactionAmount}',
                'expected': str(totalCreditAmount),
                'found': str(transactionAmount),
            }
            unmatchedRecords.append(record)
    
    return matchedRecords, unmatchedRecords


@tracer.capture_method
def erpConditionalMapping(record, paymentMethod):
    """
    Conditional mapping for ERP records based on payment method
    """
    if paymentMethod == 'GRABPAY':
        return True
    elif paymentMethod == 'GRABFOOD' or paymentMethod == 'GRABMART':
        label = record.get('label', '')

        if not label:
            return False
        
        return label.startswith('GRABF')
    
    return False


@tracer.capture_method
def matchSalesWithErp(matchedTransactionRecords, paymentMethod='GRABPAY'):
    """
    Match Payment ERP with Sales Records & Payment Transaction
    """
    initialMatchedRecords = deepcopy(matchedTransactionRecords)
    matchedRecords = []
    unmatchedRecords = []
    for records in initialMatchedRecords:
        transactionDate = records.get('transactionDate', '')
        branchCode = records.get('branchCode', '')
        branchName = records.get('branchName', '')
        paymentTransactionRecords = records.get('paymentRecord', [])

        filteredPaymentErpRecords = getPaymentReportErpData(MERCHANT_ID, branchCode, transactionDate)

        debitRecord = [record for record in filteredPaymentErpRecords if record.get('amountType') == 'DEBIT' and erpConditionalMapping(record, paymentMethod) and record.get('type') == paymentMethod]
        creditRecord = [record for record in filteredPaymentErpRecords if record.get('amountType') == 'CREDIT' and record.get('type') == paymentMethod]

        if not records.get('matchDetails'):
            records['matchDetails'] = {
                'transactionDate': transactionDate,
                'settlementId': None,
                'branchCode': branchCode,
                'branchName': branchName,
                'paymentTransactionMatched': False,
            }

        totalSalesNetAmount = round(float(records.get('totalSalesNetAmount', 0)), 2)
        totalPaymentTransactionSalesAmount = 0
        if paymentTransactionRecords:
            totalPaymentTransactionSalesAmount = sum([round(float(payment.get('salesNetAmount', 0)), 2) for payment in paymentTransactionRecords])

        if not debitRecord or not creditRecord:
            recordType = 'DEBIT' if not debitRecord else 'CREDIT'
            # Calculate variance based on missing record type
            if not debitRecord:
                records['varianceAmount'] = totalSalesNetAmount
            else:
                records['varianceAmount'] = totalPaymentTransactionSalesAmount
                
            records['matchDetails']['erpMatched'] = False
            records['mismatchDetails'] = {
                'error': f'No matching {recordType} ERP record for {branchCode} on {transactionDate}',
                'expected': f'ERP record for {branchCode} on {transactionDate}',
                'found': None,
            }
            records['erpData'] = []
            if debitRecord:
                records['erpData'].extend(debitRecord)
            if creditRecord:
                records['erpData'].extend(creditRecord)
            unmatchedRecords.append(records)
            continue

        totalDebitAmount = sum([round(float(debit.get('amount', 0)), 2) for debit in debitRecord])
        totalCreditAmount = sum([round(float(credit.get('amount', 0)), 2) for credit in creditRecord])

        # Calculate variance - prioritize the larger variance
        salesVariance = round(totalSalesNetAmount - totalDebitAmount, 2)
        paymentVariance = round(totalPaymentTransactionSalesAmount - totalCreditAmount, 2)
        varianceAmount = salesVariance if abs(salesVariance) >= abs(paymentVariance) else paymentVariance
        records['varianceAmount'] = varianceAmount

        if round(totalSalesNetAmount,2) == round(totalDebitAmount,2) and round(totalPaymentTransactionSalesAmount,2) == round(totalCreditAmount,2):
            records['matchDetails']['erpMatched'] = True
            records['erpData'] = []
            records['erpData'].extend(debitRecord)
            records['erpData'].extend(creditRecord)
            matchedRecords.append(records)
        else:
            if totalSalesNetAmount != totalDebitAmount:
                errorMessage = f'Mismatch in total debit amounts between Sales record and ERP record. Expected: {totalSalesNetAmount}, Found: {totalDebitAmount}'
                expected = str(totalSalesNetAmount)
                found = str(totalDebitAmount)
            else:
                errorMessage = f'Mismatch in total credit amounts between Payment Transaction record and ERP record. Expected: {totalPaymentTransactionSalesAmount}, Found: {totalCreditAmount}'
                expected = str(totalPaymentTransactionSalesAmount)
                found = str(totalCreditAmount)

            records['matchDetails']['erpMatched'] = False
            records['erpData'] = []
            records['erpData'].extend(debitRecord)
            records['erpData'].extend(creditRecord)
            records['mismatchDetails'] = {
                'error': errorMessage,
                'expected': expected,
                'found': found,
            }
            unmatchedRecords.append(records)
            continue
    
    return matchedRecords, unmatchedRecords


@tracer.capture_method
def processReconciliationChunk(merchant_id, start_date, end_date, reconciliation_type, reconciliation_sub_type, payment_method):
    """Process a single reconciliation chunk based on reconciliation type and sub-type"""
    matched_records = []
    unmatched_records = []

    # Get payment transaction data for all sub-types
    payment_transaction_data = getPaymentTransactionData(
        merchant_id, start_date, end_date, payment_method
    )
    
    if reconciliation_type == "salesAmount":
        # Get sales statement data
        sales_statement_data = getSalesStatementData(
            merchant_id, start_date, end_date, payment_method
        )
        
        if not sales_statement_data:
            return []
        
        # Match sales statements with payment transactions
        matched_tx_records, unmatched_tx_records = matchSalesStatementWithPaymentTransaction(
            sales_statement_data, payment_transaction_data
        )
        
        # Handle different reconciliation sub-types
        if reconciliation_sub_type == "foodMarketplace":
            # Food marketplace: Match with ERP
            # Process unmatched records
            matched_sales_erp, unmatched_sales_erp = matchSalesWithErp(unmatched_tx_records, payment_method)
            unmatched_records.extend(unmatched_sales_erp)
            
            # Process matched records
            matched_erp, unmatched_erp = matchSalesWithErp(matched_tx_records, payment_method)
            unmatched_records.extend(unmatched_erp)
            matched_records.extend(matched_erp)
            matched_records.extend(matched_sales_erp)
            
        elif reconciliation_sub_type == "creditCard":
            # Credit card: Only match sales with payment transaction
            matched_records.extend(matched_tx_records)
            unmatched_records.extend(unmatched_tx_records)
            
        else:
            return []
        
        # Send results to SQS
        for record in matched_records + unmatched_records:
            payload = createReconciliationResults(record, reconciliation_type, reconciliation_sub_type, payment_method)
            sendToSQS(payload)

    elif reconciliation_type == "settlementAmount":
        # Future implementation for settlement amount reconciliation
        return []
    
    return matched_records + unmatched_records

@tracer.capture_method
def updateJobProgress(job_id):
    """Update job progress based on completed chunks"""
    try:
        # Get current job status
        response = JOB_TRACKING_DDB_TABLE.get_item(Key={'jobTrackingId': job_id})
        if 'Item' not in response:
            return
            
        job = response['Item']
        completed_chunks = job.get('completedChunks', 0) + 1
        total_chunks = job.get('totalChunks', 0)
        
        # Update job status
        status = 'IN_PROGRESS'
        if completed_chunks >= total_chunks:
            status = 'COMPLETED'
            
        updateJobStatus(job_id, status, {
            'completedChunks': completed_chunks,
            'progress': f"{(completed_chunks / total_chunks) * 100:.1f}%" if total_chunks > 0 else "0%"
        })
        
    except Exception as ex:
        logger.error(f"Error updating job progress: {str(ex)}")


@tracer.capture_method
def sendToSQS(payload):
    # Check for duplicates based on unique key
    unique_key = payload.get('reconciliationData', {}).get('uniqueKey')
    if unique_key:
        try:
            # Check if this reconciliation result already exists
            response = RECONCILIATION_RESULTS_DDB_TABLE.query(
                IndexName='gsi-uniqueKey', 
                KeyConditionExpression=Key('uniqueKey').eq(unique_key),
                Limit=1
            )
            if response.get('Items'):
                return None
        except Exception as e:
            pass
    
    payloadJson = json.dumps(payload, default=decimalDefault)
    response = SQS_CLIENT.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=payloadJson
    )
    return response


@tracer.capture_method
def decimalDefault(obj):
    """Helper function for JSON serialization of Decimal types"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


@tracer.capture_method
def roundDecimal(value):
    """Helper function to round Decimal values to 2 decimal places"""
    if isinstance(value, float):
        return round(float(value), 2)
    return value


@tracer.capture_method
def createReconciliationResults(record, reconciliationType, reconciliationSubType, paymentMethod):
    """
    Create reconciliation results payload with sub-type information
    """
   
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    reconciliationResultId = str(uuid.uuid4())
    posData = record.get('posData', [])
    paymentRecords = record.get('paymentRecord', [])
    paymentGatewayData = record.get('gatewayRecord', {})
    bankStatementData = record.get('bankStatementData', {})
    erpData = record.get('erpData', [])

    # Include sub-type in unique key
    unique_key = f"{record.get('branchCode', '')}_{record.get('transactionDate', '')}_{paymentMethod}_{reconciliationType}_{reconciliationSubType}"

    totalSalesAmount = 0
    totalTaxAmount = 0
    totalGatewaySalesAmount = 0
    totalGatewayTransactionAmount = 0
    totalGatewayCreditAmount = 0
    totalGatewayProcessingFee = 0
    erpCreditAmount = 0
    erpDebitAmount = 0
    
    if posData:
        totalSalesAmount = sum([float(sales.get('totalPayableAmount', 0)) for sales in posData])
        totalTaxAmount = sum([float(sales.get('totalTaxAmount', 0)) for sales in posData])
    
    if paymentRecords:
        if reconciliationType == "salesAmount":
            totalGatewaySalesAmount = sum([float(payment.get('salesNetAmount', 0)) for payment in paymentRecords])
        totalGatewayTransactionAmount = sum([float(payment.get('creditAmount', 0)) for payment in paymentRecords])
        totalGatewayProcessingFee = sum([float(payment.get('processingFee', 0)) for payment in paymentRecords])
    
    # Only calculate ERP amounts for foodMarketplace sub-type
    if erpData and reconciliationSubType == "foodMarketplace":
        erpCreditAmount = sum([float(erp.get('amount', 0)) for erp in erpData if erp.get('amountType') == 'CREDIT'])
        erpDebitAmount = sum([float(erp.get('amount', 0)) for erp in erpData if erp.get('amountType') == 'DEBIT'])

    if paymentGatewayData:
        totalGatewayCreditAmount = paymentGatewayData.get('totalCreditAmount', 0)
    
    if not record.get('mismatchDetails'):
        status = "Matched"
        exceptionDescription = None
    else:
        status = "Mismatched"
        exceptionDescription = record.get('mismatchDetails', {}).get('error', None)

    varianceAmount = roundDecimal(record.get('varianceAmount', 0))

    # Set sales merchant based on sub-type
    sales_merchant = "Grab" if reconciliationSubType == "foodMarketplace" else "CIMB"

    reconciliationResultPayload = {
        'reconciliationResultsId': reconciliationResultId,
        'uniqueKey': unique_key,
        'merchantId': MERCHANT_ID,
        'currency': "MYR",
        'branchCode': record.get('branchCode', ''),
        'branchName': record.get('branchName', ''),
        'salesMerchant': sales_merchant,
        'salesChannel': paymentMethod,
        'reconciliationSubType': reconciliationSubType,  # Add sub-type to results
        'transactionDate': record.get('transactionDate', ''),
        'gatewayTransactionDate': record.get('transactionDate', ''),
        'gatewaySettlementDateTime': record.get('settlementDate', ''),
        'settlementId': record.get('settlementId', ''),
        'totalSalesAmount': roundDecimal(totalSalesAmount),
        'totalTaxAmount': roundDecimal(totalTaxAmount),
        'totalGatewaySalesAmount': roundDecimal(totalGatewaySalesAmount),
        'totalGatewayTransactionAmount': roundDecimal(totalGatewayTransactionAmount),
        'totalGatewayCreditAmount': roundDecimal(totalGatewayCreditAmount),
        'totalGatewayProcessingFee': roundDecimal(totalGatewayProcessingFee),
        'erpCreditAmount': roundDecimal(erpCreditAmount),
        'erpDebitAmount': roundDecimal(erpDebitAmount),
        'matchingStatus': status,
        'varianceAmount': varianceAmount,
        'exceptionCategory': None,
        'exceptionDescription': exceptionDescription,
        'recommendedAction': None,
        'reconciliationType': reconciliationType,
        'remarks': None,
        'confidenceScore': 0,
        'createdAt': now,
        'createdBy': "System",
        'updatedAt': now,
        'updatedBy': "System"
    }

    if reconciliationType in ["settlementAmount"]:
        reconciliationResultPayload['branchUUID'] = paymentGatewayData.get('branchUUID', '')
        reconciliationResultPayload['bankRef'] = bankStatementData.get('bankRef', '')
        reconciliationResultPayload['vendorRef'] = bankStatementData.get('vendorRef', '')
        reconciliationResultPayload['bankTransactionDate'] = bankStatementData.get('transactionDateTime', '')
        reconciliationResultPayload['bankCreditAmount'] = roundDecimal(bankStatementData.get('transactionAmount', 0)),
        reconciliationResultPayload['bankName'] = bankStatementData.get('bankName', '')

    sqsPayload = {
        'merchantId': MERCHANT_ID,
        'reconciliationType': reconciliationType,
        'reconciliationSubType': reconciliationSubType,  # Include sub-type in SQS payload
        'reconciliationData': reconciliationResultPayload,
    }

    return sqsPayload

@tracer.capture_method
def createTimelineRecord(job_id, merchant_id, user, title, description, record_type, reconciliation_type, reconciliation_sub_type, additional_data=None):
    """Create a timeline record for audit trail"""
    if not TIMELINE_DDB_TABLE:
        return
        
    timeline_id = str(uuid.uuid4())
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    timeline_item = {
        'timelineId': timeline_id,
        'timelineForId': job_id,
        'merchantId': merchant_id,
        'createdAt': now,
        'createdBy': user,
        'updatedAt': now,
        'updatedBy': user,
        'type': record_type,
        'title': title,
        'description': description,
        'reconciliationType': reconciliation_type,
        'reconciliationSubType': reconciliation_sub_type,
        'module': 'reconciliation'
    }
    
    # Add any additional data
    if additional_data:
        timeline_item.update(additional_data)
    
    try:
        TIMELINE_DDB_TABLE.put_item(Item=timeline_item)
    except Exception as e:
        logger.error(f"Failed to create timeline record: {str(e)}")