from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, udf, col, when, concat_ws, date_format, to_timestamp, regexp_replace, regexp_extract, trim
from pyspark.sql.types import StringType, DecimalType


import boto3
import sys
from datetime import datetime
from decimal import Decimal
import json
import uuid

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "documentUploadIds",
    "s3Paths", 
    "documentTypes",
    "fileNames",
    "merchantId",
    "BUCKET_NAME",
    "DOCUMENT_UPLOAD_TABLE",
    "SQS_QUEUE_URL",
    "DDB_TABLE_NAME"
])

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# AWS clients
DDB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3')
DOCUMENT_UPLOAD_DDB_TABLE = DDB_RESOURCE.Table(args['DOCUMENT_UPLOAD_TABLE'])


SALES_FIELD_MAPPING = {
    'Outlet': 'branchName',
    'Outlet Code': 'branchCode',
    'Order Date': 'orderDateTime',
    'System Order ID': 'systemOrderId',
    'Invoice Number': 'invoiceNumber',
    'Is Cancel Receipt': 'salesStatus',
    'Non-Sale': 'salesType',
    'Payment Amount': 'totalPayableAmount',
    'Net Sales After Payment Rounding': 'totalSalesAmount',
    'Tax After Discount': 'totalTaxAmount',
    "Debit/Credit Card - RHB Terminal": "RHB_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - CIMB Terminal": "CREDIT_CARD",
    "Debit/Credit Card - Bank Rakyat Terminal": "BANK_RAKYAT_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - Ambank Terminal": "AMBANK_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - Affin Terminal": "AFFIN_DEBIT_CREDIT_CARD",
    "CIMB Bonus Point": "CIMB_BONUS_POINT",
    "DuitNow QR": "QR",
    "Ambank QR": "AMBANK_QR",
    "ShopeeFood": "SHOPEE_FOOD",
    "ShopBack": "SHOPEE_BACK",
    "Retail Cash": "CASH",
    "GrabPay": "GRABPAY",
    "Shopee Pay": "SHOPEE_PAY",
    "Touch N Go": "TNG",
    "Affin QR Code": "AFFIN_QR_CODE",
    "GrabFood / GrabCake - Campaign Testing": "GRABFOOD_GRABCAKE_CAMPAIGN_TESTING",
    "GrabFood / GrabCake": "GRABFOOD",
    "FoodPanda": "FOODPANDA",
    "DeliverEat": "DELIVEREAT",
    "WhatsApp": "WHATSAPP",
    "Air-Asia Food": "AIRASIA_FOOD",
    "Mall Voucher": "MALL_VOUCHER",
    "BR Voucher": "BR_VOUCHER",
    "Payment To HQ": "PAYMENT_TO_HQ",
    "Sunway Pals": "SUNWAY_PALS",
    "Setel Deliver2Me": "SETEL_DELIVER2ME",
    "OneShop": "ONESHOP",
    "Redemption OneCard": "REDEMPTION_ONECARD",
    "1Pay E-Wallet": "1PAY_EWALLET",
    "KM App": "KM_APP",
    "Redemption Genting": "REDEMPTION_GENTING",
    "Redemption Sunway": "REDEMPTION_SUNWAY",
    "Mobile App": "MOBILE_APP",
    "Kiosk": "KIOSK",
    "WebComm": "WEBCOMM",
    "IPay88": "IPAY88",
    "Voucher Redemption (Product)": "VOUCHER_REDEMPTION_PRODUCT",
    "Voucher Redemption (Cash)": "VOUCHER_REDEMPTION_CASH",
    "Alipay": "ALIPAY",
    "Debit/Credit Card - MBB Terminal": "DEBIT_CREDIT_CARD_MBB",
    "PrimePay": "PRIMEPAY",
    "Maybank Treatspoints": "MAYBANK_TREAT_POINTS",
    "HSBC Birthday - Free Junior Scoop": "HSBC_BIRTHDAY_FREE_JUNIOR_SCOOP",
    "Lazada E-Wallet": "LAZADA_EWALLET",
    "Mesra Card": "MESRA_CARD"
}

PAYMENT_METHOD_MAPPING = {
    "Debit/Credit Card - RHB Terminal": "RHB_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - CIMB Terminal": "CREDIT_CARD",
    "Debit/Credit Card - Bank Rakyat Terminal": "BANK_RAKYAT_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - Ambank Terminal": "AMBANK_DEBIT_CREDIT_CARD",
    "Debit/Credit Card - Affin Terminal": "AFFIN_DEBIT_CREDIT_CARD",
    "CIMB Bonus Point": "CIMB_BONUS_POINT",
    "DuitNow QR": "QR",
    "Ambank QR": "AMBANK_QR",
    "ShopeeFood": "SHOPEE_FOOD",
    "ShopBack": "SHOPEE_BACK",
    "Retail Cash": "CASH",
    "GrabPay": "GRABPAY",
    "Shopee Pay": "SHOPEE_PAY",
    "Touch N Go": "TNG",
    "Affin QR Code": "AFFIN_QR_CODE",
    "GrabFood / GrabCake - Campaign Testing": "GRABFOOD_GRABCAKE_CAMPAIGN_TESTING",
    "GrabFood / GrabCake": "GRABFOOD",
    "FoodPanda": "FOODPANDA",
    "DeliverEat": "DELIVEREAT",
    "WhatsApp": "WHATSAPP",
    "Air-Asia Food": "AIRASIA_FOOD",
    "Mall Voucher": "MALL_VOUCHER",
    "BR Voucher": "BR_VOUCHER",
    "Payment To HQ": "PAYMENT_TO_HQ",
    "Sunway Pals": "SUNWAY_PALS",
    "Setel Deliver2Me": "SETEL_DELIVER2ME",
    "OneShop": "ONESHOP",
    "Redemption OneCard": "REDEMPTION_ONECARD",
    "1Pay E-Wallet": "1PAY_EWALLET",
    "KM App": "KM_APP",
    "Redemption Genting": "REDEMPTION_GENTING",
    "Redemption Sunway": "REDEMPTION_SUNWAY",
    "Mobile App": "MOBILE_APP",
    "Kiosk": "KIOSK",
    "WebComm": "WEBCOMM",
    "IPay88": "IPAY88",
    "Voucher Redemption (Product)": "VOUCHER_REDEMPTION_PRODUCT",
    "Voucher Redemption (Cash)": "VOUCHER_REDEMPTION_CASH",
    "Alipay": "ALIPAY",
    "Debit/Credit Card - MBB Terminal": "DEBIT_CREDIT_CARD_MBB",
    "PrimePay": "PRIMEPAY",
    "Maybank Treatspoints": "MAYBANK_TREAT_POINTS",
    "HSBC Birthday - Free Junior Scoop": "HSBC_BIRTHDAY_FREE_JUNIOR_SCOOP",
    "Lazada E-Wallet": "LAZADA_EWALLET",
    "Mesra Card": "MESRA_CARD",
}

# Fields that need to be converted to integers or floats
SALES_FLOAT_FIELDS = [
    'Tax After Discount',
    'Debit',
    'Credit',
    'Net Sales After Payment Rounding',
    'Tax After Discount'
]

def updateDocumentUploadStatus(documentUploadId, status, **kwargs):
    """Update status in DynamoDB"""
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    update_expression = "SET #status = :status, #updatedAt = :updatedAt"
    expression_attribute_names = {"#status": "status", "#updatedAt": "updatedAt"}
    expression_attribute_values = {":status": status, ":updatedAt": now}
    
    for key, value in kwargs.items():
        if value is not None:
            update_expression += f", #{key} = :{key}"
            expression_attribute_names[f"#{key}"] = key
            expression_attribute_values[f":{key}"] = value
    
    DOCUMENT_UPLOAD_DDB_TABLE.update_item(
        Key={'documentUploadId': documentUploadId},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )

def processSalesData(df, documentUploadId):
    """Transform sales data to match the expected DynamoDB schema, but do not insert to DDB."""
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    merchantId = args['merchantId']

    # Generate UUIDs for each row
    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

    # Add/transform columns as per createPosRecord logic
    df_transformed = (
        df
        .withColumn("salesStatementId", uuid_udf())
        .withColumn("merchantId", lit(merchantId))
        .withColumn("currency", lit("MYR"))
        .withColumn("taxCode", lit("GST"))
        .withColumn("documentUploadId", lit(documentUploadId))
        .withColumn("filePath", lit(args['s3Paths'].split(",")[0]))
        .withColumn("sourceFile", lit(args['fileNames'].split(",")[0]))
        .withColumn("createdAt", lit(now))
        .withColumn("createdBy", lit("System"))
        .withColumn("updatedAt", lit(now))
        .withColumn("updatedBy", lit("System"))
    )

    # If you want to ensure no duplicate invoiceNumbers, you can drop duplicates here:
    if "invoiceNumber" in df_transformed.columns:
        df_transformed = df_transformed.dropDuplicates(["invoiceNumber"])

    return df_transformed

def mapSalesDataFrame(df):
    """
    Map and clean sales DataFrame to match the expected schema, similar to readPosCsvFromS3 in Lambda.
    Only mapped and required columns will be included in the result.
    """
    try:
        mapped_df = df

        # 1. Convert float fields (using original names)
        for field in SALES_FLOAT_FIELDS:
            if field in mapped_df.columns:
                mapped_df = mapped_df.withColumn(
                    field,
                    when(col(field).isNotNull(), col(field).cast("double")).otherwise(0.0)
                )

        # 2. Rename columns first
        for src, dest in SALES_FIELD_MAPPING.items():
            if src in mapped_df.columns:
                mapped_df = mapped_df.withColumnRenamed(src, dest)

        # 3. Initialize paymentMethod and totalPayableAmount columns if not present
        if "paymentMethod" not in mapped_df.columns:
            mapped_df = mapped_df.withColumn("paymentMethod", lit(None).cast(StringType()))
        if "totalPayableAmount" not in mapped_df.columns:
            mapped_df = mapped_df.withColumn("totalPayableAmount", lit(None).cast("double"))

        # 4. Map payment methods and amounts (using new names)
        for src, dest in PAYMENT_METHOD_MAPPING.items():
            dest_col = SALES_FIELD_MAPPING.get(src, src)
            if dest_col in mapped_df.columns:
                mapped_df = mapped_df.withColumn(
                    "paymentMethod",
                    when(col(dest_col).isNotNull() & (col(dest_col) != ""), lit(dest)).otherwise(col("paymentMethod"))
                ).withColumn(
                    "totalPayableAmount",
                    when(col(dest_col).isNotNull() & (col(dest_col) != ""), col(dest_col).cast("double")).otherwise(col("totalPayableAmount"))
                )

        # 5. Handle 'Is Cancel Receipt' and 'Non-Sale'
        if "salesStatus" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "salesStatus",
                when(col("salesStatus") == "TRUE", lit("CANCELLED")).otherwise(lit("PAID"))
            )
        if "salesType" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "salesType",
                when(col("salesType") == "TRUE", lit("NON_SALES")).otherwise(lit("SALES"))
            )

        # 6. Handle 'orderDateTime' and 'Order Time' to create 'orderDateTime'
        if "orderDateTime" in mapped_df.columns and "Order Time" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "orderDateTime",
                date_format(
                    to_timestamp(
                        concat_ws(" ", col("orderDateTime"), col("Order Time")),
                        "dd/MM/yyyy HH:mm:ss"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss"
                )
            )

        # 7. Drop any duplicate invoiceNumbers
        if "invoiceNumber" in mapped_df.columns:
            mapped_df = mapped_df.dropDuplicates(["invoiceNumber"])

        # 8. Select only mapped and required columns
        # These are the mapped columns + extra columns added in processSalesData
        required_columns =  ["branchName", "branchCode", "orderDateTime", "systemOrderId",
            "invoiceNumber", "salesStatus", "salesType", "totalPayableAmount", "totalSalesAmount", 
            "totalTaxAmount", "salesStatementId", "merchantId", "currency", "taxCode", "documentUploadId",
            "filePath", "sourceFile", "createdAt", "createdBy", "updatedAt", "updatedBy",
            "paymentMethod"
        ]

        # Only keep columns that actually exist in the DataFrame
        final_columns = [col_name for col_name in required_columns if col_name in mapped_df.columns]
        mapped_df = mapped_df.select(*final_columns)

        return mapped_df
    except Exception as e:
        logger.error(f"ERROR in mapSalesDataFrame: {str(e)}")
        raise

def processPaymentReportErpData(df, documentUploadId):
    """Transform Odoo Payment Report data to match the expected DynamoDB schema, but do not insert to DDB."""
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    merchantId = args['merchantId']

    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

    df_transformed = (
        df
        .withColumn("paymentReportErpId", uuid_udf())
        .withColumn("merchantId", lit(merchantId))
        .withColumn("documentUploadId", lit(documentUploadId))
        .withColumn("filePath", lit(args['s3Paths'].split(",")[0]))
        .withColumn("sourceFile", lit(args['fileNames'].split(",")[0]))
        .withColumn("createdAt", lit(now))
        .withColumn("createdBy", lit("System"))
        .withColumn("updatedAt", lit(now))
        .withColumn("updatedBy", lit("System"))
    )
    return df_transformed

def mapPaymentReportErpData(df):
    """
    Map and clean Odoo Payment Report DataFrame to match the expected schema.
    """
    try:
        mapped_df = df

        # Field mapping
        field_mapping = {
            'Outlet Code': 'branchCode',
            'Analytic Account/Display Name': 'erpDisplayName',
            'Date': 'reportDateTime',
            'Amount': 'amount',
            'Label': 'label'
        }

        # 1. Clean string columns
        for colname in mapped_df.columns:
            mapped_df = mapped_df.withColumn(colname, when(col(colname).isNotNull(), col(colname)).otherwise(lit("-")))

        # 2. Map and rename columns
        for src, dest in field_mapping.items():
            if src in mapped_df.columns:
                mapped_df = mapped_df.withColumnRenamed(src, dest)

        # Ensure branchCode column exists
        if "branchCode" not in mapped_df.columns:
            mapped_df = mapped_df.withColumn("branchCode", lit("-"))

        # Extract branchCode from erpDisplayName if pattern matches
        if "erpDisplayName" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "branchCode",
                when(
                    col("erpDisplayName").rlike("R-[A-Z0-9]{4}"),
                    regexp_extract(col("erpDisplayName"), r"R-([A-Z0-9]{4})", 1)
                ).otherwise(col("branchCode"))  # Simply use the existing value, which will be "-" if we created it above
            )

        # 3. Parse and format reportDateTime
        if "reportDateTime" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "reportDateTime",
                when(col("reportDateTime").isNotNull(),
                     date_format(to_timestamp(col("reportDateTime"), "yyyy-MM-dd"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
                .otherwise(lit("-"))
            )

        # 4. Extract transactionDateTime from label using regex (Txn date : dd.mm.yyyy)
        if "label" in mapped_df.columns:
            # Extract date parts from label
            mapped_df = mapped_df.withColumn(
                "txn_day", regexp_extract(col("label"), r"Txn date\s*:\s*(\d{2})\.(\d{2})\.(\d{4})", 1)
            ).withColumn(
                "txn_month", regexp_extract(col("label"), r"Txn date\s*:\s*(\d{2})\.(\d{2})\.(\d{4})", 2)
            ).withColumn(
                "txn_year", regexp_extract(col("label"), r"Txn date\s*:\s*(\d{2})\.(\d{2})\.(\d{4})", 3)
            ).withColumn(
                "transactionDateTime",
                when(
                    (col("txn_day") != "") & (col("txn_month") != "") & (col("txn_year") != ""),
                    date_format(
                        to_timestamp(
                            concat_ws("-", col("txn_year"), col("txn_month"), col("txn_day")),
                            "yyyy-MM-dd"
                        ),
                        "yyyy-MM-dd'T'00:00:00.000'Z'"
                    )
                ).otherwise(lit("-"))
            ).drop("txn_day", "txn_month", "txn_year")

        # 5. Handle amount and amountType
        if "Credit" in mapped_df.columns and "Debit" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "amount",
                when(col("Debit").isNotNull() & (col("Debit") != ""), col("Debit").cast(DecimalType(18, 2)))
                .when(col("Credit").isNotNull() & (col("Credit") != ""), col("Credit").cast(DecimalType(18, 2)))
                .otherwise(lit(0.00))
            ).withColumn(
                "amountType",
                when(col("Debit").isNotNull() & (col("Debit") != ""), lit("DEBIT"))
                .when(col("Credit").isNotNull() & (col("Credit") != ""), lit("CREDIT"))
                .otherwise(lit(None))
            )

        # 6. Set type based on file name (if available)
        if "sourceFile" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "type",
                when(col("sourceFile").rlike("(?i)grabpay"), lit("GRABPAY"))
                .when(col("sourceFile").rlike("(?i)grabfood"), lit("GRABFOOD"))
                .otherwise(lit(None))
            )

        # 7. Select only required columns
        required_columns = [
            "paymentReportErpId", "merchantId", "branchCode", "erpDisplayName", "label",
            "reportDateTime", "amount", "amountType", "transactionDateTime", "type",
            "documentUploadId", "filePath", "sourceFile", "createdAt", "createdBy", "updatedAt", "updatedBy"
        ]
        final_columns = [c for c in required_columns if c in mapped_df.columns]
        mapped_df = mapped_df.select(*final_columns)

        return mapped_df
    except Exception as e:
        logger.error(f"ERROR in mapPaymentReportErpData: {str(e)}")
        raise

def processPaymentTransactionData(df, documentUploadId):
    """Transform payment transaction data to match the expected DynamoDB schema, but do not insert to DDB."""
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    merchantId = args['merchantId']

    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

    df_transformed = (
        df
        .withColumn("paymentTransactionId", uuid_udf())
        .withColumn("merchantId", lit(merchantId))
        .withColumn("currency", lit("MYR"))
        .withColumn("documentUploadId", lit(documentUploadId))
        .withColumn("filePath", lit(args['s3Paths'].split(",")[0]))
        .withColumn("sourceFile", lit(args['fileNames'].split(",")[0]))
        .withColumn("createdAt", lit(now))
        .withColumn("createdBy", lit("System"))
        .withColumn("updatedAt", lit(now))
        .withColumn("updatedBy", lit("System"))
    )
    return df_transformed

def mapPaymentTransactionData(df, store_mapping_df):
    """
    Map and clean Payment Transaction DataFrame to match the expected schema,
    and enrich branchCode using store mapping.
    """
    try:
        mapped_df = df

        # Field mapping
        field_mapping = {
            'Store Name': 'branchName',
            'Store ID': 'branchUUID',
            'Transaction ID': 'transactionId',
            'Type': 'gatewayTransactionType',
            'Category': 'gatewayTransactionCategory',
            'Status': 'status',
            'Created On': 'paymentDateTime',
            'Transfer Date': 'settlementDateTime',
            'Settlement ID': 'settlementId',
            'Net Sales': 'salesNetAmount',
            'Net MDR': 'processingFee',
            'Total': 'creditAmount',
            'Payment Method': 'paymentMethod',
            'Cancelled by': 'cancelBy',
            'Cancellation Reason': 'cancelReason',
            'Reason for Refund': 'refundReason'
        }

         # 1. Clean string columns
        for colname in mapped_df.columns:
            if colname == 'Transaction ID':  # Use the original column name before renaming
                # For transactionId, convert empty strings to a default value or None
                mapped_df = mapped_df.withColumn(
                    colname, 
                    when(col(colname).isNotNull() & (col(colname) != ""), col(colname))
                    .otherwise(lit(None)) 
                )
            else:
                mapped_df = mapped_df.withColumn(colname, when(col(colname).isNotNull(), col(colname)).otherwise(lit("-")))

        # 2. Map and rename columns
        for src, dest in field_mapping.items():
            if src in mapped_df.columns:
                mapped_df = mapped_df.withColumnRenamed(src, dest)

        # 2.1. Generate UUID for null transactionId after renaming
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        if "transactionId" in mapped_df.columns:  # Now we can use the renamed column
            mapped_df = mapped_df.withColumn(
                "transactionId",
                when(col("transactionId").isNotNull(), col("transactionId"))
                .otherwise(uuid_udf())
            )

        # 3. Convert float fields
        for field in ['salesNetAmount', 'processingFee', 'creditAmount']:
            if field in mapped_df.columns:
                mapped_df = mapped_df.withColumn(
                    field,
                    when(col(field).isNotNull() & (col(field) != ""), regexp_replace(col(field), ",", "").cast("double")).otherwise(0.0)
                )

        # 4. Format datetime fields
        for dt_field in ['paymentDateTime', 'settlementDateTime']:
            if dt_field in mapped_df.columns:
                mapped_df = mapped_df.withColumn(
                    dt_field,
                    when(
                        col(dt_field).isNotNull() & (trim(col(dt_field)) != "-"),
                        date_format(
                            to_timestamp(trim(col(dt_field)), "d MMM yyyy h:mm a"),
                            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                        )
                    ).otherwise(lit(None))
                )

        # 5. Map gatewayTransactionType
        if "gatewayTransactionType" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "gatewayTransactionType",
                when(col("gatewayTransactionType") == "GrabPay", lit("GRABPAY"))
                .when(col("gatewayTransactionType") == "GrabFood", lit("GRABFOOD"))
                .when(col("gatewayTransactionType") == "GrabMart", lit("GRABMART"))
                .otherwise(col("gatewayTransactionType"))
            )

        # 6. Map gatewayTransactionCategory
        if "gatewayTransactionCategory" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "gatewayTransactionCategory",
                when(col("gatewayTransactionCategory") == "Payment", lit("PAYMENT"))
                .when(col("gatewayTransactionCategory") == "Adjustment", lit("ADJUSTMENT"))
                .when(col("gatewayTransactionCategory") == "Advertisement", lit("ADVERTISEMENT"))
                .when(col("gatewayTransactionCategory") == "Refund", lit("REFUND"))
                .when(col("gatewayTransactionCategory") == "Voucher", lit("VOUCHER"))
                .otherwise(col("gatewayTransactionCategory"))
            )

        # 7. Map paymentMethod
        payment_method_map = {
            "RPP": "CREDIT_CARD",
            "GrabPay Wallet": "EWALLET",
            "Cashless": "CASHLESS",
            "Cash": "CASH",
            "Cashless - DBMY": "CASHLESS_DBMY",
            "Cashless - GrabPay Wallet": "CASHLESS_GRABPAY_WALLET",
            "Cashless - MasterCard Credit Card": "CASHLESS_MASTERCARD_CREDIT_CARD",
            "Cashless - PayLater": "CASHLESS_PAYLATER",
            "Cashless - Visa Credit Card": "CASHLESS_VISA_CREDIT_CARD",
            "PayLater Instalments": "PAYLATER_INSTALMENTS",
            "PayLater Postpaid": "PAYLATER_POSTPAID"
        }
        if "paymentMethod" in mapped_df.columns:
            expr = col("paymentMethod")
            for k, v in payment_method_map.items():
                expr = when(col("paymentMethod") == k, lit(v)).otherwise(expr)
            mapped_df = mapped_df.withColumn("paymentMethod", expr)

        # 8. Map status
        status_map = {
            'Transferred': 'SUCCESS',
            'Failed': 'FAILED',
            'Completed': 'SUCCESS',
            'Cancelled': 'FAILED'
        }
        
        if "status" in mapped_df.columns:
            expr = col("status")
            for k, v in status_map.items():
                expr = when(col("status") == k, lit(v)).otherwise(expr)
            mapped_df = mapped_df.withColumn("status", expr)

        # 9. Map transactionType based on refundReason
        if "refundReason" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "transactionType",
                when(col("refundReason").isNotNull() & (col("refundReason") != "-"), lit("SALE")).otherwise(lit("SALE"))
            )
        else:
            mapped_df = mapped_df.withColumn("transactionType", lit("SALE"))

        # 10. Add bankName and bankNameCode columns
        mapped_df = mapped_df.withColumn("bankName", lit("CIMB Bank Berhad"))
        mapped_df = mapped_df.withColumn("bankNameCode", lit("CIMB_I"))

        if "branchCode" not in mapped_df.columns:
            mapped_df = mapped_df.withColumn("branchCode", lit("-"))

        # 11. Enrich branchCode using store mapping
        # Add joinName column for join logic
        mapped_df = mapped_df.withColumn(
            "joinName",
            when(col("gatewayTransactionType") == "GRABPAY", col("branchName"))
            .when(col("gatewayTransactionType") == "GRABFOOD", col("branchName"))
            .when(col("gatewayTransactionType") == "GRABMART", col("branchName"))
            .otherwise(lit(None))
        )

        grabpay_map = store_mapping_df.filter(col("salesChannel") == "GRABPAY") \
            .select(col("branchName").alias("map_joinName"), col("branchCode").alias("map_branchCode"))
        grabfood_map = store_mapping_df.filter(col("salesChannel") == "GRABFOOD") \
            .select(col("branchName").alias("map_joinName"), col("branchCode").alias("map_branchCode"))
        grabmart_map = store_mapping_df.filter(col("salesChannel") == "GRABMART") \
            .select(col("branchName").alias("map_joinName"), col("branchCode").alias("map_branchCode"))

        # Join for each gateway type: transaction.branchName == map_joinName
        grabpay_df = mapped_df.filter(col("gatewayTransactionType") == "GRABPAY") \
            .join(grabpay_map, col("branchName") == col("map_joinName"), "left")
        grabfood_df = mapped_df.filter(col("gatewayTransactionType") == "GRABFOOD") \
            .join(grabfood_map, col("branchName") == col("map_joinName"), "left")
        grabmart_df = mapped_df.filter(col("gatewayTransactionType") == "GRABMART") \
            .join(grabmart_map, col("branchName") == col("map_joinName"), "left")

        others_df = mapped_df.filter(
            (col("gatewayTransactionType") != "GRABPAY") & 
            (col("gatewayTransactionType") != "GRABFOOD") & 
            (col("gatewayTransactionType") != "GRABMART")
        ).withColumn("map_joinName", lit(None)).withColumn("map_branchCode", lit(None))

        # Union all
        enriched_df = grabpay_df.unionByName(grabfood_df).unionByName(grabmart_df).unionByName(others_df)

        # If branchCode is missing or '-', use the mapped branchCode
        enriched_df = enriched_df.withColumn(
            "branchCode",
            when((col("branchCode").isNull()) | (col("branchCode") == "-"), col("map_branchCode")).otherwise(col("branchCode"))
        )

        # Drop helper columns
        enriched_df = enriched_df.drop("map_joinName", "map_branchCode")

        # 12. Select only required columns
        required_columns = [
            "paymentTransactionId", "merchantId", "branchName", "branchUUID", "branchCode", "transactionId",
            "gatewayTransactionType", "gatewayTransactionCategory", "status", "paymentDateTime",
            "settlementDateTime", "settlementId", "salesNetAmount", "processingFee", "creditAmount",
            "paymentMethod", "cancelBy", "cancelReason", "refundReason", "transactionType",
            "bankName", "bankNameCode", "currency", "documentUploadId", "filePath", "sourceFile",
            "createdAt", "createdBy", "updatedAt", "updatedBy"
        ]
        final_columns = [c for c in required_columns if c in enriched_df.columns]
        enriched_df = enriched_df.select(*final_columns)

        return enriched_df
    except Exception as e:
        logger.error(f"ERROR in mapPaymentTransactionData: {str(e)}")
        raise

def mapCreditCardSettlementData(df, store_mapping_df):
    """
    Map and clean Credit Card Settlement DataFrame to match the expected schema.
    """
    try:
        mapped_df = df

        field_mapping = {
            'No.': 'recordNumber',
            'Merchant Name': 'branchName',
            'Merchant Id': 'branchId',
            'EDC Terminal No.': 'posTerminalId',
            'Settlement Date': 'settlementDate',
            'Card No./Trans. ID': 'transactionId',
            'Trans. Date': 'transactionDate',
            'Trans. Time': 'transactionTime',
            'Trans. Amount': 'salesNetAmount',
            'Disc. Amount': 'processingFee',
            'Nett Amount': 'creditAmount',
            'Card Brand': 'cardBrand',
            'Type': 'gatewayTransactionType',
        }

        paymentMethodMapping = {
            'CC': 'CREDIT_CARD',
            'DCC': 'DIRECT_CREDIT_CARD',
            'BP': 'CIMB_BONUS_POINT',
            'TNG': 'TNG',
            'QR': 'QR'
        }

        cardBrandMapping = {
            'VISA': ['Visa Credit', 'Visa Debit','Visa Prepaid'],
            'MASTERCARD': ['MasterCard Credit', 'MasterCard Debit', 'MasterCard Prepaid'],
            'CIMB': ['CIMB CC', 'CIMB DC/CS'],
            'JCB': ['JCB Credit'],
            'MyDebit': ['MyDebit'],
            'RPP': ['RPP Card', 'RPP Casa','RPP Wallet'],
            'TNG': ['TNG'],
            'UnionPay': ['UnionPay Credit', 'UnionPay Debit'],
        }

        for colname in mapped_df.columns:
            if colname == 'Card No./Trans. ID':
                mapped_df = mapped_df.withColumn(
                    colname, 
                    when(col(f"`{colname}`").isNotNull() & (col(f"`{colname}`") != ""), col(f"`{colname}`"))
                    .otherwise(lit(None)) 
                )
            else:
                mapped_df = mapped_df.withColumn(
                    colname, 
                    when(col(f"`{colname}`").isNotNull(), col(f"`{colname}`")).otherwise(lit("-"))
                )

        for src, dest in field_mapping.items():
            if src in mapped_df.columns:
                mapped_df = mapped_df.withColumnRenamed(src, dest)

        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        if "transactionId" in mapped_df.columns:
            if "recordNumber" in mapped_df.columns:
                mapped_df = mapped_df.withColumn(
                    "transactionId",
                    when(col("transactionId").isNotNull(), 
                         concat_ws("-", col("transactionId"), col("recordNumber")))
                    .otherwise(concat_ws("-", uuid_udf(), col("recordNumber")))
                )
            else:
                mapped_df = mapped_df.withColumn(
                    "transactionId",
                    when(col("transactionId").isNotNull(), col("transactionId"))
                    .otherwise(uuid_udf())
                )

        numeric_fields = ['salesNetAmount', 
                          'processingFee',
                          'creditAmount'
                          ]
        for field in numeric_fields:
            if field in mapped_df.columns:
                mapped_df = mapped_df.withColumn(
                    field,
                    when(col(field).isNotNull() & (col(field) != ""), 
                         regexp_replace(col(field), ",", "").cast("double"))
                    .otherwise(0.0)
                )

        if "transactionDate" in mapped_df.columns and "transactionTime" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "paymentDateTime",
                when(
                    col("transactionDate").isNotNull() & (col("transactionTime").isNotNull()),
                    date_format(
                        to_timestamp(
                            concat_ws(" ", col("transactionDate"), col("transactionTime")),
                            "d-MMM-yy HH:mm:ss"
                        ),
                        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                    )
                ).otherwise(lit(None))
            )

        if "settlementDate" in mapped_df.columns:
            mapped_df = mapped_df.withColumn(
                "settlementDateTime",
                when(
                    col("settlementDate").isNotNull() & (trim(col("settlementDate")) != "-"),
                    date_format(
                        to_timestamp(trim(col("settlementDate")), "d-MMM-yy"),
                        "yyyy-MM-dd'T'00:00:00.000'Z'"
                    )
                ).otherwise(lit(None))
            )

        if "cardBrand" in mapped_df.columns:
            expr = col("cardBrand")
            for normalized, variants in cardBrandMapping.items():
                for variant in variants:
                    expr = when(col("cardBrand") == variant, lit(normalized)).otherwise(expr)
            mapped_df = mapped_df.withColumn("cardBrand", expr)

        store_mapping_df = store_mapping_df \
            .withColumnRenamed('merchantId', 'map_merchantId') \
            .withColumnRenamed('branchName', 'map_branchName') \
            .withColumnRenamed('branchCode', 'map_branchCode')
        
        join_df = mapped_df.join(
            store_mapping_df,
            mapped_df.branchId == store_mapping_df.map_merchantId,
            how="left"
        )

        join_df = join_df \
            .withColumn("terminalProvider", col("bankName")) \
            .withColumn("branchName", col("map_branchName")) \
            .withColumn("branchCode", col("map_branchCode"))

        mapped_df = join_df

        mapped_df = mapped_df.withColumn("gatewayTransactionCategory", lit("PAYMENT")) \
            .withColumn("status", lit("SUCCESS")) \
            .withColumn("transactionType", lit("SALE")) \
            .withColumn("bankName", lit("CIMB Bank Berhad")) \
            .withColumn("bankNameCode", lit("CIMB_I")) \
            .withColumn("branchUUID", lit("-"))

        mapped_df = mapped_df.withColumn("cancelBy", lit(None).cast(StringType())) \
            .withColumn("cancelReason", lit(None).cast(StringType())) \
            .withColumn("refundReason", lit(None).cast(StringType())) \
            .withColumn("settlementId", lit(None).cast(StringType())) 

        if "gatewayTransactionType" in mapped_df.columns:
            expr = col("gatewayTransactionType")
            for src, mapped in paymentMethodMapping.items():
                expr = when(col("gatewayTransactionType") == src, lit(mapped)).otherwise(expr)
            mapped_df = mapped_df.withColumn("gatewayTransactionType", expr)

        required_columns = [
            "paymentTransactionId", "merchantId", "branchName", "branchUUID", "branchCode", 
            "transactionId", "gatewayTransactionType", "gatewayTransactionCategory", "status", "paymentDateTime",
            "settlementDateTime", "settlementId", "salesNetAmount", "processingFee", 
            "creditAmount", "cancelBy", "cancelReason", "refundReason", 
            "transactionType", "bankName", "bankNameCode", "currency", "documentUploadId", 
            "filePath", "sourceFile", "createdAt", "createdBy", "updatedAt", 
            "updatedBy", "posTerminalId", "branchId", "terminalProvider", "cardBrand"
        ]
        final_columns = [c for c in required_columns if c in mapped_df.columns]
        mapped_df = mapped_df.select(*final_columns)

        return mapped_df
    except Exception as e:
        logger.error(f"ERROR in mapCreditCardSettlementData: {str(e)}")
        raise

def get_latest_store_mapping_csv(bucket, prefix):
    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    csv_files = [obj for obj in resp.get('Contents', []) if obj['Key'].endswith('.csv')]
    if not csv_files:
        raise Exception("No store mapping CSV found in S3")
    latest = max(csv_files, key=lambda x: x['LastModified'])
    return f"s3://{bucket}/{latest['Key']}"

def processBankData(df, documentUploadId):
    """Process bank data - placeholder for now"""
    return df.withColumn("documentUploadId", lit(documentUploadId))

def processTransactionData(df, documentUploadId):
    """Process transaction data - placeholder for now"""
    return df.withColumn("documentUploadId", lit(documentUploadId))

def saveProcessedData(df, documentType, documentUploadId, batch_size=500):
    """Save processed data to S3 in batches and send SQS messages per batch."""
    try:
        output_base = f"s3://{args['BUCKET_NAME']}/reconciliation/processed/{args['merchantId']}/{documentType}/documentUploadId={documentUploadId}/"
        # Repartition DataFrame to control batch size
        record_count = df.count()
        num_batches = max(1, record_count // batch_size + (1 if record_count % batch_size else 0))
        df = df.repartition(num_batches)
        df.write.mode("overwrite").parquet(output_base)

        # List output files and send SQS message for each
        s3 = boto3.client('s3')
        bucket = args['BUCKET_NAME']
        prefix = f"reconciliation/processed/{args['merchantId']}/{documentType}/documentUploadId={documentUploadId}/"
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        parquet_files = [obj['Key'] for obj in resp.get('Contents', []) if obj['Key'].endswith('.parquet')]

        for key in parquet_files:
            s3_key = f"s3://{bucket}/{key}"
            sendS3KeyToSQS(
                s3_key=s3_key,
                documentUploadId=documentUploadId,
                documentType=documentType,
                fileName=args['fileNames'].split(",")[0]
            )

        return output_base

    except Exception as e:
        logger.error(f"Error saving processed data: {str(e)}")
        raise Exception(f"Error saving processed data: {str(e)}")

def sendS3KeyToSQS(s3_key, documentUploadId, documentType, fileName):
    sqs = boto3.client('sqs')
    queue_url = args.get('SQS_QUEUE_URL')  
    table_name = args.get('DDB_TABLE_NAME') 

    payload = {
        "s3Key": s3_key,
        "documentUploadId": documentUploadId,
        "documentType": documentType,
        "fileName": fileName,
        "tableName": table_name,
        "merchantId": args['merchantId']
    }

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload)
    )

def main():
    documentUploadIds = args['documentUploadIds'].split(",")
    s3Paths = args['s3Paths'].split(",")
    documentTypes = args['documentTypes'].split(",")
    fileNames = args['fileNames'].split(",")
    
    store_mapping_df = None
    
    for idx, documentUploadId in enumerate(documentUploadIds):
        try:
            updateDocumentUploadStatus(documentUploadId, "PROCESSING")
            
            # Read CSV
            df = glueContext.create_dynamic_frame.from_options(
                format_options={"withHeader": True},
                connection_type="s3",
                format="csv",
                connection_options={"paths": [s3Paths[idx]]}
            ).toDF()
            
            # Process based on document type
            documentType = documentTypes[idx]
            if documentType == "sales":
                processed_df = processSalesData(df, documentUploadId)
                processed_df = mapSalesDataFrame(processed_df)
            elif documentType == "erp":
                processed_df = processPaymentReportErpData(df, documentUploadId)
                processed_df = mapPaymentReportErpData(processed_df)
            elif documentType == "bank":
                processed_df = processBankData(df, documentUploadId)
            elif documentType == "transaction":
                # Load store mapping only once
                if store_mapping_df is None:
                    store_mapping_path = get_latest_store_mapping_csv(
                        bucket=args['BUCKET_NAME'],
                        prefix=f"reconciliation/archive/{args['merchantId']}/store/"
                    )
                    store_mapping_df = spark.read.option("header", True).csv(store_mapping_path)
                    # Rename columns for easier join
                    store_mapping_df = store_mapping_df \
                        .withColumnRenamed('Outlet Name', 'branchName') \
                        .withColumnRenamed('Outlet Code', 'branchCode') \
                        .withColumnRenamed('Outlet Code ®', 'alternativeBranchCode') \
                        .withColumnRenamed('Sales Channel', 'salesChannel')

                processed_df = processPaymentTransactionData(df, documentUploadId)
                processed_df = mapPaymentTransactionData(processed_df, store_mapping_df)
            elif documentType == "credit_card_settlement":
                if store_mapping_df is None:
                    store_mapping_path = get_latest_store_mapping_csv(
                        bucket=args['BUCKET_NAME'],
                        prefix=f"reconciliation/archive/{args['merchantId']}/store/"
                    )
                    store_mapping_df = spark.read.option("header", True).csv(store_mapping_path)
                    store_mapping_df = store_mapping_df \
                        .withColumnRenamed('Outlet Name', 'branchName') \
                        .withColumnRenamed('Outlet Code', 'branchCode') \
                        .withColumnRenamed('Merchant ID', 'merchantId') \
                        .withColumnRenamed('Bank Name', 'bankName')

                processed_df = processPaymentTransactionData(df, documentUploadId)
                processed_df = mapCreditCardSettlementData(processed_df, store_mapping_df)
            else:
                raise Exception(f"Unsupported document type: {documentType}")
            
            # Save processed data
            outputPath = saveProcessedData(processed_df, documentType, documentUploadId)
            
            # Update status
            record_count = processed_df.count()
            updateDocumentUploadStatus(
                documentUploadId, 
                "COMPLETED",
                totalRecords=record_count,
                processedPath=outputPath
            )
            
        except Exception as e:
            updateDocumentUploadStatus(documentUploadId, "FAILED", exceptionStatus=str(e))
            logger.error(f"Error processing documentUploadId {documentUploadId}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
    job.commit()