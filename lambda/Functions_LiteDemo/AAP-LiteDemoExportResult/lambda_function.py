import os
import boto3
import json
from aws_lambda_powertools import Logger, Tracer
from decimal import Decimal
import re
import pandas as pd
from io import BytesIO

DOCUMENT_TABLE = os.environ.get('DOCUMENTS_TABLE_NAME')
LITE_DEMO_BUCKET = os.environ.get('LITE_DEMO_BUCKET')


DDB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3')

DOCUMENT_DDB_TABLE = DDB_RESOURCE.Table(DOCUMENT_TABLE)

logger = Logger()
tracer = Tracer()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        logger.info(event)
        parameters = event.get('queryStringParameters', {})

        if parameters and parameters.get('documentId'):
            extracted_document_id = parameters.get('documentId')
                
            extracted_document = get_document(extracted_document_id)

            update_document(extracted_document_id, status="In progress")

            result_key = generate_xlsx(extracted_document)

            presigned_url = generate_presigned_url(result_key, extracted_document)

            update_document(extracted_document_id, status="Completed", s3_key=result_key)

            return create_response(200, "Success", {'presignedUrl': presigned_url})

        else:
            raise Exception("ID is required")
                    
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")
        tracer.put_annotation("lambda_name", context.function_name)
        tracer.put_metadata("event", event)
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return create_response(
            500, 
            "The server encountered an unexpected condition that prevented it from fulfilling your request."
        )
    

@tracer.capture_method
def get_document(document_id):
    response = DOCUMENT_DDB_TABLE.get_item(
        Key={'documentId': document_id}
    )

    existing_document = response.get('Item')
    return existing_document

@tracer.capture_method
def update_document(document_id, status, s3_key=None):
    update_expr = 'SET exportStatus = :exportStatus'
    attr_values = {':exportStatus': status}
    
    if s3_key:
        update_expr += ', resultKey = :resultKey'
        attr_values[':resultKey'] = s3_key
    
    return DOCUMENT_DDB_TABLE.update_item(
        Key={'documentId': document_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=attr_values
    )

@tracer.capture_method
def generate_xlsx(extracted_document):
    """
    {
 "documentId": "82aba35e-26a0-4c34-b881-a2d8dd0bee35",
 "createdAt": 1761286982,
 "data": {
  "formData": [
   {
    "columnName": "buyerAddress",
    "columnValue": "8 Sentosa Gateway, Sentosa Island, Singapore 098269",
    "confidenceScore": 100,
    "displayName": "Buyer Address"
   },
   {
    "assessException": "",
    "columnName": "buyerName",
    "columnValue": "Sentosa Hotels & Resorts Pte Ltd",
    "confidenceScore": 100,
    "displayName": "Buyer Name"
   },
   {
    "columnName": "currency",
    "columnValue": "SGD",
    "confidenceScore": 100,
    "displayName": "Currency"
   },
   {
    "columnName": "deliveryAddress",
    "columnValue": "Sentosa Hotels Main Office",
    "confidenceScore": 100,
    "displayName": "Delivery Address"
   },
   {
    "columnName": "paymentTerms",
    "columnValue": "Net 30 days",
    "confidenceScore": 100,
    "displayName": "Payment Terms"
   },
   {
    "columnName": "poDate",
    "columnValue": "Jan 26, 2025",
    "confidenceScore": 100,
    "displayName": "Po Date"
   },
   {
    "columnName": "poNumber",
    "columnValue": "SNT-PO-2025-0156",
    "confidenceScore": 100,
    "displayName": "Po Number"
   },
   {
    "columnName": "requestDeliveryDate",
    "columnValue": "Feb 15, 2025",
    "confidenceScore": 100,
    "displayName": "Requested Delivery Date"
   },
   {
    "columnName": "supplierAddress",
    "columnValue": "88 North Bridge Road, #12-01, Singapore 179098",
    "confidenceScore": 100,
    "displayName": "Supplier Address"
   },
   {
    "columnName": "supplierName",
    "columnValue": "TechVision Solutions Pte Ltd",
    "confidenceScore": 100,
    "displayName": "Supplier Name"
   },
   {
    "columnName": "taxRate",
    "columnValue": "9%",
    "confidenceScore": 100,
    "displayName": "Tax Rate"
   },
   {
    "columnName": "taxType",
    "columnValue": "GST",
    "confidenceScore": 100,
    "displayName": "Tax Type"
   },
   {
    "columnName": "totalPoAmount",
    "columnValue": "27,577.00",
    "confidenceScore": 100,
    "displayName": "Total PO Price"
   }
  ],
  "tableData": [
   [
    {
     "columnName": "itemDescription",
     "columnValue": "Lenovo ThinkPad E14 Laptop i5/16GB/512GB",
     "confidenceScore": 100,
     "displayName": "Item Description"
    },
    {
     "columnName": "uom",
     "columnValue": "",
     "confidenceScore": 50,
     "displayName": "Unit of Measure"
    },
    {
     "columnName": "quantity",
     "columnValue": "10",
     "confidenceScore": 100,
     "displayName": "Quantity"
    },
    {
     "columnName": "unitPrice",
     "columnValue": "1,850.00",
     "confidenceScore": 100,
     "displayName": "Unit Price"
    },
    {
     "columnName": "totalamountwithtax",
     "columnValue": "18,500.00",
     "confidenceScore": 100,
     "displayName": "TotalAmountWithTax"
    },
    {
     "columnName": "itemCode",
     "columnValue": "",
     "confidenceScore": 100,
     "displayName": "Item Code"
    },
    {
     "columnName": "itemStatus",
     "columnValue": "SUCCESS",
     "confidenceScore": 100,
     "displayName": "Status"
    }
   ],
   [
    {
     "columnName": "itemDescription",
     "columnValue": "HP LaserJet Pro Printer M404dn",
     "confidenceScore": 100,
     "displayName": "Item Description"
    },
    {
     "columnName": "uom",
     "columnValue": "",
     "confidenceScore": 50,
     "displayName": "Unit of Measure"
    },
    {
     "columnName": "quantity",
     "columnValue": "5",
     "confidenceScore": 100,
     "displayName": "Quantity"
    },
    {
     "columnName": "unitPrice",
     "columnValue": "780.00",
     "confidenceScore": 100,
     "displayName": "Unit Price"
    },
    {
     "columnName": "totalamountwithtax",
     "columnValue": "3,900.00",
     "confidenceScore": 100,
     "displayName": "TotalAmountWithTax"
    },
    {
     "columnName": "itemCode",
     "columnValue": "",
     "confidenceScore": 100,
     "displayName": "Item Code"
    },
    {
     "columnName": "itemStatus",
     "columnValue": "SUCCESS",
     "confidenceScore": 100,
     "displayName": "Status"
    }
   ],
   [
    {
     "columnName": "itemDescription",
     "columnValue": "Logitech MX Master 3S Wireless Mouse",
     "confidenceScore": 100,
     "displayName": "Item Description"
    },
    {
     "columnName": "uom",
     "columnValue": "",
     "confidenceScore": 50,
     "displayName": "Unit of Measure"
    },
    {
     "columnName": "quantity",
     "columnValue": "20",
     "confidenceScore": 100,
     "displayName": "Quantity"
    },
    {
     "columnName": "unitPrice",
     "columnValue": "145.00",
     "confidenceScore": 100,
     "displayName": "Unit Price"
    },
    {
     "columnName": "totalamountwithtax",
     "columnValue": "2,900.00",
     "confidenceScore": 100,
     "displayName": "TotalAmountWithTax"
    },
    {
     "columnName": "itemCode",
     "columnValue": "",
     "confidenceScore": 100,
     "displayName": "Item Code"
    },
    {
     "columnName": "itemStatus",
     "columnValue": "SUCCESS",
     "confidenceScore": 100,
     "displayName": "Status"
    }
   ]
  ]
 },
 "fileName": "Sentosa_Hotels_All_Match.pdf",
 "processedAt": "2025-10-24T06:57:47.675554",
 "s3Key": "input/82aba35e-26a0-4c34-b881-a2d8dd0bee35/Sentosa_Hotels_All_Match.pdf",
 "status": "completed",
 "updatedAt": "2025-10-24T06:57:47.675554",
 "uploadedAt": "2025-10-24T06:23:02.881847",
 "uploadType": "document"
}
    """
    # generate a single xlsx file
    # tab 1 is on the formData level
    # tab 2 is tableData level
        
    # Create formData sheet
    form_data = extracted_document['data']['formData']
    form_df = pd.DataFrame([{
        'Field': item['displayName'],
        'Value': item['columnValue'],
        'Confidence Score': item['confidenceScore'],
        'Assess Exception': item.get('assessException', '')
    } for item in form_data])
    
    # Create tableData sheet
    table_data = extracted_document['data']['tableData']
    table_rows = []
    for row in table_data:
        row_dict = {item['displayName']: item['columnValue'] for item in row}
        table_rows.append(row_dict)
    table_df = pd.DataFrame(table_rows)
    
    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        form_df.to_excel(writer, sheet_name='Header Information', index=False)
        table_df.to_excel(writer, sheet_name='Line Item Details', index=False)
    
    # Upload to S3
    fileName = re.sub('.pdf', '', extracted_document['fileName'])
    s3_key = f"results/{fileName}/extracted_data.xlsx"
    
    S3_CLIENT.put_object(
        Bucket=LITE_DEMO_BUCKET,
        Key=s3_key,
        Body=output.getvalue(),
        ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    return s3_key

    
@tracer.capture_method
def generate_presigned_url(s3_key, extracted_document):
    fileName = re.sub('.pdf', '', extracted_document['fileName'])
    presigned_url = S3_CLIENT.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': LITE_DEMO_BUCKET, 
                'Key': s3_key,
                'ResponseContentDisposition': f'attachment; filename="{fileName}.xlsx"'
            }
        )
    
    return presigned_url


@tracer.capture_method
def create_response(status_code, message, payload=None):
    if not payload:
        payload = {}
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Content-Security-Policy': "default-src 'self'; script-src 'self'",
            'X-Content-Type-Options': 'nosniff',
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
            'Cache-control': 'no-store',
            'Pragma': 'no-cache',
            'X-Frame-Options':'SAMEORIGIN'
        },
        'body': json.dumps({"statusCode": status_code, "message": message, **payload}, cls=DecimalEncoder)
    }