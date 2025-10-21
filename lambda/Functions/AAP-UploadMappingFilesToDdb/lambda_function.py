import boto3
import csv
import uuid
import io
from datetime import datetime

S3_CLIENT = boto3.client('s3')
DDB_RESOURCE = boto3.resource('dynamodb')

"""
AWS Lambda function to process a CSV file from S3 and import to DynamoDB

Expected event structure:
{
    "bucket_name": "your-s3-bucket",
    "file_key": "path/to/your-file.csv",
    "table_name": "your-dynamodb-table",
    "type": "supplier"/"item"
}
"""

def lambda_handler(event, context):
    # Extract parameters from the event
    bucket_name = event['bucket_name']
    file_key = event['file_key']
    table_name = event['table_name']
    table_type = event.get('type', 'item')  # 'item' or 'supplier'
    user_id = 'System'
        
    # Get the CSV file from S3
    response = S3_CLIENT.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')
    
    # Parse CSV
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    # Fixed values
    merchant_id = "6b8a78e2-95fe-403b-8008-e5f7c1a631fc"
    current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Process rows in batches (DynamoDB batch limit is 25 items)
    batch_size = 25
    items_processed = 0
    batch_items = []
    
    for row in csv_reader:
        # Create item based on table type
        if table_type == 'item':
            # For the original items table
            item_id = str(uuid.uuid4())
            item = {
                'supplierItemId': item_id,
                'merchantId': merchant_id,
                'accountName': row.get('Account Name', ''),
                'accountCode': row.get('Account Code', ''),
                'itemDescription': row.get('Item Description', ''),
                'createdAt': current_time,
                'createdBy': user_id,
                'updatedAt': current_time,
                'updatedBy': user_id
            }
        else:
            # For the supplier table
            supplier_id = str(uuid.uuid4())
            item = {
                'supplierId': supplier_id,
                'merchantId': merchant_id,
                'createdAt': current_time,
                'createdBy': user_id,
                'updatedAt': current_time,
                'updatedBy': user_id
            }
            
            # Add optional fields only if they exist in the CSV
            field_mappings = {
                'Supplier Code': 'supplierCode',
                'Supplier Name': 'supplierName',
                'Supplier Address': 'supplierAddress',
                'Status': 'isActive',
                'Contract/Lease Id': 'contractId',
                'Outlet Type': 'locationType',
                'Outlet Location': 'branchLocation',
                'Outlet Code': 'branchCode',
                'Outlet Name': 'branchName',
                'Odoo Supplier Name': 'erpBranchName',
                'Odoo Display Name': 'erpDisplayName',
                'Odoo External Id': 'erpBranchId',
                'Account No': 'accountId'
            }
            
            for csv_field, dynamo_field in field_mappings.items():
                if csv_field in row and row[csv_field]:
                    item[dynamo_field] = row[csv_field]
        
        # Add to batch
        batch_items.append({
            'PutRequest': {
                'Item': item
            }
        })
        items_processed += 1
        
        # Write batch when it reaches batch_size
        if len(batch_items) >= batch_size:
            DDB_RESOURCE.batch_write_item(
                RequestItems={
                    table_name: batch_items
                }
            )
            batch_items = []
    
    # Process any remaining items
    if batch_items:
        DDB_RESOURCE.batch_write_item(
            RequestItems={
                table_name: batch_items
            }
        )
    
    return {
        'statusCode': 200,
        'body': f'Processed {items_processed} items to table {table_name} as type {table_type}'
    }