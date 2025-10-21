import os
import uuid
import json
import boto3
import io
from datetime import datetime
import pandas as pd
from aws_lambda_powertools import Logger, Tracer
from authorizationHelper import Permission, has_permission, is_authenticated, get_user, get_user_group
from custom_exceptions import ApiException, AuthenticationException, AuthorizationException, ResourceNotFoundException, BadRequestException

MERCHANT_TABLE = os.environ.get('MERCHANT_TABLE')
COGNITO_USER_POOL = os.environ.get('COGNITO_USER_POOL')
AGENT_CONFIGURATION_BUCKET = os.environ.get('AGENT_CONFIGURATION_BUCKET')
USER_GROUP_TABLE = os.environ.get('USER_GROUP_TABLE')
USER_MATRIX_TABLE = os.environ.get('USER_MATRIX_TABLE')
USER_TABLE = os.environ.get('USER_TABLE')

DDB_RESOURCE = boto3.resource('dynamodb')
COGNITO_CLIENT = boto3.client('cognito-idp')
S3_CLIENT = boto3.client('s3')

MERCHANT_DDB_TABLE = DDB_RESOURCE.Table(MERCHANT_TABLE)
USER_GROUP_DDB_TABLE = DDB_RESOURCE.Table(USER_GROUP_TABLE)
USER_MATRIX_DDB_TABLE = DDB_RESOURCE.Table(USER_MATRIX_TABLE)
USER_DDB_TABLE = DDB_RESOURCE.Table(USER_TABLE)

logger = Logger()
tracer = Tracer()

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)
    
USER_GROUP_NAMES = ["Checker", "Maker", "Admin"]


# {
#     "module": "3 Way Matching Agent",
#     "subModules": ["PO Listing", "Matching Result", "GRN Listing", "GRN Extraction Result"]
# }, {
#     "module": "Customers",
#     "subModules": ["All Customers"]
# }
MODULES = [{
    "module": "AP Invoice Processing",
    "subModules": ["Uploaded Documents", "Extraction Result", "Audit Trails"]
}, {
    "module": "User Matrix",
    "subModules": ["Roles", "All Users"]
}]

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        sub, email, _ = is_authenticated(event)
        user = get_user(sub)
        user_group = get_user_group(user.get('userGroupId')).get('userGroupName')
        has_permission(user_group, Permission.CREATE_CUSTOMER.value)
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        request_body = json.loads(event.get('body', '{}'))

        if request_body.get('merchantId'):
            update_merchant_flow(request_body, email, now)
        else:
            create_merchant_flow(request_body, email, now)

        return create_response(200, "Merchant successfully created/updated")
    
    except (BadRequestException, AuthenticationException, AuthorizationException, ResourceNotFoundException) as ex:
        logger.error(f"Custom error: {str(ex)}")
        return create_response(400, ex.message)
    
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
def update_merchant_flow(request_body, user, timestamp):
    merchant_id = request_body.get('merchantId')
    merchant_item = MERCHANT_DDB_TABLE.get_item(Key={'merchantId': merchant_id}).get('Item')

    if not merchant_item:
        raise BadRequestException("Merchant not found")

    request_body = process_mapping_files(merchant_id, request_body)
    update_merchant_record(request_body, merchant_id, user, timestamp)

@tracer.capture_method
def update_merchant_record(request_body, merchant_id, user, timestamp):
    update_item = {
        'updatedAt': timestamp,
        'updatedBy': user,
        'name': request_body.get('name'),
        'itemMapping': request_body.get('itemMapping'),
        'supplierMapping': request_body.get('supplierMapping')
    }

    MERCHANT_DDB_TABLE.update_item(
        Key={'merchantId': merchant_id},
        UpdateExpression="set #name = :name, #itemMapping = :itemMapping, #supplierMapping = :supplierMapping, updatedAt = :updatedAt, updatedBy = :updatedBy",
        ExpressionAttributeNames={
            '#name': 'name',
            '#itemMapping': 'itemMapping',
            '#supplierMapping': 'supplierMapping'
        },
        ExpressionAttributeValues={
            ':name': update_item['name'],
            ':itemMapping': update_item['itemMapping'],
            ':supplierMapping': update_item['supplierMapping'],
            ':updatedAt': update_item['updatedAt'],
            ':updatedBy': update_item['updatedBy']
        }
    )

@tracer.capture_method
def create_merchant_flow(request_body, user, timestamp):
    admin_email = request_body.get('email')

    cognito_user = create_cognito_user(admin_email)
    merchant_id = create_merchant(request_body, user, timestamp)
    user_group_id = create_user_group(merchant_id, user, timestamp)
    create_admin_user(merchant_id, user_group_id, cognito_user.get('Username'), admin_email, user, timestamp)
    
    
@tracer.capture_method
def create_merchant(request_body, user, timestamp):
    merchant_id = str(uuid.uuid4())

    request_body = process_mapping_files(merchant_id, request_body)
    create_merchant_record(request_body, merchant_id, user, timestamp)

    return merchant_id

@tracer.capture_method
def create_merchant_record(request_body, merchant_id, user, timestamp):
    merchant_item = {
        'merchantId': merchant_id,
        'createdAt': timestamp,
        'createdBy': user,
        'updatedAt': timestamp,
        'updatedBy': user,
        'name': request_body.get('name'),
        'itemMapping': request_body.get('itemMapping'),
        'supplierMapping': request_body.get('supplierMapping')
    }

    MERCHANT_DDB_TABLE.put_item(Item=merchant_item)

    return merchant_id

@tracer.capture_method
def create_user_group(merchant_id, user, timestamp):
    user_groups = ['Admin', 'Maker', 'Checker']

    for group in user_groups:
        user_group_id = str(uuid.uuid4())
        if group == 'Admin':
            admin_user_group_id = user_group_id
            user_group_item = {
                'userGroupId': user_group_id,
                'userGroupName': group,
                'createdAt': timestamp,
                'createdBy': user,
                'updatedAt': timestamp,
                'updatedBy': user,
                'merchantId': merchant_id,
                'totalUser': 1
            }
            create_user_matrix(merchant_id, user_group_id, group, MODULES, timestamp)
        else:
            user_group_item = {
                'userGroupId': user_group_id,
                'userGroupName': group,
                'createdAt': timestamp,
                'createdBy': user,
                'updatedAt': timestamp,
                'updatedBy': user,
                'merchantId': merchant_id,
                'totalUser': 0
            }
            create_user_matrix(merchant_id, user_group_id, group, MODULES, timestamp)
        
        USER_GROUP_DDB_TABLE.put_item(Item=user_group_item)
    
    return admin_user_group_id

@tracer.capture_method
def create_cognito_user(admin_email):
    userAttr = [
        {
            'Name': 'email',
            'Value':admin_email
        },
        {
            'Name': 'email_verified',
            'Value': 'True'
        },
    ]
    checkUserExists(admin_email)

    response = COGNITO_CLIENT.admin_create_user(
        UserPoolId=COGNITO_USER_POOL,
        Username = admin_email,
        UserAttributes=userAttr,
        ForceAliasCreation=False,
        DesiredDeliveryMediums=['EMAIL'],
    )

    return response['User']

@tracer.capture_method
def checkUserExists(email):
    try:
        cognitoResp = COGNITO_CLIENT.admin_get_user(
            UserPoolId = COGNITO_USER_POOL,
            Username=email
        )
        
    except:
        cognitoResp = None
    
    if cognitoResp:
        raise BadRequestException('User with this Email Exists!')

@tracer.capture_method
def create_admin_user(merchant_id, user_group_id, cognito_username, admin_email, user, timestamp):
    user_id = str(uuid.uuid4())

    user_item = {
        'userId': user_id,
        'userGroupId': user_group_id,
        'cognitoUsername': cognito_username,
        'name': "Admin",
        'merchantId': merchant_id,
        'email': admin_email,
        'isDisabled': False,
        'mobileNo': "",
        'createdAt': timestamp,
        'createdBy': user,
        'updatedAt': timestamp,
        'updatedBy': user
    }

    USER_DDB_TABLE.put_item(Item=user_item)

@tracer.capture_method
def create_user_matrix(merchantId, userGroupId, userGroupName, modules, now):
    for module in modules:
        if userGroupName == "Admin" and module.get('module') in ["AP Invoice Processing", "3 Way Matching Agent", "Customers"]:
            continue
        if userGroupName in ["Checker", "Maker"] and module.get('module') not in ["AP Invoice Processing", "3 Way Matching Agent"]:
            continue
        
        for subModule in module.get('subModules'):
            user_matrix_record = {
                "userMatrixId": str(uuid.uuid4()),
                "canAdd": True,
                "canDelete": True,
                "canEdit": True,
                "canList": True,
                "canView": True,
                "createdAt": now,
                "createdBy": "System",
                "merchantId": merchantId,
                "module": module.get('module'),
                "parentUserMatrixId": None,
                "subModule": subModule,
                "updatedAt": now,
                "updatedBy": "System",
                "userGroupId": userGroupId
            }
            USER_MATRIX_DDB_TABLE.put_item(Item=user_matrix_record)

@tracer.capture_method
def process_mapping_files(merchant_id, request_body):
    supplier_mapping = request_body.get('mappingOneURL')
    item_mapping = request_body.get('mappingTwoURL')

    if not supplier_mapping and not item_mapping:
        raise BadRequestException("Both supplier and item mapping files are required")

    mapping_paths = {
        'supplier_mapping': supplier_mapping,
        'item_mapping': item_mapping
    }

    file_paths = [path for path in mapping_paths.values() if path]
    verify_files_exist(file_paths)
    verify_file_contents(file_paths)
    
    perm_mapping_paths = move_files_to_permanent(merchant_id, mapping_paths)

    request_body['supplierMapping'] = perm_mapping_paths.get('supplier_mapping')
    request_body['itemMapping'] = perm_mapping_paths.get('item_mapping')

    return request_body

    
@tracer.capture_method
def verify_files_exist(file_paths):
    for path in file_paths:
        file_name = path.split('/')[-1]
        try:
            S3_CLIENT.head_object(
                Bucket=AGENT_CONFIGURATION_BUCKET,
                Key=path
            )
        except S3_CLIENT.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"Mapping file not found: {path}")
                raise ResourceNotFoundException("Mapping file", file_name)
            else:
                logger.error(f"S3 error when checking file: {path}, error: {str(e)}")
                raise Exception(f"Error accessing mapping files")
    
    return True


@tracer.capture_method
def verify_file_contents(file_paths):
    for file_path in file_paths:
        response = S3_CLIENT.get_object(
            Bucket=AGENT_CONFIGURATION_BUCKET,
            Key=file_path
        )
        csv_content = response['Body'].read().decode('utf-8')

        if not csv_content.strip():
            logger.error(f"File {file_path} is empty")
            raise BadRequestException("One or more mapping files is empty")

        try:
            df = pd.read_csv(io.StringIO(csv_content))
        except Exception as csv_error:
            logger.error(f"Failed to parse CSV file {file_path}: {str(csv_error)}")
            raise BadRequestException("One or more mapping files is not a valid CSV")

        if df.empty:
            logger.error(f"File {file_path} has no data rows")
            raise BadRequestException("One or more mapping files has no data rows")
        
        df_headers = list(df.columns)
        has_code = any("code" in header.lower() for header in df_headers)
        has_name_or_desc = any("name" in header.lower() or "description" in header.lower() for header in df_headers)
        
        if not has_code or not has_name_or_desc:
            logger.error(f"File {file_path} is missing required columns. Headers found: {df_headers}")
            raise BadRequestException("Mapping files must contain required columns")

    return True


@tracer.capture_method
def move_files_to_permanent(merchant_id, file_paths):
    permanent_paths = {}
    moved_files = []

    for file_type, path in file_paths.items():
        folder_name = path.split('/', 1)[0]
        if folder_name == "temp":
            original_filename = path.split('/')[-1]
            permanent_path = f"{merchant_id}/{file_type}/{original_filename}"
            S3_CLIENT.copy_object(
                Bucket=AGENT_CONFIGURATION_BUCKET,
                CopySource={'Bucket': AGENT_CONFIGURATION_BUCKET, 'Key': path},
                Key=permanent_path
            )
            permanent_paths[file_type] = permanent_path
            moved_files.append(path)
        else:
            permanent_paths[file_type] = path

    for temp_path in moved_files:
        S3_CLIENT.delete_object(
            Bucket=AGENT_CONFIGURATION_BUCKET,
            Key=temp_path
        )

    return permanent_paths

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
        'body': json.dumps({"statusCode": status_code, "message": message, **payload})
    }