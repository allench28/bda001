import boto3
import os
import imaplib, email
from datetime import datetime, timedelta 
from aws_lambda_powertools import Logger, Tracer


INBOX_MONITORING_TABLE = os.environ.get('INBOX_MONITORING_TABLE')
S3_BUCKET = os.environ.get('S3_BUCKET')

DDB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3')

INBOX_MONITORING_DDB_TABLE = DDB_RESOURCE.Table(INBOX_MONITORING_TABLE)

logger = Logger()
tracer = Tracer()

@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        now = datetime.now().replace(microsecond=0).isoformat().replace(':', '-')
        since = (datetime.now() - timedelta(days=1)).strftime('%d-%b-%Y')
        before = (datetime.now()  + timedelta(days=2)).strftime('%d-%b-%Y')
        smartEyeRecipients = getSmartEyeRecipient()
        for smartEyeRecipient in smartEyeRecipients:
            #Get configurations
            logger.info("Getting: recipients form AAP-InboxMonitoring")
            imapUrl = smartEyeRecipient.get('imapUrl')
            user = smartEyeRecipient.get('user')
            appKey = smartEyeRecipient.get('appKey')
            lastProcessedEmailId = smartEyeRecipient.get('lastProcessedEmailId', 0)
            merchantEmail = smartEyeRecipient.get('merchantEmail')
            merchant_id = smartEyeRecipient.get('merchantId')

            logger.info("Hitting the IMAP4_SSL function with")
            con = imaplib.IMAP4_SSL(imapUrl)
            con.login(user, appKey) 
             
            con.list()
            con.select('Inbox') 
            
            _, data = con.uid("SEARCH", f'(SINCE "{since}" BEFORE "{before}")' )
            logger.info(f"since: {since}")
            logger.info(f"before: {before}")
            logger.info(data)
            ids = data[0]
            idList = ids.split()
            latestEmailId = idList[-1] if len(idList) > 0 else b'-1'
            latestEmailIdInt = int(latestEmailId.decode('utf-8'))
            logger.info("Latest email id: " + str(latestEmailIdInt))
            #Compare the latest emailId with the lastProcessedEmailId
            if lastProcessedEmailId < latestEmailIdInt:
                emailId = -1
                #iterate from lastProcessedEmailId + 1 to latestEmailId
                for emailId in range(int(lastProcessedEmailId) + 1, latestEmailIdInt + 1):
                    _, data = con.uid('FETCH', str(emailId).encode('utf-8'), "(RFC822)")
                    rawEmail = data[0][1] 
                    emailRawString = rawEmail.decode('utf-8')
                    emailObject = email.message_from_string(emailRawString)
                    emailObjectString = emailObject.as_string()
                    #Get the from and to email address
                    toEmail = emailObject['To'].split("<")[-1].split(">")[0]
                    fromEmail = emailObject['From'].split("<")[-1].split(">")[0]
                    #Skip if the email is sent from the merchant
                    # if fromEmail == merchantEmail:
                    #     continue
                    #Manipulate the email object, to make it look like the email is sent from customer directly to our smarteye email
                    emailObjectString = emailObjectString.replace(toEmail, merchantEmail)
                    emailObjectString = emailObjectString.replace(fromEmail, toEmail)
                    emailObjectByte = emailObjectString.encode('utf-8')

                    fileName = now + str(emailId) + smartEyeRecipient.get('inboxMonitoringId')
                    filePath = f'/tmp/{fileName}'

                    with open(filePath, "wb") as binary_file:
                        binary_file.write(emailObjectByte)
                    
                    logger.info("Uploading: " + fileName)
                    S3_CLIENT.upload_file(filePath, S3_BUCKET, f'email/{merchant_id}/{fileName}')
                if emailId != -1:
                    updateSmartEyeRecipient(smartEyeRecipient.get('inboxMonitoringId'), emailId, now)

        logger.info("Successfully grab emails.")       
                
        return {
            "statusCode": 200,
            "body": {
                "message": "Successfully grab emails."
            }
        }
    except Exception as ex:
        tracer.put_annotation("lambda_error", "true")  
        tracer.put_metadata("message", str(ex))
        logger.exception({"message": str(ex)})
        return {'status': False, 'message': str(ex)} 
 
@tracer.capture_method
def search(key, value, con): 
    _, data = con.search(None, key, '"{}"'.format(value))
    return data

@tracer.capture_method
def getSmartEyeRecipient():
    items = []
    response = INBOX_MONITORING_DDB_TABLE.scan()
    nextToken = response.get('LastEvaluatedKey')
    items.extend(response.get('Items'))
    while nextToken:
        response = INBOX_MONITORING_DDB_TABLE.scan(ExclusiveStartKey=nextToken)
        nextToken = response.get('LastEvaluatedKey')
        items.extend(response.get('Items'))
    return items

@tracer.capture_method
def updateSmartEyeRecipient(smartEyeRecipientId, lastProcessedEmailId, now):
    item = {
        'updatedBy': 'System',
        'updatedAt': now,
        'lastProcessedEmailId': lastProcessedEmailId
    }
    updateExpression = 'Set '
    expressionAttributesValues = {}
    expressionAttributeNames = {}
    
    for key, val in item.items():
        keyVal = key
        keySharp = f'#{key}'
        updateExpression += f', {keySharp}=:{keyVal}' if updateExpression != 'Set ' else f'{keySharp}=:{keyVal}'
        expressionAttributesValues[f':{keyVal}'] = val
        expressionAttributeNames[keySharp] = key
    
    INBOX_MONITORING_DDB_TABLE.update_item(
        Key = {'inboxMonitoringId':smartEyeRecipientId},
        UpdateExpression = updateExpression,
        ExpressionAttributeValues = expressionAttributesValues,
        ExpressionAttributeNames=expressionAttributeNames
    )