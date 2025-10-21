# Lite Demo Lambda Functions

This directory contains simplified Lambda functions for demo purposes without authorization requirements.

## Functions

### 1. AAP-LiteDemoGenerateS3UploadLink

**Purpose:** Generate presigned POST URLs for uploading files directly to S3.

**Endpoint:** `POST /lite-demo/generate-upload-link`

**Request Body:**
```json
{
  "uploadType": "document",  // or "csv"
  "fileName": "example.pdf"
}
```

**Response:**
```json
{
  "statusCode": 200,
  "message": "Success",
  "processId": "uuid-here",
  "uploadUrl": "{presigned-post-data}",
  "s3Path": "input/uuid/filename.pdf"
}
```

**Upload Types:**
- `document`: PDF, JPG, JPEG, PNG files (max 15MB, 1-hour expiration)
- `csv`: CSV files (max 15MB, 1-hour expiration)

**Features:**
- No authentication required
- Automatic file extension validation
- File size limits enforced
- Unique upload IDs using UUID
- Direct S3 upload (no proxy)

---

### 2. AAP-LiteDemoGenerateS3DownloadLink

**Purpose:** Generate presigned GET URLs for downloading files from S3.

**Endpoint:** `GET /lite-demo/generate-download-link?s3Path={path}`

**Query Parameters:**
- `s3Path`: The S3 object path (URL encoded)

**Response:**
```json
{
  "statusCode": 200,
  "message": "Success",
  "url": "https://s3.amazonaws.com/..."
}
```

**Features:**
- No authentication required
- 1-hour URL expiration
- Proper Content-Disposition headers for downloads
- URL decoding handled automatically

---

## Environment Variables

Both functions use:
- `LITE_DEMO_BUCKET`: S3 bucket name for file storage
- `POWERTOOLS_SERVICE_NAME`: Service name for AWS Lambda Powertools
- `POWERTOOLS_METRICS_NAMESPACE`: Metrics namespace
- `LOG_LEVEL`: Logging level (default: INFO)

## Key Differences from Admin Functions

1. **No Authorization**: These functions don't require authentication or permission checks
2. **No DynamoDB**: No database operations or tracking
3. **Simplified Error Handling**: Basic exception handling without custom exceptions
4. **No Merchant Scoping**: Files are not scoped to specific merchants
5. **Extended File Types**: Upload function accepts more file types (PDF, JPG, JPEG, PNG)

## CDK Stack

The functions are deployed via `lite_demo_cdk/lite_demo_apigateway_lambda_stack.py`.

Stack name: `LiteDemoApiGatewayLambdaStack-{env}`

## Security Features

Both functions maintain security best practices:
- CORS headers configured
- Content Security Policy
- X-Content-Type-Options
- Strict-Transport-Security
- Cache-control headers
- X-Frame-Options

## Usage Example

### Upload a file:
```bash
curl -X POST https://api.example.com/lite-demo/generate-upload-link \
  -H "Content-Type: application/json" \
  -d '{"uploadType": "document", "fileName": "invoice.pdf"}'
```

### Download a file:
```bash
curl -X GET "https://api.example.com/lite-demo/generate-download-link?s3Path=input/uuid/invoice.pdf"
```
