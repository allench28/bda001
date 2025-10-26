# Upload Service Implementation

## Overview
Service untuk menangani upload file document ke API backend dengan flow 2-step upload.

## Flow Upload

### 1. Generate Upload Link
Request ke API untuk mendapatkan pre-signed URL:
- **URL**: `https://u8bcobwxfi.execute-api.us-east-1.amazonaws.com/prod/lite-demo/generate-upload-link`
- **Method**: POST
- **Request Body**:
```json
{
  "uploadType": "document",
  "fileName": "example.pdf"
}
```
- **Response**:
```json
{
  "uploadURL": "https://s3.amazonaws.com/..."
}
```

### 2. Upload File
Upload file menggunakan URL yang didapat dari step 1:
- **URL**: URL dari response step 1
- **Method**: PUT
- **Headers**: `Content-Type: application/pdf`
- **Body**: File binary

### 3. Redirect
Setelah upload sukses, redirect user ke `/extraction` page.

## Usage

### Upload Page
```javascript
import { uploadService } from '../services/uploadService';

const file = event.target.files[0];
await uploadService.completeUpload(file);
navigate('/extraction');
```

### Extraction Page (Upload New Document)
```javascript
import { uploadService } from '../services/uploadService';

const handleFileSelect = async (file) => {
  await uploadService.completeUpload(file);
  window.location.reload(); // atau update state
};
```

## File Validation
- **Max Size**: 5MB
- **Format**: PDF only (application/pdf)

## Error Handling
Service akan throw error jika:
- Generate upload link gagal
- Upload file gagal
- Network error

Error harus di-catch dan ditampilkan ke user dengan UI yang sesuai.

---

# PDF Service Implementation

## Overview
Service untuk generate download link PDF dari S3.

## API

### Generate Download Link
- **URL**: `https://u8bcobwxfi.execute-api.us-east-1.amazonaws.com/prod/lite-demo/generate-download-link`
- **Method**: GET
- **Query Parameters**: `s3Path` (e.g., "input/xxx/filename.pdf")
- **Response**:
```json
{
  "downloadUrl": "https://s3.amazonaws.com/presigned-url..."
}
```

## Usage

```javascript
import { pdfService } from '../services/pdfService';

const { downloadUrl } = await pdfService.generateDownloadLink(s3Path);
// Use downloadUrl with PDF viewer component
```

---

# Document Service Implementation

## Overview
Service untuk fetch document details dengan polling mechanism.

## Features
- Polling until status = "completed"
- Parse formData dan tableData
- Automatic retry dengan interval

## Usage

```javascript
import { documentService } from '../services/documentService';

// Poll until complete
const result = await documentService.pollDocumentUntilComplete(
  documentId,
  (update) => {
    console.log('Status:', update.status);
  }
);

// Access data
const formFields = result.data.formData;
const tableRows = result.data.tableData;
```
