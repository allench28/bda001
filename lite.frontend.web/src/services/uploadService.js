import { API_BASE_URL } from '../config';

export const uploadService = {
  /**
   * Generate upload link from API
   * @param {string} fileName - Name of the file to upload
   * @returns {Promise<{uploadUrl: string}>}
   */
  async generateUploadLink(fileName) {
    const response = await fetch(`${API_BASE_URL}lite-demo/generate-upload-link`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        uploadType: 'document',
        fileName: fileName
      })
    });

    if (!response.ok) {
      throw new Error('Failed to generate upload link');
    }

    return await response.json();
  },

  /**
   * Upload file to S3 using presigned POST
   * @param {object} uploadData - Object containing url and fields from API
   * @param {File} file - The file to upload
   * @returns {Promise<Response>}
   */
  async uploadFile(uploadData, file) {
    // Parse the uploadUrl if it's a string
    const uploadInfo = typeof uploadData === 'string' 
      ? JSON.parse(uploadData) 
      : uploadData;
    
    const { url, fields } = uploadInfo;
    
    // Create form data with all the presigned fields
    const formData = new FormData();
    
    // Add all the fields from the presigned POST data
    Object.keys(fields).forEach(key => {
      formData.append(key, fields[key]);
    });
    
    // Add the file last (important for S3)
    formData.append('file', file);
    
    // Make POST request to S3
    const response = await fetch(url, {
      method: 'POST',
      body: formData,
      // Don't set Content-Type header - browser will set it automatically with boundary
    });

    // S3 returns 204 No Content on success
    if (response.status !== 204 && !response.ok) {
      const errorText = await response.text();
      throw new Error(`Failed to upload file: ${response.status} - ${errorText}`);
    }

    return response;
  },

  /**
   * Extract document ID from S3 key
   * @param {string} s3Key - S3 key in format "input/{documentId}/{filename}"
   * @returns {string|null}
   */
  extractDocumentId(s3Key) {
    if (!s3Key) return null;
    
    // S3 key format: input/464aa7cf-7fcd-478a-b5d2-27bce2fba2e0/Rockstar.pdf
    const parts = s3Key.split('/');
    if (parts.length >= 2 && parts[0] === 'input') {
      return parts[1]; // Return the UUID
    }
    
    return null;
  },

  /**
   * Complete upload flow: generate link and upload file
   * @param {File} file - The file to upload
   * @returns {Promise<{success: boolean, s3Key: string, documentId: string}>}
   */
  async completeUpload(file) {
    try {
      // Step 1: Generate upload link
      const { uploadUrl, s3Path } = await this.generateUploadLink(file.name);
      
      console.log('Generated upload URL:', uploadUrl);
      
      // Step 2: Upload file using presigned POST
      await this.uploadFile(uploadUrl, file);
      
      // Parse to get S3 key for reference
      const uploadInfo = typeof uploadUrl === 'string' 
        ? JSON.parse(uploadUrl) 
        : uploadUrl;
      
      const s3Key = uploadInfo.fields?.key || null;
      const documentId = this.extractDocumentId(s3Key);
      
      return { 
        success: true, 
        s3Key,
        s3Path,
        documentId
      };
    } catch (error) {
      console.error('Upload error:', error);
      throw error;
    }
  }
};
