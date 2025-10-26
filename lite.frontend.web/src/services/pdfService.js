import { API_BASE_URL } from '../config';

export const pdfService = {
  /**
   * Generate download link for PDF from S3
   * @param {string} s3Path - The S3 path (e.g., "input/xxx/filename.pdf")
   * @returns {Promise<{downloadUrl: string}>}
   */
  async generateDownloadLink(s3Path) {
    const response = await fetch(
      `${API_BASE_URL}lite-demo/generate-download-link?s3Path=${encodeURIComponent(s3Path)}`
    );

    if (!response.ok) {
      throw new Error('Failed to generate download link');
    }

    return await response.json();
  }
};
