import { API_BASE_URL } from '../config';

export const documentService = {
  /**
   * Fetch document details by ID
   * @param {string} documentId - The document ID
   * @returns {Promise<Object>}
   */
  async getDocumentDetails(documentId) {
    const response = await fetch(`${API_BASE_URL}lite-demo/get-document?documentId=${documentId}`);

    if (!response.ok) {
      throw new Error(`Failed to fetch document: ${response.status}`);
    }

    return await response.json();
  },

  /**
   * Poll document until status is completed
   * @param {string} documentId - The document ID
   * @param {Function} onUpdate - Callback for each poll update
   * @param {number} interval - Polling interval in ms (default 3000)
   * @param {number} maxAttempts - Maximum polling attempts (default 60)
   * @returns {Promise<Object>}
   */
  async pollDocumentUntilComplete(documentId, onUpdate = null, interval = 3000, maxAttempts = 60) {
    let attempts = 0;

    while (attempts < maxAttempts) {
      try {
        const result = await this.getDocumentDetails(documentId);
        
        // Call update callback if provided
        if (onUpdate) {
          onUpdate(result);
        }

        // Check if processing is complete
        if (result.status === 'completed') {
          return result;
        }

        // Check if failed
        if (result.status === 'failed') {
          throw new Error('Document processing failed');
        }

        // Wait before next poll
        console.log(`Polling attempt ${attempts + 1} for document ${documentId}...`);
        await new Promise(resolve => setTimeout(resolve, interval));
        attempts++;
      } catch (error) {
        console.error('Polling error:', error);
        throw error;
      }
    }

    throw new Error('Polling timeout: Document processing took too long');
  },

  /**
   * Export CSV by fetching presigned URL and downloading
   * @param {string} documentId - The document ID
   * @returns {Promise<void>}
   */
  async exportCSV(documentId) {
    const response = await fetch(`${API_BASE_URL}lite-demo/get-result?documentId=${documentId}`);

    if (!response.ok) {
      throw new Error(`Failed to fetch CSV: ${response.status}`);
    }

    const data = await response.json();
    
    if (data.statusCode === 200 && data.presignedUrl) {
      window.open(data.presignedUrl, '_blank');
    } else {
      throw new Error(data.message || 'Failed to get CSV download link');
    }
  },

  /**
   * Parse form data from API response into key-value pairs
   * @param {Array} formData - Array of form data objects
   * @returns {Object}
   */
  parseFormData(formData) {
    const parsed = {};
    
    formData.forEach(field => {
      parsed[field.columnName] = {
        value: field.columnValue,
        displayName: field.displayName,
        confidenceScore: field.confidenceScore
      };
    });

    return parsed;
  },

  /**
   * Parse table data from API response
   * @param {Array} tableData - Array of table rows
   * @returns {Array}
   */
  parseTableData(tableData) {
    return tableData.map((row, index) => {
      const parsedRow = { rowNumber: index + 1 };
      
      row.forEach(cell => {
        parsedRow[cell.columnName] = {
          value: cell.columnValue,
          displayName: cell.displayName,
          confidenceScore: cell.confidenceScore
        };
      });

      return parsedRow;
    });
  }
};
