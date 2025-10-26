import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import './UploadPage.css';
import uploadIllustration from '../assets/upload-icon.png';
import { uploadService } from '../services/uploadService';

function UploadPage() {
  const navigate = useNavigate();
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);

  const handleFileSelect = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    // Validate file size (5MB max)
    const maxSize = 5 * 1024 * 1024; // 5MB in bytes
    if (file.size > maxSize) {
      setError('File size exceeds 5MB limit');
      return;
    }

    // Validate file type
    if (file.type !== 'application/pdf') {
      setError('Only PDF files are supported');
      return;
    }

    setUploading(true);
    setError(null);

    try {
      // Upload file using the service
      const { documentId } = await uploadService.completeUpload(file);
      
      if (!documentId) {
        throw new Error('Failed to get document ID');
      }
      
      // Navigate to extraction page with documentId
      navigate(`/extraction/${documentId}`);
    } catch (err) {
      console.error('Upload failed:', err);
      setError('Failed to upload file. Please try again.');
      setUploading(false);
    }
  };

  return (
    <div className="upload-page">
      <div className="upload-container">
        <div className="illustration">
          <img src={uploadIllustration} alt="Upload illustration" />
        </div>
        
        <label htmlFor="file-input" className={`upload-button ${uploading ? 'uploading' : ''}`}>
          {uploading ? (
            <>
              <svg className="spinner" width="20" height="20" viewBox="0 0 20 20" fill="none">
                <circle cx="10" cy="10" r="8" stroke="currentColor" strokeWidth="2" strokeOpacity="0.3"/>
                <path d="M10 2a8 8 0 0 1 8 8" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
              </svg>
              Uploading...
            </>
          ) : (
            <>
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <path d="M10 4v12M4 10h12" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
              </svg>
              Browse Your File Here
            </>
          )}
        </label>
        <input 
          id="file-input" 
          type="file" 
          accept=".pdf"
          onChange={handleFileSelect}
          style={{ display: 'none' }}
          disabled={uploading}
        />
        
        {error && (
          <div className="error-message">
            {error}
          </div>
        )}
        
        <p className="upload-info">
          Upload your document with maximum size: <strong>5MB</strong>
        </p>
        
        <div className="supported-formats">
          Supported formats: <span className="format-badge">PDF</span>
        </div>
      </div>
    </div>
  );
}

export default UploadPage;
