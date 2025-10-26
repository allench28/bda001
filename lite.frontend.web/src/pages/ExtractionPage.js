import React, { useState, useRef, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { uploadService } from '../services/uploadService';
import { documentService } from '../services/documentService';
import { pdfService } from '../services/pdfService';
import PDFPreview from '../components/PDFPreview';

function ExtractionPage() {
  const { documentId } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('header');
  const [uploading, setUploading] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [documentData, setDocumentData] = useState(null);
  const [processingStatus, setProcessingStatus] = useState('processing');
  const [pdfUrl, setPdfUrl] = useState(null);
  const [exporting, setExporting] = useState(false);
  const fileInputRef = useRef(null);

  useEffect(() => {
    if (!documentId) {
      setError('No document ID provided');
      setLoading(false);
      return;
    }

    // Start polling for document data
    const fetchDocument = async () => {
      try {
        setLoading(true);
        const result = await documentService.pollDocumentUntilComplete(
          documentId,
          (update) => {
            setProcessingStatus(update.status);
            console.log('Document status:', update.status);
          }
        );
        
        setDocumentData(result);
        setProcessingStatus(result.status);
        
        console.log('Document result:', result);
        console.log('S3 Key:', result.s3Key);
        
        setLoading(false);
      } catch (err) {
        console.error('Failed to fetch document:', err);
        setError(err.message);
        setLoading(false);
      }
    };

    // Fetch PDF download link in parallel
    const fetchPdfUrl = async () => {
      try {
        // Wait for document data to get s3Key
        const result = await documentService.pollDocumentUntilComplete(
          documentId,
          () => {} // No need for status updates here
        );
        
        if (result.s3Key) {
          console.log('Fetching PDF download link for:', result.s3Key);
          const { url } = await pdfService.generateDownloadLink(result.s3Key);
          console.log('Download URL received:', url);
          setPdfUrl(url);
        } else {
          console.warn('No s3Key found in document result');
        }
      } catch (pdfErr) {
        console.error('Failed to fetch PDF:', pdfErr);
      }
    };

    fetchDocument();
    fetchPdfUrl();
  }, [documentId]);

  const handleUploadClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileSelect = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const maxSize = 5 * 1024 * 1024;
    if (file.size > maxSize) {
      alert('File size exceeds 5MB limit');
      return;
    }

    if (file.type !== 'application/pdf') {
      alert('Only PDF files are supported');
      return;
    }

    setUploading(true);

    try {
      const { documentId: newDocId } = await uploadService.completeUpload(file);
      if (newDocId) {
        navigate(`/extraction/${newDocId}`);
        window.location.reload();
      }
    } catch (err) {
      console.error('Upload failed:', err);
      alert('Failed to upload file. Please try again.');
      setUploading(false);
    }
  };

  // Parse form data for header information
  const getFormDataFields = () => {
    if (!documentData?.data?.formData) return [];
    return documentData.data.formData;
  };

  // Parse table data for line items
  const getTableData = () => {
    if (!documentData?.data?.tableData) return [];
    return documentData.data.tableData;
  };

  // Render loading state
  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900 mb-4"></div>
          <p className="text-gray-700 font-medium">
            {processingStatus === 'processing' ? 'Processing document...' : 'Loading document...'}
          </p>
          <p className="text-sm text-gray-500 mt-2">This may take a few moments</p>
        </div>
      </div>
    );
  }

  // Render error state
  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="text-red-500 text-5xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold text-gray-900 mb-2">Error Loading Document</h2>
          <p className="text-gray-600 mb-4">{error}</p>
          <button
            onClick={() => navigate('/')}
            className="bg-black text-white px-6 py-2 rounded-md hover:bg-gray-800"
          >
            Upload New Document
          </button>
        </div>
      </div>
    );
  }

  const formFields = getFormDataFields();
  const tableRows = getTableData();

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b px-8 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <span className="text-gray-700">Upload a new document to extract data!</span>
          <button 
            onClick={handleUploadClick}
            disabled={uploading}
            className="bg-black text-white px-4 py-2 rounded-md text-sm font-medium hover:bg-gray-800 flex items-center gap-2 disabled:bg-gray-400 disabled:cursor-not-allowed"
          >
            <span>+</span>
            {uploading ? 'Uploading...' : 'Upload New Document'}
          </button>
            <button
              onClick={async () => {
                setExporting(true);
                try {
                  await documentService.exportCSV(documentId);
                } catch (err) {
                  console.error('CSV export failed:', err);
                  alert('Failed to export CSV. Please try again.');
                } finally {
                  setExporting(false);
                }
              }}
              disabled={exporting}
              className="bg-white border border-gray-300 text-gray-700 px-4 py-2 rounded-md text-sm font-medium hover:bg-gray-100 flex items-center gap-2 shadow-sm disabled:bg-gray-100 disabled:cursor-not-allowed"
            >
              {exporting ? (
                <div className="w-5 h-5 border-2 border-gray-300 border-t-gray-600 rounded-full animate-spin"></div>
              ) : (
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-5 h-5">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 16v-8m0 8l-3-3m3 3l3-3M4.5 12a7.5 7.5 0 1115 0 7.5 7.5 0 01-15 0z" />
                </svg>
              )}
              {exporting ? 'Exporting...' : 'Export CSV'}
            </button>
          <input 
            ref={fileInputRef}
            type="file" 
            accept=".pdf"
            onChange={handleFileSelect}
            style={{ display: 'none' }}
          />
        </div>
      </div>

      <div className="px-8 py-6">
        <div className="grid grid-cols-2 gap-6">
          <div className="bg-white rounded-lg shadow-sm overflow-hidden" style={{ height: '900px' }}>
            <PDFPreview pdfUrl={pdfUrl} fileName={documentData?.fileName} />
          </div>

          <div className="bg-white rounded-lg shadow-sm overflow-hidden">
            <div className="flex border-b bg-gray-50">
              <button 
                className={`px-6 py-3 text-sm font-medium transition relative ${
                  activeTab === 'header' 
                    ? 'bg-white text-gray-900 border-b-2 border-black' 
                    : 'bg-gray-50 text-gray-500 hover:text-gray-700'
                }`}
                onClick={() => setActiveTab('header')}
              >
                Header Information
              </button>
              <button 
                className={`px-6 py-3 text-sm font-medium transition relative ${
                  activeTab === 'line' 
                    ? 'bg-white text-gray-900 border-b-2 border-black' 
                    : 'bg-gray-50 text-gray-500 hover:text-gray-700'
                }`}
                onClick={() => setActiveTab('line')}
              >
                Line Item Details
              </button>
            </div>

            <div className="p-8">
              {activeTab === 'header' && (
                <>
                  <div className="flex items-start justify-between mb-6">
                    <div>
                      <h2 className="text-xl font-semibold text-gray-900">Header Information</h2>
                      <p className="text-sm text-gray-400 mt-1">
                        ({formFields.length} data fields detected)
                      </p>
                    </div>
                    <div className="flex gap-2">
                      <button className="w-8 h-8 border border-gray-300 rounded hover:bg-gray-50 flex items-center justify-center text-gray-600">
                        ‹
                      </button>
                      <button className="w-8 h-8 border border-gray-300 rounded hover:bg-gray-50 flex items-center justify-center text-gray-600">
                        ›
                      </button>
                    </div>
                  </div>

                  <div className="space-y-4">
                    {formFields.map((field, index) => (
                      <div key={index} className="grid grid-cols-[180px_1fr] gap-4 items-center">
                        <label className="text-sm text-gray-600 bg-gray-50 px-4 py-3 rounded">
                          {field.displayName}
                        </label>
                        <div className="relative">
                            <input 
                              type="text" 
                              value={field.columnValue || ''} 
                              readOnly 
                              className={`w-full px-4 py-3 border rounded text-sm ${field.assessException ? 'border-red-500 bg-red-50' : 'border-gray-200'}`}
                            />
                          {field.confidenceScore > 0 && (
                            <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-gray-400">
                              {field.confidenceScore}%
                            </span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </>
              )}

              {activeTab === 'line' && (
                <>
                  <div className="flex items-start justify-between mb-6">
                    <div>
                      <h2 className="text-xl font-semibold text-gray-900">Line Item Information</h2>
                      <p className="text-sm text-gray-400 mt-1">
                        ({tableRows.length} items detected)
                      </p>
                    </div>
                    <div className="flex gap-2">
                      <button className="w-8 h-8 border border-gray-300 rounded hover:bg-gray-50 flex items-center justify-center text-gray-600">
                        ‹
                      </button>
                      <button className="w-8 h-8 border border-gray-300 rounded hover:bg-gray-50 flex items-center justify-center text-gray-600">
                        ›
                      </button>
                      {/* <button className="px-4 py-2 border border-red-500 text-red-500 rounded hover:bg-red-50 flex items-center gap-2 text-sm font-medium ml-2">
                        <span>×</span>
                        Delete
                      </button> */}
                        {/* <button className="px-4 py-2 bg-black text-white rounded hover:bg-gray-800 flex items-center gap-2 text-sm font-medium">
                          <span>✓</span>
                          Save
                        </button> */}
                    </div>
                  </div>

                  <div className="border border-gray-200 rounded-lg overflow-hidden">
                    <div className="overflow-x-auto">
                        <table className="w-full">
                          <thead className="bg-gray-50 border-b border-gray-200">
                            <tr>
                              <th className="px-4 py-3 text-left text-sm font-medium text-gray-600 w-16">No</th>
                              {tableRows.length > 0 && tableRows[0].map((cell, idx) => (
                                <th key={idx} className="px-4 py-3 text-left text-sm font-medium text-gray-600">
                                  {cell.displayName}
                                </th>
                              ))}
                              <th className="px-4 py-3 w-12"></th>
                            </tr>
                          </thead>
                          <tbody>
                            {tableRows.map((row, rowIndex) => (
                              <tr key={rowIndex} className="border-b border-gray-200 last:border-b-0">
                                <td className="px-4 py-4 text-sm text-gray-900">{rowIndex + 1}</td>
                                {row.map((cell, cellIndex) => (
                                  <td key={cellIndex} className="px-4 py-4 text-sm text-gray-900">
                                      {cell.columnName === 'itemStatus' ? (
                                        cell.columnValue?.toLowerCase().includes('mismatch') ? (
                                          <span className="text-red-500">{cell.columnValue}</span>
                                        ) : cell.columnValue?.toLowerCase().includes('matched') || cell.columnValue?.toLowerCase().includes('success') ? (
                                          <span className="text-green-500">MATCHED</span>
                                        ) : cell.columnValue?.toLowerCase().includes('unknown') ? (
                                          <span className="text-black">{cell.columnValue}</span>
                                        ) : (
                                          cell.columnValue
                                        )
                                      ) : (
                                        cell.columnValue
                                      )}
                                  </td>
                                ))}
                                {/* <td className="px-4 py-4 text-center">
                                  <button className="text-red-500 hover:text-red-700">
                                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                                    </svg>
                                  </button>
                                </td> */}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                    </div>
                  </div>

                  <div className="mt-4 flex items-center justify-between text-sm text-gray-600">
                    {/* <div className="flex items-center gap-2">
                      <span>Rows per page</span>
                      <select className="border border-gray-300 rounded px-2 py-1">
                        <option>10</option>
                        <option>25</option>
                        <option>50</option>
                      </select>
                    </div> */}
                    {/* <div className="flex items-center gap-4">
                      <span>1-{tableRows.length} of {tableRows.length}</span>
                      <button className="w-8 h-8 bg-black text-white rounded flex items-center justify-center hover:bg-gray-800">
                        1
                      </button>
                    </div> */}
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default ExtractionPage;
