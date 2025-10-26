import React, { useState, useEffect } from 'react';
import { Document, Page, pdfjs } from 'react-pdf';
import 'react-pdf/dist/Page/AnnotationLayer.css';
import 'react-pdf/dist/Page/TextLayer.css';
import './PDFPreview.css';

// Set up PDF.js worker
pdfjs.GlobalWorkerOptions.workerSrc = `//unpkg.com/pdfjs-dist@${pdfjs.version}/build/pdf.worker.min.mjs`;

function PDFPreview({ pdfUrl, fileName }) {
  const [numPages, setNumPages] = useState(null);
  const [pageNumber, setPageNumber] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  function onDocumentLoadSuccess({ numPages }) {
    setNumPages(numPages);
    setLoading(false);
  }

  function onDocumentLoadError(error) {
    console.error('Error loading PDF:', error);
    setError('Failed to load PDF');
    setLoading(false);
  }

  const goToPrevPage = () => {
    setPageNumber(prev => Math.max(prev - 1, 1));
  };

  const goToNextPage = () => {
    setPageNumber(prev => Math.min(prev + 1, numPages));
  };

  if (!pdfUrl) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center text-gray-400">
          <div className="text-lg font-semibold mb-1">No PDF Available</div>
          <div className="text-sm">Upload a document to preview</div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-auto flex items-center justify-center bg-gray-100 p-4">
        {loading && (
          <div className="text-center">
            <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900 mb-2"></div>
            <p className="text-sm text-gray-600">Loading PDF...</p>
          </div>
        )}
        
        {error && (
          <div className="text-center text-red-500">
            <p className="font-medium">{error}</p>
          </div>
        )}

        <Document
          file={pdfUrl}
          onLoadSuccess={onDocumentLoadSuccess}
          onLoadError={onDocumentLoadError}
          loading=""
          error=""
          className="max-w-full"
        >
          <Page
            pageNumber={pageNumber}
            renderTextLayer={true}
            renderAnnotationLayer={true}
            className="shadow-lg"
            width={Math.min(window.innerWidth * 0.35, 600)}
          />
        </Document>
      </div>

      {numPages && (
        <div className="border-t bg-white px-4 py-3 flex items-center justify-between">
          <div className="text-sm text-gray-600">
            {fileName && <span className="font-medium">{fileName}</span>}
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={goToPrevPage}
              disabled={pageNumber <= 1}
              className="px-3 py-1 border border-gray-300 rounded text-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Previous
            </button>
            <span className="text-sm text-gray-600">
              Page {pageNumber} of {numPages}
            </span>
            <button
              onClick={goToNextPage}
              disabled={pageNumber >= numPages}
              className="px-3 py-1 border border-gray-300 rounded text-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default PDFPreview;
