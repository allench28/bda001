import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import UploadPage from './pages/UploadPage';
import ExtractionPage from './pages/ExtractionPage';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<UploadPage />} />
        <Route path="/extraction/:documentId" element={<ExtractionPage />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
