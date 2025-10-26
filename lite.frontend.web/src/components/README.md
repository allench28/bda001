# PDFPreview Component

## Overview
Reusable React component untuk menampilkan PDF preview menggunakan `react-pdf` library.

## Features
- ✅ PDF rendering dengan text layer dan annotation layer
- ✅ Pagination (Previous/Next buttons)
- ✅ Loading state dengan spinner
- ✅ Error handling
- ✅ Responsive sizing
- ✅ File name display
- ✅ Page counter

## Dependencies
- `react-pdf` - PDF rendering
- `pdfjs-dist` - PDF.js worker

## Props

| Prop | Type | Required | Description |
|------|------|----------|-------------|
| pdfUrl | string | Yes | URL to the PDF file (presigned S3 URL) |
| fileName | string | No | Name of the PDF file to display |

## Usage

```javascript
import PDFPreview from '../components/PDFPreview';

function MyComponent() {
  const [pdfUrl, setPdfUrl] = useState(null);
  
  return (
    <div style={{ height: '700px' }}>
      <PDFPreview 
        pdfUrl={pdfUrl} 
        fileName="document.pdf" 
      />
    </div>
  );
}
```

## Styling
Component menggunakan Tailwind CSS classes dan custom CSS untuk PDF.js layers.

## Notes
- PDF.js worker loaded dari CDN (unpkg.com)
- Component auto-adjusts PDF width based on container
- Height should be set on parent container
