#!/bin/bash
set -e

echo "🎨 Building React Frontend..."

cd lite.frontend.web

# Install dependencies
echo "📦 Installing npm dependencies..."
npm install

# Build React app
echo "🔨 Building React app..."
npm run build

echo "✅ Frontend build complete!"
