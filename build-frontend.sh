#!/bin/bash
set -e

echo "🎨 Building React Frontend..."

cd lite.frontend.web

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing npm dependencies..."
    npm install
fi

# Build React app
echo "🔨 Building React app..."
npm run build

echo "✅ Frontend build complete!"
