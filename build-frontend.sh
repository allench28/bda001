#!/bin/bash
set -e

echo "🎨 Building React Frontend..."

# Check if frontend directory exists
if [ ! -d "lite.frontend.web" ]; then
    echo "⚠️  Frontend directory not found, skipping frontend build"
    mkdir -p lite.frontend.web/build
    echo "<html><body>Frontend placeholder</body></html>" > lite.frontend.web/build/index.html
    exit 0
fi

cd lite.frontend.web

# Check if package.json exists
if [ ! -f "package.json" ]; then
    echo "⚠️  package.json not found, skipping frontend build"
    mkdir -p build
    echo "<html><body>Frontend placeholder</body></html>" > build/index.html
    exit 0
fi

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing npm dependencies..."
    npm install
fi

# Build React app
echo "🔨 Building React app..."
npm run build

echo "✅ Frontend build complete!"
