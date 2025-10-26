#!/bin/bash
set -e

echo "🎨 Building React Frontend..."

# Check if frontend directory has content
if [ ! -f "lite.frontend.web/package.json" ]; then
    echo "⚠️  Frontend not found, creating placeholder..."
    mkdir -p lite.frontend.web/build
    cat > lite.frontend.web/build/index.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Frontend Placeholder</title>
</head>
<body>
    <h1>Frontend Not Deployed</h1>
    <p>Please clone the frontend repository into lite.frontend.web/</p>
</body>
</html>
EOF
    echo "✅ Placeholder created"
    exit 0
fi

cd lite.frontend.web

# Install dependencies
echo "📦 Installing npm dependencies..."
npm install

# Build React app
echo "🔨 Building React app..."
npm run build

echo "✅ Frontend build complete!"
