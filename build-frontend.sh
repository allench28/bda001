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

# Get API Gateway URL from SSM
echo "🔗 Getting API Gateway URL..."
API_URL=$(aws ssm get-parameter --name "lite-demo-api-url-gw" --region us-east-1 --query "Parameter.Value" --output text 2>/dev/null || echo "")

if [ -n "$API_URL" ]; then
    echo "✅ API URL found: $API_URL"
    # Create .env file with API URL (SSM already includes full path with /lite-demo)
    echo "REACT_APP_API_BASE_URL=${API_URL}" > .env
else
    echo "⚠️  API URL not found in SSM, using default"
fi

# Install dependencies
echo "📦 Installing npm dependencies..."
npm install

# Build React app
echo "🔨 Building React app..."
npm run build

echo "✅ Frontend build complete!"
