#!/bin/bash

# EC2 Setup Script for CDK Deployment
set -e

echo "Setting up EC2 for CDK deployment..."

# Update system
sudo yum update -y

# Install Node.js 18
curl -fsSL https://rpm.nodesource.com/setup_18.x | sudo bash -
sudo yum install -y nodejs

# Install Python and pip
sudo yum install -y python3 python3-pip git unzip

# Install AWS CDK globally
sudo npm install -g aws-cdk

# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf awscliv2.zip aws/

# Verify installations
echo "Verifying installations..."
node --version
python3 --version
cdk --version
aws --version

echo "Setup complete! Next steps:"
echo "1. Run: aws configure"
echo "2. Clone your repo: git clone https://github.com/allench28/bda001.git"
echo "3. cd bda001 && source .venv/bin/activate"
echo "4. pip install -r requirements.txt"
echo "5. cdk bootstrap"
echo "6. cdk deploy --all"