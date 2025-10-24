#!/usr/bin/env python3
"""
Simple test script for Lite Demo API endpoints
Usage: python test_api.py <api_gateway_url>
"""

import requests
import json
import sys
import uuid

def test_api(base_url):
    """Test the Lite Demo API endpoints"""
    
    # Remove trailing slash
    base_url = base_url.rstrip('/')
    
    print(f"Testing API at: {base_url}")
    print("=" * 50)
    
    # Test 1: Generate Upload Link
    print("\n1. Testing Upload Link Generation...")
    upload_payload = {
        "fileName": "test-invoice.pdf",
        "documentType": "invoice"
    }
    
    try:
        response = requests.post(
            f"{base_url}/lite-demo/generate-upload-link",
            json=upload_payload,
            headers={"Content-Type": "application/json"}
        )
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Document ID: {data.get('documentId', 'N/A')}")
            print(f"Upload URL: {data.get('uploadUrl', 'N/A')[:100]}...")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 2: Get Document (with sample ID)
    print("\n2. Testing Get Document...")
    sample_doc_id = str(uuid.uuid4())
    
    try:
        response = requests.get(f"{base_url}/lite-demo/get-document?documentId={sample_doc_id}")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 3: Get Result
    print("\n3. Testing Get Result...")
    
    try:
        response = requests.get(f"{base_url}/lite-demo/get-result?documentId={sample_doc_id}")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "=" * 50)
    print("API Testing Complete!")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_api.py <api_gateway_url>")
        print("Example: python test_api.py https://abc123.execute-api.us-east-1.amazonaws.com/prod")
        sys.exit(1)
    
    api_url = sys.argv[1]
    test_api(api_url)