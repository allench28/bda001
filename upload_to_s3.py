#!/usr/bin/env python3
"""
Simple script to upload a file to S3 using presigned POST URL
"""

import json
import requests
import sys
import os


def upload_file_to_s3(upload_url_json, file_path):
    """
    Upload a file to S3 using presigned POST data
    
    Args:
        upload_url_json (str): JSON string containing url and fields from the API response
        file_path (str): Path to the file to upload
    
    Returns:
        bool: True if upload successful, False otherwise
    """
    try:
        # Parse the uploadUrl JSON string
        upload_data = json.loads(upload_url_json)
        url = upload_data['url']
        fields = upload_data['fields']
        
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' not found")
            return False
        
        print(f"Uploading to: {url}")
        print(f"S3 Key: {fields['key']}\n")
        
        # Open and upload the file
        with open(file_path, 'rb') as file:
            files = {'file': file}
            
            # Make the POST request with the presigned data
            response = requests.post(url, data=fields, files=files)
            
            if response.status_code == 204:
                print(f"✓ File uploaded successfully to S3")
                print(f"  S3 Path: {fields['key']}")
                return True
            else:
                print(f"✗ Upload failed with status code: {response.status_code}")
                print(f"  Response: {response.text}")
                return False
                
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse uploadUrl JSON: {e}")
        return False
    except Exception as e:
        print(f"Error: Upload failed: {e}")
        return False


def main():
    # Simply paste the uploadUrl value from the API response here
    # Just copy the value of the "uploadUrl" field (the JSON string)
    upload_url = "{\"url\": \"https://litedemo-documents-bucket-ap-southeast-1.s3.ap-southeast-1.amazonaws.com/\", \"fields\": {\"key\": \"input/c256d762-5c01-417d-ad6b-c37c80904187/lite-document-testing.pdf\", \"x-amz-algorithm\": \"AWS4-HMAC-SHA256\", \"x-amz-credential\": \"ASIA54WNANFR7PWNW2JB/20251021/ap-southeast-1/s3/aws4_request\", \"x-amz-date\": \"20251021T163603Z\", \"x-amz-security-token\": \"IQoJb3JpZ2luX2VjEGEaDmFwLXNvdXRoZWFzdC0xIkYwRAIgQk1GcvTwESH9caI32B3DgvrvXbqvbWoP3K2idza/8wYCIGmS5DECZcqU+IFH7YZb4l37NM752NmKOX//rXo1JBvyKqYDCBoQABoMOTU0OTg2NDI0Njc1Igymrfjgm8uBcdNQP0UqgwPmEkXFyOU/5vNK8qeJPysZuN272UxqnUrkZJQMbj1JdfyfgIzq3i2N6Hi+NeKxmN/vjcMEyGNuZ/HK1fQtIxCoSpXqlwC1EOWH6dOuIPc/u1wb4iGwwuEHVwgcRvAr4XztMHAaQ1VRZIkMWcmF2olcC6uEebVJ1Ae/WqyQgGsbZShrdNMj0Sk2v8htZitAkIJ+vneMdxz2EeXkMADn5sm12IFLk8xa2fAhqJTJI+h9N957Fb4cAVmdVSXgMLuRj3J0DtEBpgpCBj2nt1FQOHPB+b2sEDFkVnDZNMm51iNn0KTxMKxwoJPGpMjKDsWz4IoJJZASgCrpp6cWdvyf9F5LWOUmanL1QiXoA2EMPhSeVijlEgkdrUHjLFWAxG58smw5d9sdp4MSqsq4z/E+ymhr8Q5XxuVIwrlL5yqrLFKabu3Cv7O68LckwuTZvh3TOHUjLUzwM5kb6pSwd8RdW7UmV0uyboZIJn8u9AQqAT4baA+p80UMzs1Xft5fDgt/0Lj0HRow8ezexwY6ngEdhZEIQcEQanwqhBZBPZJ3Bahu6Gllkn7h6rnpkyXUPLEH53tkSreN+Ofp8nWcjYYy4H251QjYbHBFiGyMYXb92bqskoWAa7r1d17FaE8ZSu22U8gavYLKiA0qU6pZAKvw2TzxaH507TC3AmO7nexUv1asL+JVdFYytK4sRTPTo4gGlgT6LK+dGDZAY0eFB7uOtIo8LAu56Zg0xgCx+A==\", \"policy\": \"eyJleHBpcmF0aW9uIjogIjIwMjUtMTAtMjFUMTc6MzY6MDNaIiwgImNvbmRpdGlvbnMiOiBbWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsIDAsIDE1NzI4NjQwXSwgeyJidWNrZXQiOiAibGl0ZWRlbW8tZG9jdW1lbnRzLWJ1Y2tldC1hcC1zb3V0aGVhc3QtMSJ9LCB7ImtleSI6ICJpbnB1dC9jMjU2ZDc2Mi01YzAxLTQxN2QtYWQ2Yi1jMzdjODA5MDQxODcvbGl0ZS1kb2N1bWVudC10ZXN0aW5nLnBkZiJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotY3JlZGVudGlhbCI6ICJBU0lBNTRXTkFORlI3UFdOVzJKQi8yMDI1MTAyMS9hcC1zb3V0aGVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSwgeyJ4LWFtei1kYXRlIjogIjIwMjUxMDIxVDE2MzYwM1oifSwgeyJ4LWFtei1zZWN1cml0eS10b2tlbiI6ICJJUW9KYjNKcFoybHVYMlZqRUdFYURtRndMWE52ZFhSb1pXRnpkQzB4SWtZd1JBSWdRazFHY3ZUd0VTSDljYUkzMkIzRGd2cnZYYnF2YldvUDNLMmlkemEvOHdZQ0lHbVM1REVDWmNxVStJRkg3WVpiNGwzN05NNzUyTm1LT1gvL3JYbzFKQnZ5S3FZRENCb1FBQm9NT1RVME9UZzJOREkwTmpjMUlneW1yZmpnbTh1QmNkTlFQMFVxZ3dQbUVrWEZ5T1UvNXZOSzhxZUpQeXNadU4yNzJVeHFuVXJrWkpRTWJqMUpkZnlmZ0l6cTNpMk42SGkrTmVLeG1OL3ZqY01FeUdOdVovSEsxZlF0SXhDb1NwWHFsd0MxRU9XSDZkT3VJUGMvdTF3YjRpR3d3dUVIVndnY1J2QXI0WHp0TUhBYVExVlJaSWtNV2NtRjJvbGNDNnVFZWJWSjFBZS9XcXlRZ0dzYlpTaHJkTk1qMFNrMnY4aHRaaXRBa0lKK3ZuZU1keHoyRWVYa01BRG41c20xMklGTGs4eGEyZkFocUpUSkkraDlOOTU3RmI0Y0FWbWRWU1hnTUx1UmozSjBEdEVCcGdwQ0JqMm50MUZRT0hQQitiMnNFREZrVm5EWk5NbTUxaU5uMEtUeE1LeHdvSlBHcE1qS0RzV3o0SW9KSlpBU2dDcnBwNmNXZHZ5ZjlGNUxXT1VtYW5MMVFpWG9BMkVNUGhTZVZpamxFZ2tkclVIakxGV0F4RzU4c213NWQ5c2RwNE1TcXNxNHovRSt5bWhyOFE1WHh1Vkl3cmxMNXlxckxGS2FidTNDdjdPNjhMY2t3dVRadmgzVE9IVWpMVXp3TTVrYjZwU3dkOFJkVzdVbVYwdXlib1pJSm44dTlBUXFBVDRiYUErcDgwVU16czFYZnQ1ZkRndC8wTGowSFJvdzhlemV4d1k2bmdFZGhaRUlRY0VRYW53cWhCWkJQWkozQmFodTZHbGxrbjdoNnJucGt5WFVQTEVINTN0a1NyZU4rT2ZwOG5XY2pZWXk0SDI1MVFqWWJIQkZpR3lNWVhiOTJicXNrb1dBYTdyMWQxN0ZhRThaU3UyMlU4Z2F2WUxLaUEwcVU2cFpBS3Z3MlR6eGFINTA3VEMzQW1PN25leFV2MWFzTCtKVmRGWXl0SzRzUlRQVG80Z0dsZ1Q2TEsrZEdEWkFZMGVGQjd1T3RJbzhMQXU1NlpnMHhnQ3grQT09In1dfQ==\", \"x-amz-signature\": \"064d1a7a0b737a6aa2e7ad4fab4010bfa2ca88dd3d64bb1bc6ec5a76008292a7\"}}"
    
    # File to upload (update this path to your actual file)
    file_path = "resume.pdf"
    
    # Check if file path was provided as command line argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    
    print(f"Uploading file: {file_path}\n")
    
    # Upload the file
    success = upload_file_to_s3(upload_url, file_path)
    
    if success:
        print("\n✓ Upload completed successfully!")
        sys.exit(0)
    else:
        print("\n✗ Upload failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
