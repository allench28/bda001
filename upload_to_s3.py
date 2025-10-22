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
    upload_url = "{\"url\": \"https://litedemo-documents-bucket-us-east-1.s3.amazonaws.com/\", \"fields\": {\"key\": \"input/bbc4b3f0-a72f-499b-8898-974389d32c57/lite-document-testing.pdf\", \"x-amz-algorithm\": \"AWS4-HMAC-SHA256\", \"x-amz-credential\": \"ASIA54WNANFRTS6MXHIS/20251022/us-east-1/s3/aws4_request\", \"x-amz-date\": \"20251022T052801Z\", \"x-amz-security-token\": \"IQoJb3JpZ2luX2VjEG4aCXVzLWVhc3QtMSJHMEUCIQDRYXQ7W1Lzy8a+T3eAGMYfzaaL/7+5j6/Db/ejtk+pigIgC9j06hdNe4+C2K+STdsXsLjCIJAdpkWNecIpQ4Wqd3QqoQMIJxAAGgw5NTQ5ODY0MjQ2NzUiDAYsr1/QGEirQKn/xir+At6HDSEGllZwKX6tnWvpY13qzYgG+FAh7kNhhdUFSmwsQkHGfnlimcg28SoC9hZldkM7s4jNg0UeNDh8dHoG/7QpBRAlxUPuFPQhXgSvSVKl1anh7xkHKy1RBgXhpmvNLgQb0cplLuzU/cXROazKZ9iyY0ZXPUgF+a7Z5aAsVnQl15D+/47/ug+h5OWPk49zjckZ3PxXqtmFY2j8cWBpdmqgUc7F8RnIr2acf1WD2IhzMkU4ytwB8vfmEnaiOrfxll4FazFbWVJQuCSD/uWjMXiCmNCvLip0EY6sz18ezCV0FwIfO1NcuJ1EdlXMqgxTpxgVhSxLjpwE7UOs32WaNPmZu9SsswFocKDmcUZUm3nBiN2VwIBE7BxVWgmXcx4Yfz7oCujYV6tIsTIRr46zfq9H2UBFwJoi/b7I8vVsrFh+GizYbe/dWd7T3kR3uXbD1hiVOluLvSwNzeO84tW/O/+WVduhudSBkt2+dDWdpVDxNBvCbb5JeEmC/h9Y2Qww39bhxwY6nQEjAdy7Aozh2mMZFTe38VzHMpRUKgkcQ6a2341R56vCsWjwUdZvhA5Ig4rrFQLhKtQSZ3GRVi4t3Om2u5WKD7nSEMDLNKEDdEUwdL7zfGVUS551/e8nl5NSRZUjXQ2XeuCXPo6Ib9K7IcNAVaqtw+S56hEWBXrpoGX1lIsxqLJ/6oxZURToz5yfE9DY8kS22HSeI1/kb2GUd7LAxxbo\", \"policy\": \"eyJleHBpcmF0aW9uIjogIjIwMjUtMTAtMjJUMDY6Mjg6MDFaIiwgImNvbmRpdGlvbnMiOiBbWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsIDAsIDE1NzI4NjQwXSwgeyJidWNrZXQiOiAibGl0ZWRlbW8tZG9jdW1lbnRzLWJ1Y2tldC11cy1lYXN0LTEifSwgeyJrZXkiOiAiaW5wdXQvYmJjNGIzZjAtYTcyZi00OTliLTg4OTgtOTc0Mzg5ZDMyYzU3L2xpdGUtZG9jdW1lbnQtdGVzdGluZy5wZGYifSwgeyJ4LWFtei1hbGdvcml0aG0iOiAiQVdTNC1ITUFDLVNIQTI1NiJ9LCB7IngtYW16LWNyZWRlbnRpYWwiOiAiQVNJQTU0V05BTkZSVFM2TVhISVMvMjAyNTEwMjIvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWRhdGUiOiAiMjAyNTEwMjJUMDUyODAxWiJ9LCB7IngtYW16LXNlY3VyaXR5LXRva2VuIjogIklRb0piM0pwWjJsdVgyVmpFRzRhQ1hWekxXVmhjM1F0TVNKSE1FVUNJUURSWVhRN1cxTHp5OGErVDNlQUdNWWZ6YWFMLzcrNWo2L0RiL2VqdGsrcGlnSWdDOWowNmhkTmU0K0MySytTVGRzWHNMakNJSkFkcGtXTmVjSXBRNFdxZDNRcW9RTUlKeEFBR2d3NU5UUTVPRFkwTWpRMk56VWlEQVlzcjEvUUdFaXJRS24veGlyK0F0NkhEU0VHbGxad0tYNnRuV3ZwWTEzcXpZZ0crRkFoN2tOaGhkVUZTbXdzUWtIR2ZubGltY2cyOFNvQzloWmxka003czRqTmcwVWVORGg4ZEhvRy83UXBCUkFseFVQdUZQUWhYZ1N2U1ZLbDFhbmg3eGtIS3kxUkJnWGhwbXZOTGdRYjBjcGxMdXpVL2NYUk9hektaOWl5WTBaWFBVZ0YrYTdaNWFBc1ZuUWwxNUQrLzQ3L3VnK2g1T1dQazQ5empja1ozUHhYcXRtRlkyajhjV0JwZG1xZ1VjN0Y4Um5JcjJhY2YxV0QySWh6TWtVNHl0d0I4dmZtRW5haU9yZnhsbDRGYXpGYldWSlF1Q1NEL3VXak1YaUNtTkN2TGlwMEVZNnN6MThlekNWMEZ3SWZPMU5jdUoxRWRsWE1xZ3hUcHhnVmhTeExqcHdFN1VPczMyV2FOUG1adTlTc3N3Rm9jS0RtY1VaVW0zbkJpTjJWd0lCRTdCeFZXZ21YY3g0WWZ6N29DdWpZVjZ0SXNUSVJyNDZ6ZnE5SDJVQkZ3Sm9pL2I3STh2VnNyRmgrR2l6WWJlL2RXZDdUM2tSM3VYYkQxaGlWT2x1THZTd056ZU84NHRXL08vK1dWZHVodWRTQmt0MitkRFdkcFZEeE5CdkNiYjVKZUVtQy9oOVkyUXd3MzliaHh3WTZuUUVqQWR5N0FvemgybU1aRlRlMzhWekhNcFJVS2drY1E2YTIzNDFSNTZ2Q3NXandVZFp2aEE1SWc0cnJGUUxoS3RRU1ozR1JWaTR0M09tMnU1V0tEN25TRU1ETE5LRURkRVV3ZEw3emZHVlVTNTUxL2U4bmw1TlNSWlVqWFEyWGV1Q1hQbzZJYjlLN0ljTkFWYXF0dytTNTZoRVdCWHJwb0dYMWxJc3hxTEovNm94WlVSVG96NXlmRTlEWThrUzIySFNlSTEva2IyR1VkN0xBeHhibyJ9XX0=\", \"x-amz-signature\": \"80fad43f7f4296c6e51f5d6a14ff524bdbb24929f90fa00cfaecf2d08d989654\"}}"
    
    # File to upload (update this path to your actual file)
    file_path = "pic_control.pdf"
    
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
