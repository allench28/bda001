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
    upload_url = "{\"url\": \"https://litedemo-documents-bucket-ap-southeast-1.s3.ap-southeast-1.amazonaws.com/\", \"fields\": {\"key\": \"input/d36b3075-6b71-476f-94e3-ffe6fcc26e77/lite-document-testing.pdf\", \"x-amz-algorithm\": \"AWS4-HMAC-SHA256\", \"x-amz-credential\": \"ASIA54WNANFRXTUASMU6/20251021/ap-southeast-1/s3/aws4_request\", \"x-amz-date\": \"20251021T104320Z\", \"x-amz-security-token\": \"IQoJb3JpZ2luX2VjEFsaDmFwLXNvdXRoZWFzdC0xIkgwRgIhAMq36B9RcO3sp1VIURKWRQIzXF0koxX+tQonQhBPRme4AiEAidQ1PIxSywrxUWB2J59UXJNd+QchygSaLmN/88gH2ykqpgMIFBAAGgw5NTQ5ODY0MjQ2NzUiDH67v08J1XLXAMf6pyqDAyO14h0JxvnlwlZuuEGUF7i0QJ3xn1IyH1gnXbho+k/jSBK0SQy/uGmULpQUbdMmQFaCb5PO7+qhc8SE0WMhwRPrMcN496caDKqqkfwTXxa5iUXRX+z5544bGfeIN7xIOE6DK6Rntqw7oPqHd5sKdYmINa63xfpHMvJSv2UofB40ovlzyroR+me7B9FAMzntDNi07wmsLsCkVbk2z17ujkeXOndHAsSQ+T6WbLbOBB7ITQOndkEM5RrrXDtfLxL7qHnyYRMb82jwJ6JGQspJigaNNdZwC+M2RoQBVWkNq6yaSmYYlE3zlPNw7jLvSRapl9UZUhMHVUVZZDdIGaYYf1ZCelbTMEevbQQILnGFg3yDhbpD2nXC3Ofb2sKUMP7sv9wbs4Y7/xK8pMqn77ZmMpX2WOYoC4sSIUZ4/em9jUuKDDr5JOzPaWLBK3a4ybrUVC1Sgzs8jgLHZrXC8y37x+Y4e9L0SN8RuwVER8JBvZUykvlAvdZ1AiTrVmIJa95tgZZ68jDGx93HBjqcAWA2nMSCk08IdPX4hd5VlmLdiDG7WKdPqmeRQ/mfVPUfQBIfSu52VR4/3AJ7d84QRG7L3KXtjydE3MrooRtPY9bNKjXS2KjMn6MFvvgGd/fiKFE9r3nLkRt3NEUsskGgbNAgsvtQKfKg+41lf7XzqM99UY60X2yjwkdCwpVQOsNF3H6b2l9Oh4vuNpLJ9v0cDLxvix6SbyMMI9P1hQ==\", \"policy\": \"eyJleHBpcmF0aW9uIjogIjIwMjUtMTAtMjFUMTE6NDM6MjBaIiwgImNvbmRpdGlvbnMiOiBbWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsIDAsIDE1NzI4NjQwXSwgeyJidWNrZXQiOiAibGl0ZWRlbW8tZG9jdW1lbnRzLWJ1Y2tldC1hcC1zb3V0aGVhc3QtMSJ9LCB7ImtleSI6ICJpbnB1dC9kMzZiMzA3NS02YjcxLTQ3NmYtOTRlMy1mZmU2ZmNjMjZlNzcvbGl0ZS1kb2N1bWVudC10ZXN0aW5nLnBkZiJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotY3JlZGVudGlhbCI6ICJBU0lBNTRXTkFORlJYVFVBU01VNi8yMDI1MTAyMS9hcC1zb3V0aGVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSwgeyJ4LWFtei1kYXRlIjogIjIwMjUxMDIxVDEwNDMyMFoifSwgeyJ4LWFtei1zZWN1cml0eS10b2tlbiI6ICJJUW9KYjNKcFoybHVYMlZqRUZzYURtRndMWE52ZFhSb1pXRnpkQzB4SWtnd1JnSWhBTXEzNkI5UmNPM3NwMVZJVVJLV1JRSXpYRjBrb3hYK3RRb25RaEJQUm1lNEFpRUFpZFExUEl4U3l3cnhVV0IySjU5VVhKTmQrUWNoeWdTYUxtTi84OGdIMnlrcXBnTUlGQkFBR2d3NU5UUTVPRFkwTWpRMk56VWlESDY3djA4SjFYTFhBTWY2cHlxREF5TzE0aDBKeHZubHdsWnV1RUdVRjdpMFFKM3huMUl5SDFnblhiaG8ray9qU0JLMFNReS91R21VTHBRVWJkTW1RRmFDYjVQTzcrcWhjOFNFMFdNaHdSUHJNY040OTZjYURLcXFrZndUWHhhNWlVWFJYK3o1NTQ0YkdmZUlON3hJT0U2REs2Um50cXc3b1BxSGQ1c0tkWW1JTmE2M3hmcEhNdkpTdjJVb2ZCNDBvdmx6eXJvUittZTdCOUZBTXpudEROaTA3d21zTHNDa1ZiazJ6MTd1amtlWE9uZEhBc1NRK1Q2V2JMYk9CQjdJVFFPbmRrRU01UnJyWER0Zkx4TDdxSG55WVJNYjgyandKNkpHUXNwSmlnYU5OZFp3QytNMlJvUUJWV2tOcTZ5YVNtWVlsRTN6bFBOdzdqTHZTUmFwbDlVWlVoTUhWVVZaWkRkSUdhWVlmMVpDZWxiVE1FZXZiUVFJTG5HRmczeURoYnBEMm5YQzNPZmIyc0tVTVA3c3Y5d2JzNFk3L3hLOHBNcW43N1ptTXBYMldPWW9DNHNTSVVaNC9lbTlqVXVLRERyNUpPelBhV0xCSzNhNHliclVWQzFTZ3pzOGpnTEhaclhDOHkzN3grWTRlOUwwU044UnV3VkVSOEpCdlpVeWt2bEF2ZFoxQWlUclZtSUphOTV0Z1paNjhqREd4OTNIQmpxY0FXQTJuTVNDazA4SWRQWDRoZDVWbG1MZGlERzdXS2RQcW1lUlEvbWZWUFVmUUJJZlN1NTJWUjQvM0FKN2Q4NFFSRzdMM0tYdGp5ZEUzTXJvb1J0UFk5Yk5LalhTMktqTW42TUZ2dmdHZC9maUtGRTlyM25Ma1J0M05FVXNza0dnYk5BZ3N2dFFLZktnKzQxbGY3WHpxTTk5VVk2MFgyeWp3a2RDd3BWUU9zTkYzSDZiMmw5T2g0dnVOcExKOXYwY0RMeHZpeDZTYnlNTUk5UDFoUT09In1dfQ==\", \"x-amz-signature\": \"5453dfc5526738c2039e6a26d9d2ed8bfd0ebf91c06481e06eeb22e07a9952e2\"}}"
    
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
