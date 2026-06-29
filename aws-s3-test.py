import os
from dotenv import load_dotenv
import boto3

load_dotenv()

bucket_name = os.getenv("S3_BUCKET")
region = os.getenv("AWS_REGION")

s3_client = boto3.client(
    "s3",
    region_name=region,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

test_key = "connection_test/test.txt"
test_content = b"Connection with S3 is working."

s3_client.put_object(
    Bucket=bucket_name,
    Key=test_key,
    Body=test_content,
)

response = s3_client.get_object(
    Bucket=bucket_name,
    Key=test_key,
)

content = response["Body"].read().decode("utf-8")

print(f"Bucket: {bucket_name}")
print(f"Region: {region}")
print(f"Object key: {test_key}")
print(f"Content: {content}")
print("S3 connection test completed successfully.")