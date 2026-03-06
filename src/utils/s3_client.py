import boto3

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio12345",
        region_name="us-east-1",
    )
