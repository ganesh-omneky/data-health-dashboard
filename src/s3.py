import boto3


def read_html_from_s3(bucket_name: str, key: str) -> str:
    """
    Get a file from S3 and return the file path.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(key)
    return obj.get()["Body"].read().decode("utf-8")
