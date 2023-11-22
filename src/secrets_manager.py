# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

import json
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from src.logging import get_logger

logger = get_logger(__name__)
USE_SECRET_MANAGER = os.environ.get("USE_SECRET_MANAGER", "True")
USE_SECRET_MANAGER = USE_SECRET_MANAGER.lower() == "true"
if USE_SECRET_MANAGER:
    logger.info("Using Secrets Manager")
else:
    logger.info("Using environment variables")


def _get_value_from_env(key: str, default_val: Optional[str] = None):
    val = os.environ.get(key, default_val)
    return val


def _get_value_from_secrets(key: str, default_val: Optional[str] = None):
    secret_name = os.environ.get("SECRET_NAME")
    secret_region = os.environ.get("SECRET_REGION")
    if None in [secret_name, secret_region]:
        raise Exception("Missing required environment variables for Secrets Manager")

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=secret_region)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]

    secret = json.loads(secret)
    return secret.get(key, default_val)


def get_secret(key: str, default_val: Optional[str] = None):
    if USE_SECRET_MANAGER:
        return _get_value_from_secrets(key, default_val)
    else:
        return _get_value_from_env(key, default_val)
