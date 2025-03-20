from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import pandas as pd
from io import StringIO, BytesIO
import re  # For wildcard support
from typing import List, Dict, Union, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache for AWS clients
AWS_CLIENTS = {}

def get_aws_client(client_type: str):
    """
    Get or create an AWS client using Airflow's AwsBaseHook.
    """
    if client_type not in AWS_CLIENTS:
        logger.info(f"Creating new AWS client for {client_type}.")
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type=client_type)
        AWS_CLIENTS[client_type] = aws_hook.get_client_type()
    return AWS_CLIENTS[client_type]

def fetch_s3_file_keys(bucket_name: str, prefix: str = None, wildcard: str = None) -> List[str]:
    """
    Fetch keys of files in an S3 bucket, optionally filtered by prefix and/or wildcard.
    """
    s3_client = get_aws_client("s3")
    keys = []
    continuation_token = None
    logger.info(f"Fetching S3 file keys from bucket: {bucket_name}, prefix: {prefix}, wildcard: {wildcard}")
    try:
        while True:
            list_kwargs = {"Bucket": bucket_name}
            if prefix:
                list_kwargs["Prefix"] = prefix
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**list_kwargs)
            
            if "Contents" in response:
                for obj in response["Contents"]:
                    key = obj["Key"]
                    if wildcard and not re.match(wildcard.replace("*", ".*"), key):
                        continue
                    keys.append(key)
            
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
    except Exception as e:
        logger.error(f"Failed to fetch S3 file keys: {e}")
        raise

    logger.info(f"Fetched {len(keys)} file keys from S3.")
    return keys

def fetch_s3_file(bucket_name: str, key: str) -> Dict:
    """
    Fetch a single file from S3.
    """
    s3_client = get_aws_client("s3")
    logger.info(f"Fetching file from S3: s3://{bucket_name}/{key}")
    try:
        file = s3_client.get_object(Bucket=bucket_name, Key=key)
        return file
    except Exception as e:
        logger.error(f"Failed to fetch S3 file {key}: {e}")
        raise

def fetch_s3_files(bucket_name: str, prefix: Optional[str] = None, wildcard: Optional[str] = None, keys: Optional[List[str]] = None) -> Dict[str, Dict]:
    """
    Fetch multiple files from S3.
    """
    logger.info("Fetching multiple files from S3.")
    try:
        if keys:
            logger.info(f"Fetching specific keys: {keys}")
            return {key: fetch_s3_file(bucket_name, key) for key in keys}
        
        keys = fetch_s3_file_keys(bucket_name, prefix, wildcard)
        files = {key: fetch_s3_file(bucket_name, key) for key in keys}
        logger.info(f"Fetched {len(files)} files from S3.")
        return files
    except Exception as e:
        logger.error(f"Failed to fetch S3 files: {e}")
        raise

def upload_df_to_s3_as_csv(df: pd.DataFrame, bucket_name: str, key: str) -> bool:
    """
    Upload a Pandas DataFrame to S3 as a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        bucket_name (str): The name of the S3 bucket.
        key (str): The S3 key (path) where the CSV file will be stored.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    s3_client = get_aws_client("s3")
    logger.info(f"Uploading DataFrame to s3://{bucket_name}/{key}")
    try:
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload the CSV string to S3
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"DataFrame uploaded successfully to s3://{bucket_name}/{key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload DataFrame to S3: {e}")
        return False

def upload_file_to_s3(bucket_name: str, file_path: str, key: str) -> bool:
    """
    Upload a single file to S3.
    """
    s3_client = get_aws_client("s3")
    logger.info(f"Uploading file {file_path} to s3://{bucket_name}/{key}")
    try:
        s3_client.upload_file(file_path, bucket_name, key)
        logger.info("File uploaded successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to upload file to S3: {e}")
        return False

def move_s3_files_by_key(source_bucket: str, source_keys: List[str], destination_bucket: str, destination_prefix: str) -> bool:
    """
    Move specific files from one S3 location to another by specifying their keys.
    """
    s3_client = get_aws_client("s3")
    logger.info(f"Moving {len(source_keys)} files from {source_bucket} to {destination_bucket}/{destination_prefix}")
    try:
        for source_key in source_keys:
            destination_key = f"{destination_prefix}{source_key.split('/')[-1]}"
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
            s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            logger.info(f"Moved {source_key} to {destination_key}.")
        return True
    except Exception as e:
        logger.error(f"Failed to move files: {e}")
        return False

def delete_s3_files_by_key(bucket_name: str, keys: List[str]) -> bool:
    """
    Delete specified files from an S3 bucket.
    """
    s3_client = get_aws_client("s3")
    logger.info(f"Deleting {len(keys)} files from {bucket_name}")
    try:
        for key in keys:
            s3_client.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"Deleted {key} from S3.")
        return True
    except Exception as e:
        logger.error(f"Failed to delete files: {e}")
        return False
