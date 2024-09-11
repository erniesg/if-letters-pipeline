import logging
from botocore.exceptions import ClientError
from helpers.resource_management import get_s3_client, ensure_resource, handle_existing_item
from helpers.config import get_config
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import boto3.s3.transfer
from botocore.config import Config

logger = logging.getLogger(__name__)

# Get the configuration
config = get_config()
s3_config = config['s3']
processing_config = config.get('processing', {})

# Update the transfer_config to use the new settings
def get_transfer_config(max_concurrency=None, multipart_threshold=None, multipart_chunksize=None):
    return boto3.s3.transfer.TransferConfig(
        multipart_threshold=multipart_threshold or processing_config.get('multipart_threshold', 16 * 1024 * 1024),
        max_concurrency=max_concurrency or processing_config.get('max_concurrency', 64),
        multipart_chunksize=multipart_chunksize or processing_config.get('multipart_chunksize', 16 * 1024 * 1024),
        use_threads=True
    )

def get_optimized_s3_client():
    config = Config(
        retries={'max_attempts': 5, 'mode': 'adaptive'},
        max_pool_connections=processing_config.get('max_pool_connections', 200)
    )
    return boto3.client('s3', config=config)

@ensure_resource('s3')
@handle_existing_item
def upload_to_s3(content, bucket, s3_key):
    try:
        s3_client = get_optimized_s3_client()
        transfer_config = get_transfer_config()
        s3_client.upload_fileobj(BytesIO(content), bucket, s3_key, Config=transfer_config)
        logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
    except ClientError as e:
        logger.error(f"Error uploading to s3://{bucket}/{s3_key}: {str(e)}")
        raise

@ensure_resource('s3')
def batch_upload_to_s3(file_list, bucket, max_workers=None, multipart_threshold=None, multipart_chunksize=None):
    if max_workers is None:
        max_workers = processing_config.get('max_concurrency', 64)

    s3_client = get_optimized_s3_client()
    transfer_config = get_transfer_config(max_workers, multipart_threshold, multipart_chunksize)

    def upload_file(file_info):
        content, s3_key = file_info
        try:
            s3_client.upload_fileobj(BytesIO(content), bucket, s3_key, Config=transfer_config)
            logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
            return s3_key, True
        except ClientError as e:
            logger.error(f"Error uploading to s3://{bucket}/{s3_key}: {str(e)}")
            return s3_key, False

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {executor.submit(upload_file, file_info): file_info[1] for file_info in file_list}
        for future in as_completed(future_to_key):
            s3_key = future_to_key[future]
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Unexpected error uploading {s3_key}: {str(e)}")
                results.append((s3_key, False))

    return results

@ensure_resource('s3')
@handle_existing_item
def download_from_s3(bucket, s3_key):
    try:
        s3_client = get_optimized_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        logger.info(f"Successfully downloaded from s3://{bucket}/{s3_key}")
        return response['Body'].read()
    except ClientError as e:
        logger.error(f"Error downloading from s3://{bucket}/{s3_key}: {str(e)}")
        return None

@ensure_resource('s3')
@handle_existing_item
def check_s3_object_exists(bucket, s3_key):
    try:
        s3_client = get_optimized_s3_client()
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logger.error(f"Error checking s3://{bucket}/{s3_key}: {str(e)}")
            return False

@ensure_resource('s3')
@handle_existing_item
def get_s3_object_info(bucket, s3_key):
    try:
        s3_client = get_optimized_s3_client()
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        return {
            'ContentLength': response.get('ContentLength', 0),
            'ContentType': response.get('ContentType', 'Unknown')
        }
    except ClientError as e:
        logger.error(f"Error getting info for s3://{bucket}/{s3_key}: {str(e)}")
        return None

@ensure_resource('s3')
@handle_existing_item
def check_s3_prefix_exists(bucket, prefix):
    try:
        s3_client = get_optimized_s3_client()
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response
    except ClientError as e:
        logger.error(f"Error checking prefix s3://{bucket}/{prefix}: {str(e)}")
        return False

@ensure_resource('s3')
@handle_existing_item
def list_s3_objects(bucket, prefix):
    try:
        s3_client = get_optimized_s3_client()
        paginator = s3_client.get_paginator('list_objects_v2')
        objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects.extend(page.get('Contents', []))
        return objects
    except ClientError as e:
        logger.error(f"Error listing objects in s3://{bucket}/{prefix}: {str(e)}")
        return []

@ensure_resource('s3')
@handle_existing_item
def copy_s3_object(source_bucket, source_key, dest_bucket, dest_key):
    s3_client = get_optimized_s3_client()
    try:
        s3_client.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=dest_bucket,
            Key=dest_key
        )
        logger.info(f"Successfully copied s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}")
    except ClientError as e:
        logger.error(f"Error copying s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}: {str(e)}")
        raise

# Add this new function at the beginning of the file, after the imports and config setup
def get_s3_path(dataset_name, file_type='data', file_index=None):
    if file_type == 'data':
        base_path = f"{s3_config['data_prefix']}/{dataset_name}"
        return f"{base_path}_{file_index}.zip" if file_index is not None else f"{base_path}.zip"
    elif file_type == 'processed':
        return f"{s3_config['processed_folder']}/{dataset_name}"
    else:
        raise ValueError(f"Invalid file_type: {file_type}")

# Update the stream_download_from_s3 function
@ensure_resource('s3')
@handle_existing_item
def stream_download_from_s3(s3_client, bucket, key, chunk_size=None):
    if chunk_size is None:
        chunk_size = processing_config.get('chunk_size', 128 * 1024 * 1024)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    for chunk in response['Body'].iter_chunks(chunk_size=chunk_size):
        yield chunk

def multipart_upload_to_s3(s3_client, file_content, bucket, s3_key):
    transfer_config = get_transfer_config()
    try:
        s3_client.upload_fileobj(BytesIO(file_content), bucket, s3_key, Config=transfer_config)
        logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Error uploading to s3://{bucket}/{s3_key}: {str(e)}")
        return False

__all__ = ['upload_to_s3', 'batch_upload_to_s3', 'download_from_s3', 'check_s3_object_exists',
           'get_s3_object_info', 'check_s3_prefix_exists', 'list_s3_objects',
           'copy_s3_object', 'stream_download_from_s3', 'multipart_upload_to_s3', 'get_s3_path']
