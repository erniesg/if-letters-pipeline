import logging
from botocore.exceptions import ClientError
from helpers.resource_management import get_s3_client, ensure_resource, handle_existing_item
from helpers.config import get_config
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

# Get the configuration
config = get_config()
s3_config = config['s3']
processing_config = config.get('processing', {})

# S3 transfer config for multipart uploads
transfer_config = boto3.s3.transfer.TransferConfig(
    multipart_threshold=processing_config.get('multipart_threshold', 8 * 1024 * 1024),  # 8MB
    max_concurrency=processing_config.get('max_concurrency', 10),
    multipart_chunksize=processing_config.get('multipart_chunksize', 8 * 1024 * 1024),  # 8MB
    use_threads=True
)

def get_optimized_s3_client():
    config = Config(
        retries={'max_attempts': 3, 'mode': 'adaptive'},
        max_pool_connections=processing_config.get('max_pool_connections', 50)
    )
    return get_s3_client(config=config)

@ensure_resource('s3')
@handle_existing_item
def upload_to_s3(content, bucket, s3_key):
    try:
        s3_client = get_optimized_s3_client()
        s3_client.upload_fileobj(BytesIO(content), bucket, s3_key, Config=transfer_config)
        logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
    except ClientError as e:
        logger.error(f"Error uploading to s3://{bucket}/{s3_key}: {str(e)}")
        raise

@ensure_resource('s3')
def batch_upload_to_s3(file_list, bucket, max_workers=None):
    if max_workers is None:
        max_workers = processing_config.get('max_concurrency', 16)

    s3_client = get_optimized_s3_client()

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
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return response.get('Contents', [])
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

__all__ = ['upload_to_s3', 'batch_upload_to_s3', 'download_from_s3', 'check_s3_object_exists',
           'get_s3_object_info', 'check_s3_prefix_exists', 'list_s3_objects',
           'copy_s3_object']
