import logging
from botocore.exceptions import ClientError
from helpers.resource_management import get_s3_client, ensure_resource, handle_existing_item
from helpers.config import get_config
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import boto3.s3.transfer
from botocore.config import Config
import queue
import threading
from stream_unzip import stream_unzip

logger = logging.getLogger(__name__)

# Get the configuration
config = get_config()
s3_config = config['s3']
processing_config = config.get('processing', {})

def get_transfer_config(max_concurrency=None, multipart_threshold=None, multipart_chunksize=None):
    return boto3.s3.transfer.TransferConfig(
        multipart_threshold=multipart_threshold or processing_config.get('multipart_threshold', 8 * 1024 * 1024),
        max_concurrency=max_concurrency or processing_config.get('max_concurrency', 100),
        multipart_chunksize=multipart_chunksize or processing_config.get('multipart_chunksize', 8 * 1024 * 1024),
        use_threads=True
    )

def get_optimized_s3_client():
    config = Config(
        retries={'max_attempts': 10, 'mode': 'adaptive'},
        max_pool_connections=processing_config.get('max_pool_connections', 500)
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
def stream_download_from_s3(s3_client, bucket, key, chunk_size=None):
    if chunk_size is None:
        chunk_size = processing_config.get('chunk_size', 67108864)  # Default to 64MB if not specified
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        for chunk in response['Body'].iter_chunks(chunk_size=chunk_size):
            yield chunk
    except ClientError as e:
        logger.error(f"Error streaming from s3://{bucket}/{key}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error streaming from s3://{bucket}/{key}: {str(e)}")
        raise

def bulk_upload_to_s3(s3_client, batch, bucket, max_workers=None):
    if max_workers is None:
        max_workers = processing_config.get('max_concurrency', 64)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(upload_single_file, s3_client, file_name, file_content, bucket)
                   for file_name, file_content in batch]
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error in bulk upload: {str(e)}")
                results.append((str(e), False))

    return results

def upload_single_file(s3_client, file_name, file_content, bucket):
    try:
        s3_client.put_object(Body=file_content, Bucket=bucket, Key=file_name)
        logger.info(f"Uploaded: {file_name}")
        return (file_name, True)
    except Exception as e:
        logger.error(f"Error uploading file {file_name} to s3://{bucket}: {str(e)}")
        return (file_name, False)

def stream_unzip_and_upload(s3_client, source_bucket, source_key, dest_bucket, dest_prefix,
                            batch_size=1000, max_workers=64, concurrent_batches=8, unzip_password=None):
    batch_queue = queue.Queue(maxsize=concurrent_batches * 2)
    upload_queue = queue.Queue(maxsize=concurrent_batches * 2)

    def process_batches():
        while True:
            batch = batch_queue.get()
            if batch is None:
                break
            upload_queue.put(batch)
            batch_queue.task_done()

    def upload_batches():
        while True:
            batch = upload_queue.get()
            if batch is None:
                break
            bulk_upload_to_s3(s3_client, batch, dest_bucket, max_workers)
            upload_queue.task_done()

    # Start batch processing thread
    batch_thread = threading.Thread(target=process_batches)
    batch_thread.start()

    # Start upload threads
    upload_threads = []
    for _ in range(concurrent_batches):
        t = threading.Thread(target=upload_batches)
        t.start()
        upload_threads.append(t)

    current_batch = []
    total_files = 0

    try:
        for chunk in stream_download_from_s3(s3_client, source_bucket, source_key):
            for file_name, file_size, unzipped_chunks in stream_unzip(chunk, password=unzip_password):
                if isinstance(file_name, bytes):
                    file_name = file_name.decode('utf-8')
                dest_key = f"{dest_prefix}/{file_name}"

                current_batch.append((dest_key, b''.join(unzipped_chunks)))
                total_files += 1

                if len(current_batch) >= batch_size:
                    batch_queue.put(current_batch)
                    current_batch = []

        if current_batch:
            batch_queue.put(current_batch)
    except Exception as e:
        logger.error(f"Error in stream_unzip_and_upload: {str(e)}")
    finally:
        # Signal the end of processing
        batch_queue.put(None)
        batch_thread.join()

        # Signal the end of batches
        for _ in range(concurrent_batches):
            upload_queue.put(None)

        # Wait for all upload threads to complete
        for t in upload_threads:
            t.join()

    return total_files

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

def get_s3_path(dataset_name, file_type='data', file_index=None):
    if file_type == 'data':
        base_path = f"{s3_config['data_prefix']}/{dataset_name}"
        return f"{base_path}_{file_index}.zip" if file_index is not None else f"{base_path}.zip"
    elif file_type == 'processed':
        return f"{s3_config['processed_folder']}/{dataset_name}"
    else:
        raise ValueError(f"Invalid file_type: {file_type}")

@ensure_resource('s3')
def get_s3_object_size(bucket, s3_key):
    try:
        s3_client = get_optimized_s3_client()
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        return response['ContentLength']
    except ClientError as e:
        logger.error(f"Error getting size for s3://{bucket}/{s3_key}: {str(e)}")
        return None

__all__ = ['upload_to_s3', 'download_from_s3', 'stream_download_from_s3', 'bulk_upload_to_s3',
           'stream_unzip_and_upload', 'check_s3_object_exists', 'get_s3_object_info',
           'check_s3_prefix_exists', 'list_s3_objects', 'copy_s3_object', 'get_s3_path',
           'get_s3_object_size']
