import boto3
from stream_unzip import stream_unzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from airflow.models import BaseOperator
from helpers.s3 import stream_download_from_s3, multipart_upload_to_s3, get_optimized_s3_client, check_s3_prefix_exists, list_s3_objects
from helpers.dynamodb import update_job_state
from helpers.config import get_config
import logging
import time
import json

logger = logging.getLogger(__name__)

class BufferPool:
    def __init__(self, pool_size, buffer_size):
        self.pool = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self.pool.put(bytearray(buffer_size))

    def get(self):
        return self.pool.get()

    def put(self, buffer):
        self.pool.put(buffer)

class StreamingZipProcessor:
    def __init__(self, s3_client, source_bucket, source_key, dest_bucket, dest_prefix, buffer_pool, unzip_password=None):
        self.s3_client = s3_client
        self.source_bucket = source_bucket
        self.source_key = source_key
        self.dest_bucket = dest_bucket
        self.dest_prefix = dest_prefix
        self.buffer_pool = buffer_pool
        self.unzip_password = unzip_password
        logger.debug(f"StreamingZipProcessor initialized with source_bucket: {source_bucket}, source_key: {source_key}")

    def process(self):
        logger.debug("Starting StreamingZipProcessor.process()")
        try:
            for file_name, file_size, unzipped_chunks in stream_unzip(self.zipped_chunks(), password=self.unzip_password):
                logger.debug(f"Processing file: {file_name}, size: {file_size}")
                buffer = self.buffer_pool.get()
                buffer_view = memoryview(buffer)
                pos = 0
                for chunk in unzipped_chunks:
                    chunk_len = len(chunk)
                    buffer_view[pos:pos+chunk_len] = chunk
                    pos += chunk_len
                yield file_name, buffer_view[:pos]
        except Exception as e:
            logger.error(f"Error in StreamingZipProcessor.process(): {str(e)}")
            raise

    def zipped_chunks(self):
        logger.debug(f"Starting zipped_chunks for {self.source_bucket}/{self.source_key}")
        try:
            for chunk in stream_download_from_s3(self.s3_client, self.source_bucket, self.source_key):
                yield chunk
        except Exception as e:
            logger.error(f"Error in zipped_chunks: {str(e)}")
            raise

class UnzipOperator(BaseOperator):
    def __init__(
        self,
        dataset_name,
        s3_keys,
        s3_bucket,
        destination_prefix,
        max_concurrency=32,
        buffer_size=8*1024*1024,
        buffer_pool_size=64,
        max_pool_connections=None,
        batch_size=None,
        chunk_size=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.s3_keys = s3_keys if isinstance(s3_keys, list) else [s3_keys]
        self.s3_bucket = s3_bucket
        self.destination_prefix = destination_prefix
        self.config = get_config()
        self.unzip_password = self.config['datasets'][dataset_name].get('unzip_password')
        self.max_concurrency = max_concurrency
        self.buffer_size = buffer_size
        self.buffer_pool_size = buffer_pool_size
        self.max_pool_connections = max_pool_connections
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        self.error_files = []
        self.checkpoint_interval = 5000
        self.processed_files = 0
        self.total_files = 0
        logger.debug(f"UnzipOperator initialized for dataset: {dataset_name}")
        logger.debug(f"S3 bucket: {s3_bucket}")
        logger.debug(f"S3 keys: {self.s3_keys}")
        logger.debug(f"Destination prefix: {destination_prefix}")

    def save_checkpoint(self, job_id, data):
        s3_client = get_optimized_s3_client()
        checkpoint_key = f"{self.destination_prefix}/checkpoints/{job_id}.json"
        try:
            s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=checkpoint_key,
                Body=json.dumps(data)
            )
            logger.info(f"Checkpoint saved: {checkpoint_key}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")

    def load_checkpoint(self, job_id):
        s3_client = get_optimized_s3_client()
        checkpoint_key = f"{self.destination_prefix}/checkpoints/{job_id}.json"
        try:
            response = s3_client.get_object(Bucket=self.s3_bucket, Key=checkpoint_key)
            checkpoint_data = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Checkpoint loaded: {checkpoint_key}")
            return checkpoint_data
        except s3_client.exceptions.NoSuchKey:
            logger.info("No checkpoint found.")
            return None
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            return None

    def execute(self, context):
        job_id = f"{self.dataset_name}_unzip_{context['execution_date']}"
        logger.info(f"Starting UnzipOperator execution for job_id: {job_id}")
        logger.debug(f"S3 bucket: {self.s3_bucket}")
        logger.debug(f"Destination prefix: {self.destination_prefix}")
        s3_client = get_optimized_s3_client()
        buffer_pool = BufferPool(self.buffer_pool_size, self.buffer_size)

        try:
            if check_s3_prefix_exists(self.s3_bucket, self.destination_prefix):
                existing_files = list_s3_objects(self.s3_bucket, self.destination_prefix)
                logger.info(f"Files already exist in destination: {self.destination_prefix}")
                logger.info(f"Number of existing files: {len(existing_files)}")
                update_job_state(job_id, status='completed', progress=100, metadata={'existing_files': len(existing_files)})
                return

            update_job_state(job_id, status='in_progress', progress=0)
            logger.info(f"Starting streaming unzip operation for {len(self.s3_keys)} files")

            # Try to load checkpoint
            checkpoint = self.load_checkpoint(job_id)
            if checkpoint:
                self.processed_files = checkpoint['processed_files']
                self.total_files = checkpoint['total_files']
                logger.info(f"Resuming from checkpoint. Processed files: {self.processed_files}")

            start_time = time.time()

            for s3_key in self.s3_keys:
                self.process_single_file(s3_client, s3_key, buffer_pool, job_id, start_time)

            self.log_progress(job_id, start_time, force=True)

            if self.error_files:
                logger.warning(f"Encountered errors with {len(self.error_files)} files.")
                for error in self.error_files[:10]:  # Log first 10 errors
                    logger.error(f"Error in file {error['file']}: {error['error']}")

                update_job_state(job_id, status='completed_with_errors', progress=100,
                                 metadata={'processed_files': self.processed_files, 'total_files': self.total_files,
                                           'error_files': len(self.error_files), 'first_10_errors': self.error_files[:10]})
            else:
                update_job_state(job_id, status='completed', progress=100,
                                 metadata={'processed_files': self.processed_files, 'total_files': self.total_files})

            logger.info(f"Unzip operation completed. Processed {self.processed_files}/{self.total_files} files.")
            logger.info(f"Files uploaded to s3://{self.s3_bucket}/{self.destination_prefix}/")

            return {
                'processed_files': self.processed_files,
                'total_files': self.total_files,
                'error_files': len(self.error_files)
            }

        except Exception as e:
            logger.error(f"Unzip operation failed: {str(e)}")
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise

    def process_single_file(self, s3_client, s3_key, buffer_pool, job_id, start_time):
        logger.info(f"Processing file: s3://{self.s3_bucket}/{s3_key}")
        try:
            processor = StreamingZipProcessor(
                s3_client, self.s3_bucket, s3_key,
                self.s3_bucket, self.destination_prefix, buffer_pool, self.unzip_password
            )

            with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                future_to_file = {}
                for file_name, file_content in processor.process():
                    self.total_files += 1
                    s3_key = f"{self.destination_prefix}/{file_name}"
                    future = executor.submit(
                        multipart_upload_to_s3,
                        s3_client, file_content, self.s3_bucket, s3_key
                    )
                    future_to_file[future] = file_name

                    if self.total_files % 1000 == 0:
                        self.log_progress(job_id, start_time)

                for future in as_completed(future_to_file):
                    file_name = future_to_file[future]
                    try:
                        future.result()
                        self.processed_files += 1
                    except Exception as e:
                        logger.error(f"Error uploading file {file_name}: {str(e)}")
                        self.error_files.append({"file": file_name, "error": str(e)})

                    buffer_pool.put(future.result())

                    if self.processed_files % self.checkpoint_interval == 0:
                        self.save_checkpoint(job_id, {'processed_files': self.processed_files, 'total_files': self.total_files})

                    if self.processed_files % 1000 == 0:
                        self.log_progress(job_id, start_time)

        except Exception as e:
            logger.error(f"Error processing file {s3_key}: {str(e)}")
            self.error_files.append({"file": s3_key, "error": str(e)})

    def log_progress(self, job_id, start_time, force=False):
        current_time = time.time()
        elapsed_time = current_time - start_time
        files_per_second = self.processed_files / elapsed_time if elapsed_time > 0 else 0
        progress = (self.processed_files / self.total_files) * 100 if self.total_files > 0 else 0

        logger.info(f"Progress: {progress:.2f}% ({self.processed_files}/{self.total_files} files)")
        logger.info(f"Processing speed: {files_per_second:.2f} files/second")
        logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")

        if force or progress in [0, 5, 10, 25, 50, 75, 100]:
            update_job_state(job_id, status='in_progress', progress=progress,
                             metadata={'processed_files': self.processed_files, 'total_files': self.total_files,
                                       'files_per_second': files_per_second, 'elapsed_time': elapsed_time})
