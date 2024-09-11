import boto3
from stream_unzip import stream_unzip
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Thread
from airflow.models import BaseOperator
from helpers.s3 import bulk_upload_to_s3, get_optimized_s3_client, stream_download_from_s3
from helpers.dynamodb import update_job_state
from helpers.config import get_config
import logging
import time
from decimal import Decimal
import traceback
from http.client import IncompleteRead as http_incompleteRead
from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class UnzipOperator(BaseOperator):
    def __init__(
        self,
        dataset_name,
        s3_keys,
        s3_bucket,
        destination_prefix,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.s3_keys = s3_keys if isinstance(s3_keys, list) else [s3_keys]
        self.s3_bucket = s3_bucket
        self.destination_prefix = destination_prefix

        # Get configuration
        self.config = get_config()
        dataset_config = self.config['datasets'].get(dataset_name, {})
        processing_config = self.config['processing']

        # Set parameters from config
        self.unzip_password = dataset_config.get('unzip_password')
        self.max_concurrency = dataset_config.get('max_concurrency', processing_config.get('max_concurrency', 64))
        self.batch_size = dataset_config.get('batch_size', processing_config.get('batch_size', 1000))
        self.concurrent_batches = dataset_config.get('concurrent_batches', processing_config.get('concurrent_batches', 8))
        self.chunk_size = processing_config.get('chunk_size', 268435456)  # 64MB default

        self.processed_files = 0
        self.total_files = 0
        self.error_files = []
        self.current_offset = 0
        self.retry_count = 0
        self.max_retries = 5

    def execute(self, context):
        job_id = f"{self.dataset_name}_unzip_{context['execution_date']}"
        logger.info(f"Starting UnzipOperator execution for job_id: {job_id}")
        s3_client = get_optimized_s3_client()

        try:
            update_job_state(job_id, status='in_progress', progress=0)
            logger.info(f"Starting streaming unzip operation for {len(self.s3_keys)} files")

            start_time = time.time()

            for s3_key in self.s3_keys:
                self.process_zip_file(s3_client, s3_key, job_id, start_time)

            self.log_progress(job_id, start_time, force=True)

            if self.error_files:
                logger.warning(f"Encountered errors with {len(self.error_files)} files.")
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
            logger.error(traceback.format_exc())
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise

    def process_zip_file(self, s3_client, s3_key, job_id, start_time):
        logger.info(f"Processing file: s3://{self.s3_bucket}/{s3_key}")

        batch_queue = Queue(maxsize=self.concurrent_batches)

        # Start the upload thread
        upload_thread = Thread(target=self.upload_batches, args=(s3_client, batch_queue, job_id, start_time))
        upload_thread.start()

        self.current_batch = []
        self.current_offset = 0
        self.retry_count = 0

        while self.retry_count < self.max_retries:
            try:
                for file_name, file_size, unzipped_chunks in stream_unzip(
                    self.stream_download_from_s3(s3_client, self.s3_bucket, s3_key, start_byte=self.current_offset),
                    password=self.unzip_password.encode('utf-8') if self.unzip_password else None
                ):
                    self.total_files += 1
                    if isinstance(file_name, bytes):
                        file_name = file_name.decode('utf-8')
                    dest_key = f"{self.destination_prefix}/{file_name}"

                    # Collect all chunks into a single bytes object
                    file_content = b''.join(unzipped_chunks)

                    self.current_batch.append((dest_key, file_content))

                    if len(self.current_batch) >= self.batch_size:
                        batch_queue.put(self.current_batch)
                        self.current_batch = []

                    if self.total_files % 10000 == 0:
                        self.log_progress(job_id, start_time)

                    # Update the current offset
                    self.current_offset += file_size

                # If we've made it here, we've successfully processed the entire file
                break

            except (http_incompleteRead, urllib3_incompleteRead, ClientError) as e:
                self.retry_count += 1
                wait_time = 2 ** self.retry_count
                logger.warning(f"Incomplete Read error: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            except Exception as e:
                logger.error(f"Error processing zip file {s3_key}: {str(e)}")
                logger.error(traceback.format_exc())
                self.error_files.append({"file": s3_key, "error": str(e)})
                break

        # Put any remaining files in the last batch
        if self.current_batch:
            batch_queue.put(self.current_batch)

        # Signal that we're done processing
        batch_queue.put(None)
        upload_thread.join()

    def stream_download_from_s3(self, s3_client, bucket, key, start_byte=0):
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key, Range=f'bytes={start_byte}-')
            for chunk in response['Body'].iter_chunks(chunk_size=self.chunk_size):
                yield chunk
        except ClientError as e:
            logger.error(f"Error streaming from s3://{bucket}/{key}: {str(e)}")
            raise

    def upload_batches(self, s3_client, batch_queue, job_id, start_time):
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            while True:
                batch = batch_queue.get()
                if batch is None:
                    break
                executor.submit(self.upload_batch, s3_client, batch, job_id, start_time)
                batch_queue.task_done()

    def upload_batch(self, s3_client, batch, job_id, start_time):
        try:
            bulk_upload_to_s3(s3_client, batch, self.s3_bucket)
            self.processed_files += len(batch)
            self.log_progress(job_id, start_time)
        except Exception as e:
            logger.error(f"Error uploading batch: {str(e)}")
            logger.error(traceback.format_exc())
            for dest_key, _ in batch:
                self.error_files.append({"file": dest_key, "error": str(e)})

    def log_progress(self, job_id, start_time, force=False):
        current_time = time.time()
        elapsed_time = Decimal(str(current_time - start_time))
        files_per_second = Decimal(str(self.processed_files)) / elapsed_time if elapsed_time > 0 else Decimal('0')
        progress = (Decimal(str(self.processed_files)) / Decimal(str(self.total_files))) * Decimal('100') if self.total_files > 0 else Decimal('0')

        if force or int(progress) in [0, 5, 10, 20, 40, 60, 80, 100]:
            logger.info(f"Progress: {progress:.2f}% ({self.processed_files}/{self.total_files} files)")
            logger.info(f"Processing speed: {files_per_second:.2f} files/second")
            logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")

            update_job_state(job_id, status='in_progress', progress=int(progress),
                             metadata={'processed_files': self.processed_files, 'total_files': self.total_files,
                                       'files_per_second': str(files_per_second), 'elapsed_time': str(elapsed_time)})
