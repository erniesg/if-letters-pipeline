import boto3
from stream_unzip import stream_unzip
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Thread
from airflow.models import BaseOperator
from helpers.s3 import bulk_upload_to_s3, get_optimized_s3_client, stream_download_from_s3, check_s3_prefix_exists, list_s3_objects
from helpers.dynamodb import update_job_state
from helpers.config import get_config
import logging
import time
from decimal import Decimal
import traceback

logger = logging.getLogger(__name__)

class StreamingZipProcessor:
    def __init__(self, s3_client, source_bucket, source_key, dest_bucket, dest_prefix, unzip_password=None):
        self.s3_client = s3_client
        self.source_bucket = source_bucket
        self.source_key = source_key
        self.dest_bucket = dest_bucket
        self.dest_prefix = dest_prefix
        self.unzip_password = unzip_password.encode('utf-8') if unzip_password else None

    def process(self):
        logger.debug(f"Processing zip file: s3://{self.source_bucket}/{self.source_key}")
        try:
            for file_name, file_size, unzipped_chunks in stream_unzip(
                self.zipped_chunks(),
                password=self.unzip_password
            ):
                yield file_name, file_size, unzipped_chunks
        except Exception as e:
            logger.error(f"Error in StreamingZipProcessor.process(): {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def zipped_chunks(self):
        return stream_download_from_s3(self.s3_client, self.source_bucket, self.source_key)

class UnzipOperator(BaseOperator):
    def __init__(
        self,
        dataset_name,
        s3_keys,
        s3_bucket,
        destination_prefix,
        max_concurrency=64,
        batch_size=1000,
        concurrent_batches=8,
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
        self.batch_size = batch_size
        self.concurrent_batches = concurrent_batches
        self.error_files = []
        self.processed_files = 0
        self.total_files = 0

    def execute(self, context):
        job_id = f"{self.dataset_name}_unzip_{context['execution_date']}"
        logger.info(f"Starting UnzipOperator execution for job_id: {job_id}")
        s3_client = get_optimized_s3_client()

        try:
            if check_s3_prefix_exists(self.s3_bucket, self.destination_prefix):
                existing_files = list_s3_objects(self.s3_bucket, self.destination_prefix)
                logger.info(f"Files already exist in destination: {self.destination_prefix}")
                logger.info(f"Number of existing files: {len(existing_files)}")
                update_job_state(job_id, status='completed', progress=100, metadata={'existing_files': len(existing_files)})
                return

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
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise

    def process_zip_file(self, s3_client, s3_key, job_id, start_time):
        logger.info(f"Processing file: s3://{self.s3_bucket}/{s3_key}")
        try:
            processor = StreamingZipProcessor(
                s3_client, self.s3_bucket, s3_key,
                self.s3_bucket, self.destination_prefix, self.unzip_password
            )

            batch_queue = Queue(maxsize=self.concurrent_batches * 2)

            with ThreadPoolExecutor(max_workers=self.concurrent_batches) as executor:
                futures = []
                for _ in range(self.concurrent_batches):
                    futures.append(executor.submit(self.process_batches, s3_client, batch_queue, job_id, start_time))

                current_batch = []
                for file_name, file_size, unzipped_chunks in processor.process():
                    try:
                        self.total_files += 1
                        if isinstance(file_name, bytes):
                            file_name = file_name.decode('utf-8')
                        dest_key = f"{self.destination_prefix}/{file_name}"

                        current_batch.append((dest_key, unzipped_chunks, file_size))

                        if len(current_batch) >= self.batch_size:
                            batch_queue.put(current_batch)
                            current_batch = []

                        if self.total_files % 10000 == 0:
                            self.log_progress(job_id, start_time)
                    except Exception as e:
                        logger.error(f"Error processing file {file_name}: {str(e)}")
                        self.error_files.append({"file": file_name, "error": str(e)})

                if current_batch:
                    batch_queue.put(current_batch)

                for _ in range(self.concurrent_batches):
                    batch_queue.put(None)

                for future in as_completed(futures):
                    future.result()

        except Exception as e:
            logger.error(f"Error processing zip file {s3_key}: {str(e)}")
            self.error_files.append({"file": s3_key, "error": str(e)})

    def process_batches(self, s3_client, batch_queue, job_id, start_time):
        while True:
            batch = batch_queue.get()
            if batch is None:
                break
            self.upload_batch(s3_client, batch)
            batch_queue.task_done()
            self.log_progress(job_id, start_time)

    def upload_batch(self, s3_client, batch):
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            futures = []
            for dest_key, unzipped_chunks, file_size in batch:
                futures.append(executor.submit(
                    self.upload_file,
                    s3_client, dest_key, unzipped_chunks, file_size
                ))

            for future in as_completed(futures):
                try:
                    future.result()
                    self.processed_files += 1
                except Exception as e:
                    logger.error(f"Error uploading file: {str(e)}")
                    self.error_files.append({"file": "unknown", "error": str(e)})

    def upload_file(self, s3_client, dest_key, unzipped_chunks, file_size):
        try:
            if file_size < 314572800:  # 300MB
                bulk_upload_to_s3(s3_client, [(dest_key, b''.join(unzipped_chunks))], self.s3_bucket)
            else:
                s3_client.upload_fileobj(BytesIO(b''.join(unzipped_chunks)), self.s3_bucket, dest_key)
        except Exception as e:
            logger.error(f"Error uploading file {dest_key}: {str(e)}")
            raise

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
