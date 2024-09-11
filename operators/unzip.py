import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import BaseOperator
from helpers.s3 import stream_download_from_s3, batch_upload_to_s3, get_optimized_s3_client, check_s3_prefix_exists, list_s3_objects
from helpers.dynamodb import update_job_state
from helpers.config import get_config
import logging
import traceback
import json
import threading

logger = logging.getLogger(__name__)

class UnzipOperator(BaseOperator):
    def __init__(self, dataset_name, s3_key, **kwargs):
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.s3_key = s3_key
        self.config = get_config()
        self.s3_config = self.config['s3']
        self.s3_bucket = self.s3_config['bucket_name']
        self.destination_prefix = f"{self.s3_config['processed_folder']}/{self.dataset_name}"
        self.unzip_password = self.config['datasets'][dataset_name].get('unzip_password')
        self.processing_config = self.config.get('processing', {})
        self.chunk_size = self.processing_config.get('chunk_size', 64 * 1024 * 1024)
        self.max_concurrency = self.processing_config.get('max_concurrency', 32)
        self.batch_size = self.processing_config.get('batch_size', 500)
        self.multipart_threshold = self.processing_config.get('multipart_threshold', 8 * 1024 * 1024)
        self.multipart_chunksize = self.processing_config.get('multipart_chunksize', 8 * 1024 * 1024)
        self.error_files = []
        self.checkpoint_interval = 5000  # Increased checkpoint interval
        self.progress_lock = threading.Lock()
        self.processed_files = 0
        self.total_files = 0

    def process_zip_chunk(self, zip_file):
        files_to_upload = []
        for info in zip_file.infolist():
            if info.file_size == 0:  # Skip directories
                continue
            with zip_file.open(info, pwd=self.unzip_password.encode() if self.unzip_password else None) as file:
                content = file.read()
                s3_key = f"{self.destination_prefix}/{info.filename}"
                files_to_upload.append((BytesIO(content), s3_key))
        return files_to_upload

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

    def update_progress(self, job_id, force=False):
        with self.progress_lock:
            if self.total_files > 0:
                progress = int((self.processed_files / self.total_files) * 100)
                if force or progress in [0, 20, 40, 60, 80, 100]:
                    update_job_state(job_id, status='in_progress', progress=progress,
                                     metadata={'processed_files': self.processed_files, 'total_files': self.total_files})
                    logger.info(f"Progress: {progress}% ({self.processed_files}/{self.total_files} files)")

    def execute(self, context):
        job_id = f"{self.dataset_name}_unzip_{context['execution_date']}"
        try:
            if check_s3_prefix_exists(self.s3_bucket, self.destination_prefix):
                existing_files = list_s3_objects(self.s3_bucket, self.destination_prefix)
                logger.info(f"Files already exist in destination: {self.destination_prefix}")
                logger.info(f"Number of existing files: {len(existing_files)}")
                update_job_state(job_id, status='completed', progress=100, metadata={'existing_files': len(existing_files)})
                return

            update_job_state(job_id, status='in_progress', progress=0)
            logger.info(f"Starting streaming unzip operation for {self.s3_key}")

            # Try to load checkpoint
            checkpoint = self.load_checkpoint(job_id)
            if checkpoint:
                self.processed_files = checkpoint['processed_files']
                self.total_files = checkpoint['total_files']
                logger.info(f"Resuming from checkpoint. Processed files: {self.processed_files}")

            s3_client = get_optimized_s3_client()
            response = s3_client.head_object(Bucket=self.s3_bucket, Key=self.s3_key)
            total_bytes = response['ContentLength']

            zip_buffer = BytesIO()
            upload_futures = []

            with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                for chunk in stream_download_from_s3(self.s3_bucket, self.s3_key, self.chunk_size):
                    zip_buffer.write(chunk)
                    if zip_buffer.tell() >= self.chunk_size:
                        zip_buffer.seek(0)
                        with zipfile.ZipFile(zip_buffer) as zip_file:
                            if self.total_files == 0:
                                self.total_files = len([f for f in zip_file.namelist() if not f.endswith('/')])
                                self.update_progress(job_id, force=True)
                            files_to_upload = self.process_zip_chunk(zip_file)
                            for i in range(0, len(files_to_upload), self.batch_size):
                                batch = files_to_upload[i:i+self.batch_size]
                                future = executor.submit(batch_upload_to_s3,
                                                         batch,
                                                         self.s3_bucket,
                                                         max_workers=self.max_concurrency,
                                                         multipart_threshold=self.multipart_threshold,
                                                         multipart_chunksize=self.multipart_chunksize)
                                upload_futures.append(future)
                        zip_buffer = BytesIO()  # Reset buffer for next chunk

                # Process any remaining data in the buffer
                if zip_buffer.tell() > 0:
                    zip_buffer.seek(0)
                    with zipfile.ZipFile(zip_buffer) as zip_file:
                        files_to_upload = self.process_zip_chunk(zip_file)
                        for i in range(0, len(files_to_upload), self.batch_size):
                            batch = files_to_upload[i:i+self.batch_size]
                            future = executor.submit(batch_upload_to_s3,
                                                     batch,
                                                     self.s3_bucket,
                                                     max_workers=self.max_concurrency,
                                                     multipart_threshold=self.multipart_threshold,
                                                     multipart_chunksize=self.multipart_chunksize)
                            upload_futures.append(future)

                # Wait for all uploads to complete
                for future in as_completed(upload_futures):
                    results = future.result()
                    self.handle_upload_results(results)
                    self.processed_files += len(results)
                    if self.processed_files % self.checkpoint_interval == 0:
                        self.save_checkpoint(job_id, {'processed_files': self.processed_files, 'total_files': self.total_files})
                    self.update_progress(job_id)

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
        except Exception as e:
            logger.error(f"Unzip operation failed: {str(e)}")
            self.log.error(traceback.format_exc())
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise

    def handle_upload_results(self, results):
        for s3_key, success in results:
            if not success:
                self.error_files.append({"file": s3_key, "error": "Upload failed"})
