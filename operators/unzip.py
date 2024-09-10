from airflow.models import BaseOperator
from helpers.s3 import download_from_s3, check_s3_prefix_exists, list_s3_objects
from helpers.dynamodb import update_job_state
from helpers.config import get_config, get_aws_credentials
import zipfile
import os
import tempfile
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)

class UnzipOperator(BaseOperator):
    def __init__(self, dataset_name, s3_key, max_pool_connections=50, max_concurrency=16, batch_size=5000, **kwargs):
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.s3_key = s3_key
        self.config = get_config()
        self.s3_config = self.config['s3']
        self.s3_bucket = self.s3_config['bucket_name']
        self.destination_prefix = f"{self.s3_config['processed_folder']}/{self.dataset_name}"
        self.unzip_password = self.config['datasets'][dataset_name].get('unzip_password')
        self.batch_size = batch_size
        self.error_files = []
        self.max_pool_connections = max_pool_connections
        self.max_concurrency = max_concurrency

    def get_s3_client(self):
        config = Config(
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=self.max_pool_connections
        )
        aws_credentials = get_aws_credentials()
        return boto3.client('s3', config=config, **aws_credentials)

    def clean_s3_key(self, key):
        return '/'.join(filter(None, key.split('/')))

    def upload_to_s3_with_retry(self, s3_client, local_path, s3_key, max_retries=5):
        for attempt in range(max_retries):
            try:
                clean_key = self.clean_s3_key(s3_key)
                if os.path.isfile(local_path):
                    s3_client.upload_file(local_path, self.s3_bucket, clean_key)
                return True
            except ClientError as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Error uploading {clean_key}: {str(e)}")
                    self.error_files.append({"file": clean_key, "error": str(e)})
                    return False

    def process_batch(self, s3_client, file_batch):
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            futures = []
            for local_path, s3_key in file_batch:
                futures.append(executor.submit(self.upload_to_s3_with_retry, s3_client, local_path, s3_key))

            for future in as_completed(futures):
                future.result()  # This will raise any exceptions that occurred during execution

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
            logger.info("Progress: 0%")

            logger.info("Downloading zip file...")
            zip_content = download_from_s3(self.s3_bucket, self.s3_key)
            if not zip_content:
                raise ValueError(f"Failed to download zip file from s3://{self.s3_bucket}/{self.s3_key}")

            s3_client = self.get_s3_client()
            total_files = 0
            processed_files = 0

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_zip_path = os.path.join(temp_dir, "temp.zip")
                with open(temp_zip_path, 'wb') as temp_file:
                    temp_file.write(zip_content)

                logger.info("Unzipping and uploading files in batches...")
                with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(path=temp_dir, pwd=self.unzip_password.encode() if self.unzip_password else None)

                    file_batch = []
                    total_files = sum([len(files) for _, _, files in os.walk(temp_dir)]) - 1  # Subtract 1 for the zip file itself
                    logger.info(f"Total files to process: {total_files}")

                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            if file == "temp.zip":
                                continue
                            local_path = os.path.join(root, file)
                            relative_path = os.path.relpath(local_path, temp_dir)
                            s3_key = f"{self.destination_prefix}/{relative_path}"
                            file_batch.append((local_path, s3_key))

                            if len(file_batch) >= self.batch_size:
                                self.process_batch(s3_client, file_batch)
                                processed_files += len(file_batch)
                                file_batch = []

                                progress = int((processed_files / total_files) * 100)
                                if progress in [0, 20, 40, 60, 80, 100]:
                                    logger.info(f"Progress: {progress}%")
                                    update_job_state(job_id, status='in_progress', progress=progress,
                                                     metadata={'processed_files': processed_files})

                    # Process any remaining files
                    if file_batch:
                        self.process_batch(s3_client, file_batch)
                        processed_files += len(file_batch)

            if self.error_files:
                logger.warning(f"Encountered errors with {len(self.error_files)} files.")
                for error in self.error_files[:10]:  # Log first 10 errors
                    logger.error(f"Error in file {error['file']}: {error['error']}")

                update_job_state(job_id, status='completed_with_errors', progress=100,
                                 metadata={'error_files': len(self.error_files), 'first_10_errors': self.error_files[:10]})
            else:
                update_job_state(job_id, status='completed', progress=100)

            logger.info(f"Unzip operation completed. Processed {processed_files} files, with {len(self.error_files)} errors.")
            logger.info(f"Files uploaded to s3://{self.s3_bucket}/{self.destination_prefix}/")
        except Exception as e:
            logger.error(f"Unzip operation failed: {str(e)}")
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise
