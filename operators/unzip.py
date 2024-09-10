import io
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from airflow.models import BaseOperator
from helpers.s3 import download_from_s3, check_s3_prefix_exists, list_s3_objects
from helpers.dynamodb import update_job_state
from helpers.config import get_config, get_aws_credentials
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import time
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

    def upload_to_s3_with_retry(self, s3_client, file_content, s3_key, max_retries=5):
        for attempt in range(max_retries):
            try:
                s3_client.put_object(Body=file_content, Bucket=self.s3_bucket, Key=s3_key)
                return True
            except ClientError as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Error uploading {s3_key}: {str(e)}")
                    self.error_files.append({"file": s3_key, "error": str(e)})
                    return False

    def process_zip_content(self, zip_content, job_id):
        s3_client = self.get_s3_client()
        total_size = len(zip_content)
        processed_size = 0
        processed_files = 0

        with io.BytesIO(zip_content) as zip_buffer, zipfile.ZipFile(zip_buffer) as zip_ref:
            total_files = len(zip_ref.namelist())
            logger.info(f"Total files in zip: {total_files}")

            with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                futures = []
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith('/'):  # Skip directories
                        continue

                    with zip_ref.open(file_info.filename, pwd=self.unzip_password.encode() if self.unzip_password else None) as file:
                        file_content = file.read()
                        s3_key = f"{self.destination_prefix}/{file_info.filename}"
                        futures.append(executor.submit(self.upload_to_s3_with_retry, s3_client, file_content, s3_key))

                    processed_size += file_info.file_size
                    processed_files += 1
                    if processed_files % 100 == 0 or processed_files == total_files:
                        progress = int((processed_size / total_size) * 100)
                        logger.info(f"Unzip progress: {progress}% ({processed_files}/{total_files} files)")
                        update_job_state(job_id, status='in_progress', progress=progress,
                                         metadata={'processed_files': processed_files, 'total_files': total_files,
                                                   'processed_size': processed_size, 'total_size': total_size})

                for future in tqdm(as_completed(futures), total=len(futures), desc="Uploading files"):
                    future.result()

        return processed_files, total_files

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
            logger.info("Starting unzip operation")

            logger.info("Downloading zip file...")
            zip_content = download_from_s3(self.s3_bucket, self.s3_key)
            if not zip_content:
                raise ValueError(f"Failed to download zip file from s3://{self.s3_bucket}/{self.s3_key}")

            logger.info("Processing zip content...")
            processed_files, total_files = self.process_zip_content(zip_content, job_id)

            if self.error_files:
                logger.warning(f"Encountered errors with {len(self.error_files)} files.")
                for error in self.error_files[:10]:  # Log first 10 errors
                    logger.error(f"Error in file {error['file']}: {error['error']}")

                update_job_state(job_id, status='completed_with_errors', progress=100,
                                 metadata={'processed_files': processed_files, 'total_files': total_files,
                                           'error_files': len(self.error_files), 'first_10_errors': self.error_files[:10]})
            else:
                update_job_state(job_id, status='completed', progress=100,
                                 metadata={'processed_files': processed_files, 'total_files': total_files})

            logger.info(f"Unzip operation completed. Processed {processed_files}/{total_files} files.")
            logger.info(f"Files uploaded to s3://{self.s3_bucket}/{self.destination_prefix}/")
        except Exception as e:
            logger.error(f"Unzip operation failed: {str(e)}")
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise
