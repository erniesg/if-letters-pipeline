import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import BaseOperator
from helpers.s3 import check_s3_prefix_exists, list_s3_objects
from helpers.dynamodb import update_job_state
from helpers.config import get_config, get_aws_credentials
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import logging
import traceback
import tempfile
import os
import json

logger = logging.getLogger(__name__)

class UnzipOperator(BaseOperator):
    def __init__(self, dataset_name, s3_key, max_pool_connections=50, max_concurrency=16, batch_size=500, **kwargs):
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
        self.checkpoint_interval = 1000  # Save checkpoint every 1000 files

    def get_s3_client(self):
        config = Config(
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=self.max_pool_connections
        )
        aws_credentials = get_aws_credentials()
        return boto3.client('s3', config=config, **aws_credentials)

    def upload_batch_to_s3(self, s3_client, batch):
        with ThreadPoolExecutor(max_workers=min(len(batch), self.max_concurrency)) as executor:
            futures = [executor.submit(self.upload_file_to_s3, s3_client, file_key, file_content)
                       for file_key, file_content in batch]
            for future in as_completed(futures):
                future.result()

    def upload_file_to_s3(self, s3_client, file_key, file_content):
        try:
            s3_client.put_object(Bucket=self.s3_bucket, Key=file_key, Body=file_content)
        except ClientError as e:
            logger.error(f"Error uploading file {file_key}: {str(e)}")
            self.error_files.append({"file": file_key, "error": str(e)})

    def process_zip_content(self, job_id):
        s3_client = self.get_s3_client()
        processed_files = 0
        total_files = 0
        last_progress = -1

        # Try to load checkpoint
        checkpoint = self.load_checkpoint(job_id)
        if checkpoint:
            processed_files = checkpoint['processed_files']
            total_files = checkpoint['total_files']
            logger.info(f"Resuming from checkpoint. Processed files: {processed_files}")

        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
            self.download_from_s3_streaming(self.s3_bucket, self.s3_key, temp_file)
            temp_file_path = temp_file.name

        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
            if total_files == 0:
                total_files = len([f for f in zip_ref.namelist() if not f.endswith('/')])
            logger.info(f"Total files in zip: {total_files}")

            with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                batch = []
                futures = []
                for file_info in zip_ref.infolist()[processed_files:]:
                    if file_info.filename.endswith('/'):  # Skip directories
                        continue

                    with zip_ref.open(file_info.filename, pwd=self.unzip_password.encode() if self.unzip_password else None) as file:
                        file_content = file.read()
                        file_key = f"{self.destination_prefix}/{file_info.filename}"
                        batch.append((file_key, file_content))

                    if len(batch) >= self.batch_size:
                        futures.append(executor.submit(self.upload_batch_to_s3, s3_client, batch))
                        batch = []

                    processed_files += 1

                    progress = int((processed_files / total_files) * 100)
                    if progress in [0, 20, 40, 60, 80, 100] and progress != last_progress:
                        logger.info(f"Unzip progress: {progress}% ({processed_files}/{total_files} files)")
                        update_job_state(job_id, status='in_progress', progress=progress,
                                         metadata={'processed_files': processed_files, 'total_files': total_files})
                        last_progress = progress

                    # Save checkpoint
                    if processed_files % self.checkpoint_interval == 0:
                        self.save_checkpoint(job_id, {'processed_files': processed_files, 'total_files': total_files})

                if batch:
                    futures.append(executor.submit(self.upload_batch_to_s3, s3_client, batch))

                for future in as_completed(futures):
                    future.result()

        os.unlink(temp_file_path)
        return processed_files, total_files

    def download_from_s3_streaming(self, bucket, key, file_object):
        s3_client = self.get_s3_client()
        try:
            s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=file_object)
        except ClientError as e:
            logger.error(f"Error downloading {key} from S3: {str(e)}")
            raise

    def save_checkpoint(self, job_id, data):
        s3_client = self.get_s3_client()
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
        s3_client = self.get_s3_client()
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
        try:
            if check_s3_prefix_exists(self.s3_bucket, self.destination_prefix):
                existing_files = list_s3_objects(self.s3_bucket, self.destination_prefix)
                logger.info(f"Files already exist in destination: {self.destination_prefix}")
                logger.info(f"Number of existing files: {len(existing_files)}")
                update_job_state(job_id, status='completed', progress=100, metadata={'existing_files': len(existing_files)})
                return

            update_job_state(job_id, status='in_progress', progress=0)
            logger.info("Starting unzip operation")

            logger.info("Processing zip content...")
            processed_files, total_files = self.process_zip_content(job_id)

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
            self.log.error(traceback.format_exc())
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise
