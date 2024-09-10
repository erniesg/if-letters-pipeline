from airflow.models import BaseOperator
from helpers.s3 import upload_to_s3, get_s3_object_info, check_s3_object_exists, copy_s3_object
from helpers.dynamodb import update_job_state
from helpers.config import get_config
import requests
import os

class DownloadOperator(BaseOperator):
    def __init__(self, dataset_name, source, s3_bucket, s3_key, overwrite=False, **kwargs):
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.source = source
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.overwrite = overwrite
        self.config = get_config()

    def execute(self, context):
        job_id = f"{self.dataset_name}_download_{context['execution_date']}"
        try:
            update_job_state(job_id, status='in_progress', progress=0)

            if check_s3_object_exists(self.s3_bucket, self.s3_key) and not self.overwrite:
                file_info = get_s3_object_info(self.s3_bucket, self.s3_key)
                self.log.info(f"File already exists: {self.s3_key}")
                self.log.info(f"Size: {file_info['ContentLength']}, Type: {file_info['ContentType']}")
                update_job_state(job_id, status='completed', progress=100, metadata=file_info)
                return file_info

            if self.source['type'] == 'url':
                self.download_from_url()
            elif self.source['type'] == 'file':
                self.upload_from_local()
            elif self.source['type'] == 's3':
                self.copy_from_s3()
            else:
                raise ValueError(f"Unsupported source type: {self.source['type']}")

            file_info = get_s3_object_info(self.s3_bucket, self.s3_key)
            self.log.info(f"Downloaded/Uploaded to: {self.s3_key}")
            self.log.info(f"Size: {file_info['ContentLength']}, Type: {file_info['ContentType']}")

            update_job_state(job_id, status='completed', progress=100, metadata=file_info)
            return file_info
        except Exception as e:
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise

    def download_from_url(self):
        self.log.info(f"Downloading from URL: {self.source['path']}")
        response = requests.get(self.source['path'])
        response.raise_for_status()
        upload_to_s3(response.content, self.s3_bucket, self.s3_key)

    def upload_from_local(self):
        local_file_path = self.source['path']
        self.log.info(f"Uploading from local file: {local_file_path}")
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")
        with open(local_file_path, 'rb') as file:
            upload_to_s3(file.read(), self.s3_bucket, self.s3_key)

    def copy_from_s3(self):
        source_bucket, source_key = self.source['path'].split('/', 1)
        self.log.info(f"Copying from S3: s3://{source_bucket}/{source_key} to s3://{self.s3_bucket}/{self.s3_key}")
        copy_s3_object(source_bucket, source_key, self.s3_bucket, self.s3_key)
