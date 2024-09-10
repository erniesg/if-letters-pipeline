from airflow.models import BaseOperator
from helpers.s3 import upload_to_s3, get_s3_object_info, check_s3_object_exists
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

            # Check if file already exists in S3
            if check_s3_object_exists(self.s3_bucket, self.s3_key):
                file_info = get_s3_object_info(self.s3_bucket, self.s3_key)
                self.log.info(f"File already exists: {self.s3_key}")
                self.log.info(f"Size: {file_info['ContentLength']}, Type: {file_info['ContentType']}")

                if not self.overwrite:
                    update_job_state(job_id, status='completed', progress=100, metadata=file_info)
                    return file_info
                else:
                    self.log.info("Overwrite flag is set. Proceeding with download.")

            if self.source['type'] == 'url':
                self.download_from_url()
            elif self.source['type'] == 'file':
                self.copy_from_local()
            else:
                raise ValueError(f"Unsupported source type: {self.source['type']}")

            file_info = get_s3_object_info(self.s3_bucket, self.s3_key)
            self.log.info(f"Downloaded and uploaded: {self.s3_key}")
            self.log.info(f"Size: {file_info['ContentLength']}, Type: {file_info['ContentType']}")

            update_job_state(job_id, status='completed', progress=100, metadata=file_info)
            return file_info
        except Exception as e:
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise

    def download_from_url(self):
        response = requests.get(self.source['path'])
        response.raise_for_status()
        upload_to_s3(response.content, self.s3_bucket, self.s3_key)

    def copy_from_local(self):
        self.log.info(f"Copying from local file: {self.source['path']}")
        with open(self.source['path'], 'rb') as file:
            upload_to_s3(file.read(), self.s3_bucket, self.s3_key)
