from airflow.models import BaseOperator
from helpers.s3 import download_from_s3, upload_to_s3, check_s3_prefix_exists, list_s3_objects
from helpers.dynamodb import update_job_state
from helpers.config import get_config
import zipfile
import io
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import tempfile
import boto3

class UnzipOperator(BaseOperator):
    def __init__(self, dataset_name, s3_bucket, s3_key, destination_prefix, unzip_password=None, overwrite=False, batch_size=1000, **kwargs):
        super().__init__(**kwargs)
        self.dataset_name = dataset_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.destination_prefix = destination_prefix
        self.unzip_password = unzip_password
        self.overwrite = overwrite
        self.batch_size = batch_size
        self.config = get_config()

    def process_file_batch(self, file_batch, zip_ref, s3_bucket, destination_prefix, unzip_password):
        s3_client = boto3.client('s3')
        results = []
        for file in file_batch:
            try:
                with zip_ref.open(file, pwd=unzip_password.encode() if unzip_password else None) as file_in_zip:
                    file_content = file_in_zip.read()
                    destination_key = f"{destination_prefix}/{file}"
                    s3_client.put_object(Bucket=s3_bucket, Key=destination_key, Body=file_content)
                results.append(True)
            except Exception as e:
                self.log.error(f"Error processing file {file}: {str(e)}")
                results.append(False)
        return results

    def execute(self, context):
        job_id = f"{self.dataset_name}_unzip_{context['execution_date']}"
        try:
            # Check if files are already unzipped
            if check_s3_prefix_exists(self.s3_bucket, self.destination_prefix):
                existing_files = list_s3_objects(self.s3_bucket, self.destination_prefix)
                self.log.info(f"Files already exist in destination: {self.destination_prefix}")
                self.log.info(f"Number of existing files: {len(existing_files)}")

                if not self.overwrite:
                    update_job_state(job_id, status='completed', progress=100, metadata={'existing_files': len(existing_files)})
                    return
                else:
                    self.log.info("Overwrite flag is set. Proceeding with unzip operation.")

            update_job_state(job_id, status='in_progress', progress=0)

            zip_content = download_from_s3(self.s3_bucket, self.s3_key)
            if not zip_content:
                raise ValueError(f"Failed to download zip file from s3://{self.s3_bucket}/{self.s3_key}")

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(zip_content)
                temp_file_path = temp_file.name

            with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                total_files = len(zip_ref.namelist())
                processed_files = 0

                # Create batches of files
                file_batches = [zip_ref.namelist()[i:i + self.batch_size] for i in range(0, total_files, self.batch_size)]

                # Use ProcessPoolExecutor for true parallelism
                with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
                    futures = []
                    for batch in file_batches:
                        future = executor.submit(
                            self.process_file_batch,
                            batch,
                            zip_ref,
                            self.s3_bucket,
                            self.destination_prefix,
                            self.unzip_password
                        )
                        futures.append(future)

                    for future in as_completed(futures):
                        batch_results = future.result()
                        processed_files += len(batch_results)
                        successful_files = sum(batch_results)

                        progress = int((processed_files / total_files) * 100)
                        update_job_state(job_id, status='in_progress', progress=progress,
                                         metadata={'processed_files': processed_files, 'successful_files': successful_files})

            os.unlink(temp_file_path)  # Remove the temporary file

            update_job_state(job_id, status='completed', progress=100)
            self.log.info(f"Successfully unzipped and uploaded all files to s3://{self.s3_bucket}/{self.destination_prefix}/")
        except Exception as e:
            update_job_state(job_id, status='failed', progress=0, metadata={'error': str(e)})
            raise
