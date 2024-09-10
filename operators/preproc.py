from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from processors import get_processor

class PreprocessOperator(BaseOperator):
    @apply_defaults
    def __init__(self, dataset_name, s3_bucket, s3_prefix, processed_folder, num_workers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_name = dataset_name
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.processed_folder = processed_folder
        self.num_workers = num_workers

    def execute(self, context):
        processor = get_processor(self.dataset_name)
        processor.process(self.s3_bucket, self.s3_prefix, self.processed_folder, self.num_workers)
        self.log.info(f"Preprocessed {self.dataset_name} dataset")
