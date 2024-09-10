import json
import os
from helpers.s3_helper import download_from_s3, upload_to_s3
from helpers.multiprocessing_helper import parallel_process

class PuzzlePiecesProcessor:
    def process(self, s3_bucket, s3_prefix, processed_folder, num_workers):
        # Download Chinese_to_ID.json
        download_from_s3(s3_bucket, f"{s3_prefix}/Puzzle-Pieces-Picker Dataset/Chinese_to_ID.json", '/tmp/Chinese_to_ID.json')
        with open('/tmp/Chinese_to_ID.json', 'r') as f:
            chinese_to_id = json.load(f)

        # Process each class folder
        class_folders = self._list_class_folders(s3_bucket, f"{s3_prefix}/Puzzle-Pieces-Picker Dataset/Dataset")
        parallel_process(self._process_class, class_folders, num_workers, s3_bucket, s3_prefix, processed_folder)

    def _list_class_folders(self, s3_bucket, prefix):
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix, Delimiter='/')
        return [o.get('Prefix') for o in response.get('CommonPrefixes', [])]

    def _process_class(self, class_folder, s3_bucket, s3_prefix, processed_folder):
        class_id = os.path.basename(os.path.dirname(class_folder))
        images = self._list_images(s3_bucket, class_folder)
        for image in images:
            self._process_image(s3_bucket, image, f"{processed_folder}/{class_id}")

    def _list_images(self, s3_bucket, prefix):
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        return [o['Key'] for o in response.get('Contents', []) if o['Key'].lower().endswith(('.png', '.jpg', '.jpeg'))]

    def _process_image(self, s3_bucket, s3_key, dest_prefix):
        # Download, process (if needed), and upload to processed folder
        local_path = f"/tmp/{os.path.basename(s3_key)}"
        download_from_s3(s3_bucket, s3_key, local_path)
        # Add any image processing here if needed
        upload_to_s3(local_path, s3_bucket, f"{dest_prefix}/{os.path.basename(s3_key)}")
        os.remove(local_path)
