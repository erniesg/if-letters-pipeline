import logging
from botocore.exceptions import ClientError
from helpers.resource_management import get_s3_client, ensure_resource, handle_existing_item
from helpers.config import get_config
from io import BytesIO

logger = logging.getLogger(__name__)

# Get the configuration
config = get_config()
s3_config = config['s3']

@ensure_resource('s3')
@handle_existing_item
def upload_to_s3(content, bucket, s3_key):
    try:
        s3_client = get_s3_client()
        s3_client.upload_fileobj(BytesIO(content), bucket, s3_key)
        logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
    except ClientError as e:
        logger.error(f"Error uploading to s3://{bucket}/{s3_key}: {str(e)}")

@ensure_resource('s3')
@handle_existing_item
def download_from_s3(bucket, s3_key):
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        logger.info(f"Successfully downloaded from s3://{bucket}/{s3_key}")
        return response['Body'].read()
    except ClientError as e:
        logger.error(f"Error downloading from s3://{bucket}/{s3_key}: {str(e)}")
        return None

@ensure_resource('s3')
@handle_existing_item
def check_s3_object_exists(bucket, s3_key):
    try:
        s3_client = get_s3_client()
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logger.error(f"Error checking s3://{bucket}/{s3_key}: {str(e)}")
            return False

@ensure_resource('s3')
@handle_existing_item
def get_s3_object_info(bucket, s3_key):
    try:
        s3_client = get_s3_client()
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        return {
            'ContentLength': response.get('ContentLength', 0),
            'ContentType': response.get('ContentType', 'Unknown')
        }
    except ClientError as e:
        logger.error(f"Error getting info for s3://{bucket}/{s3_key}: {str(e)}")
        return None

@ensure_resource('s3')
@handle_existing_item
def check_s3_prefix_exists(bucket, prefix):
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response
    except ClientError as e:
        logger.error(f"Error checking prefix s3://{bucket}/{prefix}: {str(e)}")
        return False

@ensure_resource('s3')
@handle_existing_item
def list_s3_objects(bucket, prefix):
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return response.get('Contents', [])
    except ClientError as e:
        logger.error(f"Error listing objects in s3://{bucket}/{prefix}: {str(e)}")
        return []

__all__ = ['upload_to_s3', 'download_from_s3', 'check_s3_object_exists',
           'get_s3_object_info', 'check_s3_prefix_exists', 'list_s3_objects']
