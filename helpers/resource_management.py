import functools
import logging
from botocore.exceptions import ClientError
from helpers.config import get_config, get_aws_credentials
import boto3

logger = logging.getLogger(__name__)

s3_client = None
dynamodb_resource = None

def initialize_s3_client():
    global s3_client
    if s3_client is None:
        aws_credentials = get_aws_credentials()
        s3_client = boto3.client('s3', **aws_credentials)
    return s3_client

def initialize_dynamodb_resource():
    global dynamodb_resource
    if dynamodb_resource is None:
        aws_credentials = get_aws_credentials()
        dynamodb_resource = boto3.resource('dynamodb', **aws_credentials)
    return dynamodb_resource

def get_s3_client():
    return initialize_s3_client()

def get_dynamodb_resource():
    return initialize_dynamodb_resource()

def ensure_resource(resource_type):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            config = get_config()
            if resource_type == 's3':
                s3_client = get_s3_client()
                bucket_name = config['s3']['bucket_name']
                try:
                    s3_client.head_bucket(Bucket=bucket_name)
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        logger.info(f"Bucket {bucket_name} does not exist. Creating...")
                        s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': get_aws_credentials()['region_name']}
                        )
                        logger.info(f"Bucket {bucket_name} created successfully.")
                    else:
                        logger.error(f"Error checking S3 bucket: {e}")
                        raise
            elif resource_type == 'dynamodb':
                dynamodb = get_dynamodb_resource()
                table_name = config['dynamodb']['table_name']
                try:
                    table = dynamodb.Table(table_name)
                    table.load()
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        logger.info(f"Table {table_name} does not exist. Creating...")
                        table = dynamodb.create_table(
                            TableName=table_name,
                            KeySchema=[
                                {'AttributeName': 'job_id', 'KeyType': 'HASH'}
                            ],
                            AttributeDefinitions=[
                                {'AttributeName': 'job_id', 'AttributeType': 'S'}
                            ],
                            BillingMode='PAY_PER_REQUEST'
                        )
                        table.wait_until_exists()
                        logger.info(f"Table {table_name} created successfully.")
                    else:
                        logger.error(f"Error checking DynamoDB table: {e}")
                        raise
            return func(*args, **kwargs)
        return wrapper
    return decorator

def handle_existing_item(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] in ['ConditionalCheckFailedException', 'ResourceNotFoundException']:
                logger.warning(f"Item already exists or resource not found. Skipping. Error: {e}")
                return None
            else:
                raise
    return wrapper
