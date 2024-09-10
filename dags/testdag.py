import logging
logging.info("Loading testdag.py")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from helpers.config import get_config

logger = logging.getLogger(__name__)

def print_config(**kwargs):
    try:
        config = get_config()
        logger.info("Current configuration:")
        logger.info(f"S3 bucket: {config['s3']['bucket_name']}")
        logger.info(f"S3 data prefix: {config['s3']['data_prefix']}")
        logger.info(f"S3 processed folder: {config['s3']['processed_folder']}")
        logger.info(f"AWS region: {config['aws']['region_name']}")
        logger.info(f"DynamoDB table: {config['dynamodb']['table_name']}")
        logger.info("Datasets:")
        for dataset, dataset_config in config['datasets'].items():
            logger.info(f"  {dataset}: {dataset_config}")
    except Exception as e:
        logger.error(f"Error in print_config: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_print_config',
    default_args=default_args,
    description='A test DAG to print configuration',
    schedule_interval=None,
    catchup=False
)

print_config_task = PythonOperator(
    task_id='print_config',
    python_callable=print_config,
    dag=dag,
)

# For troubleshooting
def test_dag_loading():
    try:
        from airflow.models import DagBag
        dag_bag = DagBag()
        if 'test_print_config' not in dag_bag.dags:
            logger.error("test_print_config DAG not found in DagBag")
        else:
            logger.info("test_print_config DAG successfully loaded")
    except Exception as e:
        logger.error(f"Error testing DAG loading: {str(e)}")

test_dag_loading()

# This line is crucial for Airflow to pick up the DAG
test_print_config = dag
