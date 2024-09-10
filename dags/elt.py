from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.download import DownloadOperator
from operators.unzip import UnzipOperator
from helpers.dynamodb import create_job, update_job_state, get_job_state
from helpers.s3 import check_s3_object_exists, get_s3_object_info
from helpers.config import get_config  # Update this import
import logging

logger = logging.getLogger(__name__)

def create_dataset_dag(dataset_name, default_args):
    with DAG(
        dag_id=f'{dataset_name}_elt',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
    ) as dag:
        config = get_config()  # Get the configuration
        dataset_config = config['datasets'][dataset_name]
        s3_config = config['s3']

        logger.info(f"Creating DAG for dataset: {dataset_name}")
        logger.info(f"S3 config: {s3_config}")
        logger.info(f"Dataset config: {dataset_config}")

        start = DummyOperator(task_id='start')

        def check_and_create_job(task_id, dataset_name, operation, file_index=None, **kwargs):
            job_id = f"{dataset_name}_{operation}_{kwargs['execution_date']}"
            if file_index is not None:
                job_id += f"_{file_index}"
            logger.info(f"Checking job state for: {job_id}")
            job_state = get_job_state(job_id)
            if job_state and job_state['status'] == 'completed':
                logger.info(f"Job {job_id} already completed. Skipping.")
                return 'skip'
            logger.info(f"Creating new job: {job_id}")
            create_job(job_id, dataset_name, operation)
            return job_id

        def process_download(source, s3_bucket, s3_key, dataset_name, job_id, **kwargs):
            logger.info(f"Processing download for {dataset_name}")
            logger.info(f"S3 bucket: {s3_bucket}, S3 key: {s3_key}")
            if check_s3_object_exists(s3_bucket, s3_key):
                file_info = get_s3_object_info(s3_bucket, s3_key)
                logger.info(f"File already exists: {s3_key}")
                logger.info(f"Size: {file_info['ContentLength']}, Type: {file_info['ContentType']}")
                update_job_state(job_id, status="completed", progress=100, metadata=file_info)
                return 'skip'

            download_op = DownloadOperator(
                task_id=f'download_{dataset_name}',
                dataset_name=dataset_name,
                source=source,
                s3_bucket=s3_bucket,
                s3_key=s3_key
            )
            result = download_op.execute(kwargs)
            logger.info(f"Download result: {result}")
            update_job_state(job_id, status="completed", progress=100, metadata=result)
            return result

        def process_unzip(s3_bucket, s3_key, destination_prefix, dataset_name, unzip_password, job_id, **kwargs):
            logger.info(f"Processing unzip for {dataset_name}")
            logger.info(f"S3 bucket: {s3_bucket}, S3 key: {s3_key}, Destination: {destination_prefix}")
            logger.info(f"Unzip password: {unzip_password}")  # Add this line for debugging
            if check_s3_object_exists(s3_bucket, destination_prefix):
                logger.info(f"Destination already exists: {destination_prefix}")
                update_job_state(job_id, status="completed", progress=100)
                return 'skip'

            unzip_op = UnzipOperator(
                task_id=f'unzip_{dataset_name}',
                dataset_name=dataset_name,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                destination_prefix=destination_prefix,
                unzip_password=unzip_password
            )
            result = unzip_op.execute(kwargs)
            logger.info(f"Unzip result: {result}")
            update_job_state(job_id, status="completed", progress=100, metadata=result)
            return result

        if isinstance(dataset_config['source']['path'], list):
            download_tasks = []
            unzip_tasks = []
            for i, path in enumerate(dataset_config['source']['path']):
                check_download = PythonOperator(
                    task_id=f'check_download_{dataset_name}_{i}',
                    python_callable=check_and_create_job,
                    op_kwargs={'task_id': f'download_{dataset_name}_{i}', 'dataset_name': dataset_name, 'operation': 'download', 'file_index': i}
                )

                download_task = PythonOperator(
                    task_id=f'download_{dataset_name}_{i}',
                    python_callable=process_download,
                    op_kwargs={
                        'source': {'type': dataset_config['source']['type'], 'path': path},
                        's3_bucket': s3_config['bucket_name'],
                        's3_key': f"{s3_config['data_prefix']}/{dataset_name}_{i}.zip",
                        'dataset_name': dataset_name,
                        'job_id': f"{dataset_name}_download_{i}_{{{{ execution_date }}}}"
                    }
                )

                check_unzip = PythonOperator(
                    task_id=f'check_unzip_{dataset_name}_{i}',
                    python_callable=check_and_create_job,
                    op_kwargs={'task_id': f'unzip_{dataset_name}_{i}', 'dataset_name': dataset_name, 'operation': 'unzip', 'file_index': i}
                )

                unzip_task = PythonOperator(
                    task_id=f'unzip_{dataset_name}_{i}',
                    python_callable=process_unzip,
                    op_kwargs={
                        's3_bucket': s3_config['bucket_name'],
                        's3_key': f"{s3_config['data_prefix']}/{dataset_name}_{i}.zip",
                        'destination_prefix': f"{s3_config['processed_folder']}/{dataset_name}",
                        'dataset_name': dataset_name,
                        'unzip_password': dataset_config.get('unzip_password'),
                        'job_id': f"{dataset_name}_unzip_{i}_{{{{ execution_date }}}}"
                    }
                )

                start >> check_download >> download_task >> check_unzip >> unzip_task
                download_tasks.append(download_task)
                unzip_tasks.append(unzip_task)
        else:
            check_download = PythonOperator(
                task_id=f'check_download_{dataset_name}',
                python_callable=check_and_create_job,
                op_kwargs={'task_id': f'download_{dataset_name}', 'dataset_name': dataset_name, 'operation': 'download'}
            )

            download_task = PythonOperator(
                task_id=f'download_{dataset_name}',
                python_callable=process_download,
                op_kwargs={
                    'source': dataset_config['source'],
                    's3_bucket': s3_config['bucket_name'],
                    's3_key': f"{s3_config['data_prefix']}/{dataset_name}.zip",
                    'dataset_name': dataset_name,
                    'job_id': f"{dataset_name}_download_{{{{ execution_date }}}}"
                }
            )

            check_unzip = PythonOperator(
                task_id=f'check_unzip_{dataset_name}',
                python_callable=check_and_create_job,
                op_kwargs={'task_id': f'unzip_{dataset_name}', 'dataset_name': dataset_name, 'operation': 'unzip'}
            )

            unzip_task = PythonOperator(
                task_id=f'unzip_{dataset_name}',
                python_callable=process_unzip,
                op_kwargs={
                    's3_bucket': s3_config['bucket_name'],
                    's3_key': f"{s3_config['data_prefix']}/{dataset_name}.zip",
                    'destination_prefix': f"{s3_config['processed_folder']}/{dataset_name}",
                    'dataset_name': dataset_name,
                    'unzip_password': dataset_config.get('unzip_password'),
                    'job_id': f"{dataset_name}_unzip_{{{{ execution_date }}}}"
                }
            )

            start >> check_download >> download_task >> check_unzip >> unzip_task

        end = DummyOperator(task_id='end')

        if isinstance(dataset_config['source']['path'], list):
            for unzip_task in unzip_tasks:
                unzip_task >> end
        else:
            unzip_task >> end

    return dag

# Create a DAG for each dataset
config = get_config()  # Get the configuration once
for dataset in config['datasets']:
    dag_id = f"{dataset}_elt_dag"
    logger.info(f"Creating DAG: {dag_id}")
    globals()[dag_id] = create_dataset_dag(
        dataset,
        default_args={
            'owner': 'airflow',
            'start_date': datetime(2023, 1, 1),
            'retries': 0,
            'retry_delay': timedelta(minutes=1),
        }
    )
