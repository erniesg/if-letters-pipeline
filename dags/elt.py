from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.download import DownloadOperator
from operators.unzip import UnzipOperator
from helpers.dynamodb import create_job, update_job_state, get_job_state
from helpers.s3 import check_s3_object_exists, get_s3_object_info
from helpers.config import get_config
import logging

logger = logging.getLogger(__name__)

def create_dataset_dag(dataset_name, default_args):
    with DAG(
        dag_id=f'{dataset_name}_elt',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        dagrun_timeout=timedelta(minutes=60)
    ) as dag:
        config = get_config()
        dataset_config = config['datasets'][dataset_name]
        s3_config = config['s3']
        processing_config = config['processing']

        logger.info(f"Creating DAG for dataset: {dataset_name}")
        logger.info(f"S3 config: {s3_config}")
        logger.info(f"Dataset config: {dataset_config}")
        logger.info(f"Processing config: {processing_config}")

        start = DummyOperator(task_id='start')

        def get_download_path(dataset_name, file_index=None):
            base_path = f"{s3_config['data_prefix']}/{dataset_name}"
            return f"{base_path}_{file_index}.zip" if file_index is not None else f"{base_path}.zip"

        def get_unzip_path(dataset_name):
            return f"{s3_config['processed_folder']}/{dataset_name}"

        def check_and_create_job(task_id, dataset_name, operation, file_index=None, **context):
            execution_date = context['execution_date']
            job_id = f"{dataset_name}_{operation}_{execution_date}"
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

        def process_download(source, s3_bucket, s3_key, dataset_name, job_id, **context):
            logger.info(f"Processing download for {dataset_name}")
            logger.info(f"S3 bucket: {s3_bucket}, S3 key: {s3_key}")

            if source['type'] == 's3':
                s3_key = source['path']
                logger.info(f"S3 source detected. Using configured path: {s3_key}")
                update_job_state(job_id, status="completed", progress=100)
                return 'skip'

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
            result = download_op.execute(context)
            logger.info(f"Download result: {result}")
            update_job_state(job_id, status="completed", progress=100, metadata=result)
            return result

        def create_unzip_task(dataset_name, s3_key, file_index=None):
            task_id = f'unzip_{dataset_name}' if file_index is None else f'unzip_{dataset_name}_{file_index}'
            return UnzipOperator(
                task_id=task_id,
                dataset_name=dataset_name,
                s3_keys=s3_key,
                s3_bucket=s3_config['bucket_name'],
                destination_prefix=get_unzip_path(dataset_name),
                max_concurrency=dataset_config.get('max_concurrency', processing_config.get('max_concurrency', 32)),
                buffer_size=processing_config.get('buffer_size', 8*1024*1024),
                buffer_pool_size=processing_config.get('buffer_pool_size', 64),
                max_pool_connections=dataset_config.get('max_pool_connections', processing_config.get('max_pool_connections', 100)),
                batch_size=dataset_config.get('batch_size', processing_config.get('batch_size', 500)),
                chunk_size=dataset_config.get('chunk_size', processing_config.get('chunk_size', 128 * 1024 * 1024)),
            )

        if isinstance(dataset_config['source']['path'], list):
            for i, path in enumerate(dataset_config['source']['path']):
                check_download = PythonOperator(
                    task_id=f'check_download_{dataset_name}_{i}',
                    python_callable=check_and_create_job,
                    op_kwargs={'task_id': f'download_{dataset_name}_{i}', 'dataset_name': dataset_name, 'operation': 'download', 'file_index': i},
                    provide_context=True
                )

                download_task = PythonOperator(
                    task_id=f'download_{dataset_name}_{i}',
                    python_callable=process_download,
                    op_kwargs={
                        'source': {'type': dataset_config['source']['type'], 'path': path},
                        's3_bucket': s3_config['bucket_name'],
                        's3_key': get_download_path(dataset_name, i),
                        'dataset_name': dataset_name,
                        'job_id': f"{dataset_name}_download_{i}_{{{{ execution_date }}}}"
                    },
                    provide_context=True
                )

                check_unzip = PythonOperator(
                    task_id=f'check_unzip_{dataset_name}_{i}',
                    python_callable=check_and_create_job,
                    op_kwargs={'task_id': f'unzip_{dataset_name}_{i}', 'dataset_name': dataset_name, 'operation': 'unzip', 'file_index': i},
                    provide_context=True
                )

                s3_key = get_download_path(dataset_name, i)
                unzip_task = create_unzip_task(dataset_name, s3_key, i)

                start >> check_download >> download_task >> check_unzip >> unzip_task

        else:
            check_download = PythonOperator(
                task_id=f'check_download_{dataset_name}',
                python_callable=check_and_create_job,
                op_kwargs={'task_id': f'download_{dataset_name}', 'dataset_name': dataset_name, 'operation': 'download'},
                provide_context=True
            )

            download_task = PythonOperator(
                task_id=f'download_{dataset_name}',
                python_callable=process_download,
                op_kwargs={
                    'source': dataset_config['source'],
                    's3_bucket': s3_config['bucket_name'],
                    's3_key': get_download_path(dataset_name),
                    'dataset_name': dataset_name,
                    'job_id': f"{dataset_name}_download_{{{{ execution_date }}}}"
                },
                provide_context=True
            )

            check_unzip = PythonOperator(
                task_id=f'check_unzip_{dataset_name}',
                python_callable=check_and_create_job,
                op_kwargs={'task_id': f'unzip_{dataset_name}', 'dataset_name': dataset_name, 'operation': 'unzip'},
                provide_context=True
            )

            s3_key = dataset_config['source']['path'] if dataset_config['source']['type'] == 's3' else get_download_path(dataset_name)
            unzip_task = create_unzip_task(dataset_name, s3_key)

            start >> check_download >> download_task >> check_unzip >> unzip_task

        end = DummyOperator(task_id='end')
        if isinstance(dataset_config['source']['path'], list):
            for i in range(len(dataset_config['source']['path'])):
                dag.get_task(f'unzip_{dataset_name}_{i}') >> end
        else:
            unzip_task >> end

    return dag

# Create a DAG for each dataset
config = get_config()
for dataset in config['datasets']:
    dag_id = f"{dataset}_elt_dag"
    logger.info(f"Starting creation of DAG: {dag_id}")
    try:
        globals()[dag_id] = create_dataset_dag(
            dataset,
            default_args={
                'owner': 'airflow',
                'start_date': datetime(2023, 1, 1),
                'retries': 0,
                'retry_delay': timedelta(minutes=1),
            }
        )
        logger.info(f"Successfully created DAG: {dag_id}")
    except Exception as e:
        logger.error(f"Error creating DAG {dag_id}: {str(e)}")
        raise
