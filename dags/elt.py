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
       dagrun_timeout=timedelta(minutes=60)  # Adjust this value as needed
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
                # For S3 sources, we skip the download step
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

        def process_unzip(dataset_name, **context):
            batch_size = dataset_config.get('batch_size', processing_config['batch_size'])
            num_workers = dataset_config.get('num_workers', processing_config['num_workers'])
            max_pool_connections = num_workers * 2
            max_concurrency = num_workers

            if isinstance(dataset_config['source']['path'], list):
                unzip_tasks = []
                for i, path in enumerate(dataset_config['source']['path']):
                    s3_key = f"{s3_config['data_prefix']}/{dataset_name}_{i}.zip"
                    unzip_task = PythonOperator(
                        task_id=f'unzip_{dataset_name}_{i}',
                        python_callable=execute_unzip,
                        op_kwargs={
                            's3_key': s3_key,
                            'dataset_name': dataset_name,
                            'file_index': i,
                            'max_pool_connections': max_pool_connections,
                            'max_concurrency': max_concurrency,
                            'batch_size': batch_size
                        },
                        provide_context=True
                    )
                    unzip_tasks.append(unzip_task)
                return unzip_tasks
            else:
                s3_key = dataset_config['source']['path'] if dataset_config['source']['type'] == 's3' else f"{s3_config['data_prefix']}/{dataset_name}.zip"
                return PythonOperator(
                    task_id=f'unzip_{dataset_name}',
                    python_callable=execute_unzip,
                    op_kwargs={
                        's3_key': s3_key,
                        'dataset_name': dataset_name,
                        'max_pool_connections': max_pool_connections,
                        'max_concurrency': max_concurrency,
                        'batch_size': batch_size
                    },
                    provide_context=True
                )

        def execute_unzip(s3_key, dataset_name, max_pool_connections, max_concurrency, batch_size, file_index=None, **context):
            execution_date = context['execution_date']
            job_id = f"{dataset_name}_unzip_{execution_date}"
            if file_index is not None:
                job_id += f"_{file_index}"

            logger.info(f"Processing unzip for {dataset_name}")
            logger.info(f"S3 key: {s3_key}")

            unzip_op = UnzipOperator(
                task_id=f'unzip_{dataset_name}',
                dataset_name=dataset_name,
                s3_key=s3_key,
                max_pool_connections=max_pool_connections,
                max_concurrency=max_concurrency,
                batch_size=batch_size
            )
            result = unzip_op.execute(context)
            logger.info(f"Unzip result: {result}")
            update_job_state(job_id, status="completed", progress=100, metadata=result)
            return result

        if isinstance(dataset_config['source']['path'], list):
            download_tasks = []
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
                        's3_key': f"{s3_config['data_prefix']}/{dataset_name}_{i}.zip",
                        'dataset_name': dataset_name,
                        'job_id': f"{dataset_name}_download_{i}_{{{{ execution_date }}}}"
                    },
                    provide_context=True
                )

                start >> check_download >> download_task
                download_tasks.append(download_task)

            check_unzip = PythonOperator(
                task_id=f'check_unzip_{dataset_name}',
                python_callable=check_and_create_job,
                op_kwargs={'task_id': f'unzip_{dataset_name}', 'dataset_name': dataset_name, 'operation': 'unzip'},
                provide_context=True
            )

            unzip_tasks = process_unzip(dataset_name)
            for download_task, unzip_task in zip(download_tasks, unzip_tasks):
                download_task >> check_unzip >> unzip_task

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
                    's3_key': f"{s3_config['data_prefix']}/{dataset_name}.zip",
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

            unzip_task = process_unzip(dataset_name)

            start >> check_download >> download_task >> check_unzip >> unzip_task

        end = DummyOperator(task_id='end')

        if isinstance(dataset_config['source']['path'], list):
            for unzip_task in unzip_tasks:
                unzip_task >> end
        else:
            unzip_task >> end

    return dag

# Create a DAG for each dataset
config = get_config()
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
