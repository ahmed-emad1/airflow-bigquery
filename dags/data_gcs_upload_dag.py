import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(src_file):
    if not src_file.endswith('.csv.gz'):
        logging.error("Can only accept source files in compressed CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_gcs_upload_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    
    previous_task = None  # Variable to chain tasks sequentially

    for month in range(1, 13):
        month_str = f"{month:02d}"  # Zero-pad month to 2 digits
        
        # switch the dataset file depending on your needs
        # dataset_file = f"yellow_tripdata_2020-{month_str}.csv.gz"
        dataset_file = f"yellow_tripdata_2020-{month_str}.csv.gz"

        parquet_file = dataset_file.replace('.csv.gz', '.parquet')
        dataset_url = f"{base_url}{dataset_file}"
        
        # Task to download the dataset
        download_dataset_task = BashOperator(
            task_id=f"download_dataset_task_{month}",
            bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
        )

        # Task to convert the CSV to Parquet
        format_to_parquet_task = PythonOperator(
            task_id=f"format_to_parquet_task_{month}",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{dataset_file}",
            },
        )

        # Task to upload the Parquet file to GCS
        local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_task_{month}",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"trip_data/{parquet_file}",
                "local_file": f"{path_to_local_home}/{parquet_file}",
            },
        )

        # Chaining tasks sequentially for each month
        if previous_task:
            previous_task >> download_dataset_task
        
        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task

        # Update previous_task to the last task in the chain
        previous_task = local_to_gcs_task
