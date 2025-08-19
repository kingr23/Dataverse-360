from __future__ import annotations
import pendulum
import pandas as pd
import json
import os

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

GCS_BUCKET = "dataverse360-raw-data-rmk" # Your GCS bucket name

def process_json_and_upload(json_file_name: str, **kwargs):
    """Downloads JSON from GCS landing, transforms it from a key-value
    object to a two-column CSV, and uploads it to the raw folder."""
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # 1. Download JSON from landing using the correct method
    json_data_bytes = gcs_hook.download(
        bucket_name=GCS_BUCKET,
        object_name=f'landing/{json_file_name}',
    )
    
    # Decode the bytes into a string
    json_data_string = json_data_bytes.decode('utf-8')
    data = json.loads(json_data_string)
    
    # 2. Transform the key-value data into a DataFrame with two columns
    df = pd.DataFrame(data.items(), columns=['mcc_code', 'description'])
    
    # 3. Upload transformed data as CSV to raw
    csv_data = df.to_csv(index=False)
    
    base_name = os.path.splitext(json_file_name)[0]
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=f'raw/{base_name}.csv',
        data=csv_data,
        mime_type='text/csv',
    )


with DAG(
    dag_id='gcs_process_all_files_v1',
    start_date=pendulum.datetime(2025, 8, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['gcs', 'final'],
) as dag:
    
    start_task = EmptyOperator(task_id='start')

    # Tasks for the simple CSV copies
    move_cards_file = GCSToGCSOperator(
        task_id='move_cards_file', 
        source_bucket=GCS_BUCKET, 
        source_object='landing/cards_data.csv', 
        destination_bucket=GCS_BUCKET, 
        destination_object='raw/cards_data.csv', 
        gcp_conn_id='google_cloud_default'
    )
    
    move_transactions_file = GCSToGCSOperator(
        task_id='move_transactions_file', 
        source_bucket=GCS_BUCKET, 
        source_object='landing/transactions_data.csv', 
        destination_bucket=GCS_BUCKET, 
        destination_object='raw/transaction_data.csv', 
        gcp_conn_id='google_cloud_default'
    )
    
    move_users_file = GCSToGCSOperator(
        task_id='move_users_file', 
        source_bucket=GCS_BUCKET, 
        source_object='landing/users_data.csv', 
        destination_bucket=GCS_BUCKET, 
        destination_object='raw/users_data.csv', 
        gcp_conn_id='google_cloud_default'
    )

    # Task for the complex JSON transformation
    process_json_file = PythonOperator(
        task_id='process_json_file',
        python_callable=process_json_and_upload,
        op_kwargs={'json_file_name': 'mcc_codes.json'},
    )

    end_task = EmptyOperator(task_id='end')
    # Define parallel execution for all tasks
    start_task >> [
        move_cards_file, 
        move_transactions_file, 
        move_users_file, 
        process_json_file
    ] >> end_task
 