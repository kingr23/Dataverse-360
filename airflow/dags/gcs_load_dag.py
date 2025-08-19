from __future__ import annotations
import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

GCS_BUCKET = "dataverse360-raw-data-rmk" # Your GCS bucket name

with DAG(
    dag_id='gcs_move_landing_to_raw_v1',
    description='Moves files from a landing to a raw folder in GCS.',
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 2, tz="UTC"),
    catchup=False,
    tags=['gcs', 'gcs-to-gcs'],
) as dag:
    
    start_task = EmptyOperator(task_id='start')

    move_cards_file = GCSToGCSOperator(
        task_id='move_cards_file',
        source_bucket=GCS_BUCKET,
        source_object='landing/cards_data.csv',
        destination_bucket=GCS_BUCKET,
        destination_object='raw/cards_data.csv',
        gcp_conn_id='google_cloud_default',
    )

    move_transactions_file = GCSToGCSOperator(
        task_id='move_transactions_file',
        source_bucket=GCS_BUCKET,
        source_object='landing/transaction_data.csv',
        destination_bucket=GCS_BUCKET,
        destination_object='raw/transaction_data.csv',
        gcp_conn_id='google_cloud_default',
    )
    
    move_users_file = GCSToGCSOperator(
        task_id='move_users_file',
        source_bucket=GCS_BUCKET,
        source_object='landing/users_data.csv',
        destination_bucket=GCS_BUCKET,
        destination_object='raw/users_data.csv',
        gcp_conn_id='google_cloud_default',
    )

    end_task = EmptyOperator(task_id='end')
    start_task >> [move_cards_file, move_transactions_file, move_users_file] >> end_task