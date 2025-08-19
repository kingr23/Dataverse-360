import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='dbt_run_pipeline_v1',
    start_date=pendulum.datetime(2025, 8, 8, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['dbt'],
) as dag:
    
    # This task represents the successful completion of your data loading
    data_load_complete = EmptyOperator(
        task_id='data_load_complete'
    )

    # This task runs your dbt project
    dbt_run_task = BashOperator(
        task_id='dbt_run',
        # The command to execute
        bash_command='dbt run',
        # The directory where the command should be run from
        cwd='/opt/airflow/dbt',
    )

    data_load_complete >> dbt_run_task