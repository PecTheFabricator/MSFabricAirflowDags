from datetime import datetime
# The DAG object
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_powerbi_plugin.operators.powerbi import PowerBIDatasetRefreshOperator

with DAG(
        dag_id='refresh_dataset_powerbi',
        schedule_interval=None,
        start_date=datetime(2023, 8, 7),
        catchup=False,
        concurrency=20,
) as dag:

    # [START howto_operator_powerbi_refresh_dataset]
    dataset_refresh = PowerBIDatasetRefreshOperator(
        powerbi_conn_id= "powerbi_default",
        task_id="sync_dataset_refresh",
        dataset_id="<dataset_id>,
        group_id="<workspace_id>",
    )
    # [END howto_operator_powerbi_refresh_dataset]

    dataset_refresh
