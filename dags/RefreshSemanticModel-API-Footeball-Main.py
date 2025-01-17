from datetime import datetime
# The DAG object
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_powerbi_plugin.operators.powerbi import PowerBIDatasetRefreshOperator

with DAG(
        dag_id='RefreshPBISemanticModel-API-Footeball-Main',
        schedule_interval=None,
        start_date=datetime(2023, 8, 7),
        catchup=False,
        concurrency=20,
        tags=["Public Projects","Refresh Semantic Model","API Footeball"],
) as dag:

    # [START howto_operator_powerbi_refresh_dataset]
    dataset_refresh = PowerBIDatasetRefreshOperator(
        powerbi_conn_id= "PowerBI_Default",
        task_id="RefreshSemanticModel-API-Footeball-Main",
        dataset_id="1c808e6d-11e4-4a40-952c-f7d8952b2429",
        group_id="31d140f3-20e4-4365-97ab-15e1348d6dea",
    )
    # [END howto_operator_powerbi_refresh_dataset]

    dataset_refresh
