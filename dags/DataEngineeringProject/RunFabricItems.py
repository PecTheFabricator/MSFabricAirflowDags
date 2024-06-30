from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    #'retries': 1,
}

# Function to dynamically add authentication token to headers
def add_auth_token(**kwargs):
    auth_token = Variable.get("AzAppAuthToken")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}"
    }
    return headers

# Define the DAG
dag = DAG(
    'RunMSFabricItems',
    default_args=default_args,
    description='A simple DAG to perform a POST request with an authentication token',
    schedule_interval='@daily',
)

# Task to get the authentication headers
get_auth_headers = PythonOperator(
    task_id='get_auth_headers',
    python_callable=add_auth_token,
    provide_context=True,
    dag=dag
)

# Task to perform the POST request
AzCostManage_Transformation_dag = SimpleHttpOperator(
    task_id='AzCostManage_Transformation',
    http_conn_id='https://api.fabric.microsoft.com/v1/',  # Replace with your connection ID
    endpoint='workspaces/31d140f3-20e4-4365-97ab-15e1348d6dea/items/ccd4ed3e-30ac-4ff8-87c6-a65b4483f833/jobs/instances?jobType=DefaultJob',  # Replace with your endpoint
    method='POST',
    headers="{{ task_instance.xcom_pull(task_ids='get_auth_headers') }}",
    data=json.dumps({"key": "dsda"}),  # Replace with your data payload
    response_check=lambda response: response.status_code == 200,
    log_response=True,
    dag=dag,
)

# Define the task dependencies
get_auth_headers >> AzCostManage_Transformation_dag
