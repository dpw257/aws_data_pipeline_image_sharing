from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator

#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': 'https://dbc-b54c5c54-233d.cloud.databricks.com/?o=1865928197306450#notebook/1715220261675406',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}
 
default_args = {
    'owner': '129076a9eaf9',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# Define running schedule
with DAG('129076a9eaf9_dag',
    start_date = datetime(2024, 3, 7),
    schedule_interval= timedelta(days=1),
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
