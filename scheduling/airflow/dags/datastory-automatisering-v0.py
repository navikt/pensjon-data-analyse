from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from kubernetes import client as k8s
from datetime import datetime

from scripts.datastory_automatisering import update_datastory

def run_update_datastory():
    update_datastory()
    

with DAG('datastory-automatisering-v0', start_date=datetime(2023, 6, 1), schedule_interval="0 0 1 * *") as dag:    
    run_this = PythonOperator(
    task_id='datastory-automatising',
    python_callable=run_update_datastory,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                k8s.V1Container(
                    name="base",
                    image="europe-west1-docker.pkg.dev/knada-gcp/knada/airflow-papermill:2023-03-22-fb1c4a4"
                )
                ]
            )
        )
    },
    dag=dag)