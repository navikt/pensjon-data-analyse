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
                        image="ghcr.io/navikt/airflow-pensjon-sb:v0"
                    )
                    ]
                ),
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "dm08db03.adeo.no:1521,data.intern.nav.no"})
            )
        },
    dag=dag)
    