from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from kubernetes import client as k8s
from datetime import datetime


def run_overwrite_dataproduct():
    from scripts.dataproduct_automatisering_v2 import overwrite_dataproduct
    
    overwrite_dataproduct()
    

with DAG('dataproduct-automatisering-v2', start_date=datetime(2023, 6, 28), schedule_interval="0 0 1 * *") as dag:    
    run_this = PythonOperator(
        task_id='dataproduct-automatisering-v2',
        python_callable=run_overwrite_dataproduct,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                    k8s.V1Container(
                        name="base",
                        image="ghcr.io/navikt/airflow-pensjon-sb:v0",
                        working_dir="/dags/scripts",
                        resources={
                           "requests": {
                               "cpu": "0.5",
                               "memory": "10Gi",
                               "ephemeral-storage": "5Gi"
                           }
                         }
                    )
                    ]
                ),
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "dm08db03.adeo.no:1521,data.intern.nav.no"})
            )
        },
    dag=dag)
