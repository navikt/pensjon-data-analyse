from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from kubernetes import client as k8s
from datetime import datetime


def run_update_dataproduct():
    from scripts.dataproduct_kravstatus import update_dataproduct
    
    update_dataproduct()
    

with DAG('dataproduct-psak-nytt-design', start_date=datetime(2023, 6, 29), schedule_interval="15 */1 * * *") as dag:    
    run_this = PythonOperator(
        task_id='dataproduct-psak-nytt-design',
        python_callable=run_update_dataproduct,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                    k8s.V1Container(
                        name="base",
                        image="ghcr.io/navikt/airflow-pensjon-sb:v0",
                        working_dir="/dags/scripts",
                    )
                    ]
                ),
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "dm08db03.adeo.no:1521"})
            )
        },
    dag=dag)
