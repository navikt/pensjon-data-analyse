from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from kubernetes import client as k8s
from datetime import datetime


def run_update_quarto():
    from scripts.quarto_glemte_krav import update_quarto
    
    update_quarto()
    

with DAG('quarto-glemte-krav', start_date=datetime(2023, 8, 25), schedule_interval="8 3 * * *") as dag:    
    run_this = PythonOperator(
        task_id='quarto-glemte-krav',
        python_callable=run_update_quarto,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                    k8s.V1Container(
                        name="base",
                        image="ghcr.io/navikt/airflow-pensjon-sb:v1",
                        working_dir="/dags/scripts",
                    )
                    ]
                ),
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "dm08db03.adeo.no:1521,datamarkedsplassen.intern.nav.no"})
            )
        },
    dag=dag)
