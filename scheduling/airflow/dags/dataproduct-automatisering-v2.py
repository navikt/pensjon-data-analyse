from airflow import DAG
from kubernetes import client as k8s
from airflow.utils.dates import days_ago

from dataverk_airflow import python_operator


with DAG('dataproduct-automatisering-v2', start_date=days_ago(1), schedule_interval="22 4 1 * *", catchup=False) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-automatisering-v2",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_automatisering_v2.py",
        requirements_path="requirements.txt",
        branch="main",
        retries=0,
        resources=k8s.V1ResourceRequirements(
            requests={"memory": "10Gi", "cpu": "0.5", "ephemeral-storage": "5Gi"}
            )
    )