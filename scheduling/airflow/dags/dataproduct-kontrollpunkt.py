from airflow import DAG

from kubernetes import client as k8s
from datetime import datetime


with DAG('dataproduct-kontrollpunkt', start_date=datetime(2023, 6, 28), schedule_interval="10 06 * * *", catchup=False) as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="dataproduct-kontrollpunkt",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_kontrollpunkt.py",
        branch="main",
        retries=0,
        resources=k8s.V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "0.5", "ephemeral-storage": "5Gi"}
            )
    )