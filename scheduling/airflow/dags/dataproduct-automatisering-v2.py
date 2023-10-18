from airflow import DAG
from kubernetes import client as k8s
from datetime import datetime

from common.podop_factory import create_pod_operator


with DAG('dataproduct-automatisering-v2', start_date=datetime(2023, 6, 28), schedule_interval="22 4 1 * *", catchup=False) as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="dataproduct-automatisering-v2",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_automatisering_v2.py",
        branch="main",
        retries=0,
        resources=k8s.V1ResourceRequirements(
            requests={"memory": "10Gi", "cpu": "0.5", "ephemeral-storage": "5Gi"}
            )
    )