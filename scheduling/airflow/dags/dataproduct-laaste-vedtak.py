from airflow import DAG

from kubernetes import client as k8s
from datetime import datetime

from common.podop_factory import create_pod_operator


with DAG('dataproduct-laaste-vedtak', start_date=datetime(2023, 6, 28), schedule_interval="15 06 * * *", catchup=False) as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="dataproduct-laaste-vedtak",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataprodukt_laaste_vedtak.py",
        branch="main",
        retries=0
    )