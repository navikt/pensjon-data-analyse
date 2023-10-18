from airflow import DAG

from kubernetes import client as k8s
from datetime import datetime

from common.podop_factory import create_pod_operator


with DAG('dataproduct-kravstatus', start_date=datetime(2023, 6, 29), schedule_interval="05 06 * * *", catchup=False) as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="dataproduct-kravstatus",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_kravstatus.py",
        branch="main",
        retries=0
    )