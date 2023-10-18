from airflow import DAG

from datetime import datetime
from common.podop_factory import create_pod_operator


with DAG('dataproduct-automatisering-v1', start_date=datetime(2023, 6, 28), schedule_interval="40 5 1 * *", catchup=False) as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="dataproduct-automatisering-v1",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_automatisering_v1.py",
        branch="main",
        retries=0
    )