from airflow import DAG
from datetime import datetime

from common.podop_factory import create_pod_operator


with DAG('dataproduct-psak-nytt-design', start_date=datetime(2023, 6, 29), schedule_interval="15 */1 * * *", catchup=False) as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="dataproduct-psak-nytt-design",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_psak_nytt_design.py",
        branch="main",
        retries=0
    )
    