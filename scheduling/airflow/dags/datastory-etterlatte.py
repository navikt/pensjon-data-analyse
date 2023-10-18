from airflow import DAG

from common.podop_factory import create_pod_operator
from common.notifications import create_slack_notification
from datetime import datetime    


with DAG('datastory-etterlatte', start_date=datetime(2023, 6, 1), schedule_interval="30 5 * * 1", catchup=False) as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="datastory-etterlatte",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/datastory_etterlatte.py",
        branch="main",
        retries=0
    )