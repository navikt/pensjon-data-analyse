from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator
from images import get_image_name

WENDELBOE_IMAGE = get_image_name("wendelboe")


with DAG(
    "aldersovergang", start_date=days_ago(1), schedule_interval="0 1 2-8 * 2", catchup=False
) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-aldersovergang",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/aldersovergang.py",
        # image=WENDELBOE_IMAGE,
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dm08db03-vip.adeo.no:1521",  # prod lesekopien
        ],
    )
