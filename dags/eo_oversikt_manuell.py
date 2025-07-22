from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator
from images import get_image_name

WENDELBOE_IMAGE = get_image_name("wendelboe")

# DAG for å oppdatere data til etteroppgjøret dataprodukter


with DAG(
    dag_id="eo_oversikt_manuell",
    description="Manuell kjøring for oppdatering av dataprodukter, altså BQ-tabeller",
    schedule_interval=None,
    start_date=datetime(2025, 6, 5, tzinfo=timezone("Europe/Oslo")),
    catchup=False,
) as dag:
    oracle_til_bigquery = python_operator(
        dag=dag,
        name="oracle_til_bigquery",
        script_path="scripts/eo_oversikt.py",
        # image=WENDELBOE_IMAGE,
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dm08db03-vip.adeo.no:1521",  # prod lesekopien
        ],
        python_version="3.12",
    )

    oracle_til_bigquery
