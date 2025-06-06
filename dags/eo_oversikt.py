from airflow import DAG
from datetime import datetime
from pendulum import timezone
from kubernetes import client as k8s
from dataverk_airflow import python_operator
from images import get_image_name

WENDELBOE_IMAGE = get_image_name("wendelboe")

# DAG for å oppdatere data til etteroppgjøret dataprodukter


with DAG(
    dag_id="eo_oversikt",
    description="Manuell kjøring for oppdatering av dataprodukter, altså BQ-tabeller",
    schedule_interval=None,
    start_date=datetime(2025, 6, 5, tzinfo=timezone("Europe/Oslo")),
    catchup=False,
) as dag:

    eo_oversikt = python_operator(
        dag=dag,
        name="eo_oversikt",
        script_path="scripts/eo_oversikt.py",
        image=WENDELBOE_IMAGE,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dm08db03-vip.adeo.no:1521",  # prod lesekopien
        ],
    )

    eo_oversikt
