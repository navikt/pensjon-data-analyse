import logging
from datetime import datetime

import pendulum
from airflow import DAG
from dataverk_airflow import python_operator



with DAG(
    dag_id="dvh_saksbehandlingsstatistikk",
    start_date=datetime(2026, 10, 25, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="0 6 * * *",  # 06:00 every day
    catchup=False,
) as dag:
    datalast_dvh_sbs_prod = python_operator(
        dag=dag,
        name="datalast_dvh_sbs_prod",
        script_path="scripts/dvh_saksbehandlingsstatistikk.py",
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        slack_channel="#pensak-airflow-alerts",
        use_uv_pip_install=True,
        python_version="3.14",
        extra_envs={"ENVIRONMENT": "prod"},
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv03-scan.adeo.no:1521",  # prod lesekopien
        ],
    )
    datalast_dvh_sbs_prod

