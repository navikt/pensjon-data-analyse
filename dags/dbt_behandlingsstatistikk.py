import pendulum
from airflow import DAG
from datetime import datetime
from operators.dbt_operator import dbt_operator


with DAG(
    dag_id="dbt_behandlingsstatistikk",
    start_date=datetime(2025, 12, 8, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="0 6,11,21 * * *",  # Morgen, lønsj og kveld
    catchup=False,
) as dag:
    sak_ufore_q2 = dbt_operator(
        dag=dag,
        name="sak_ufore_q2",
        startup_timeout_seconds=60 * 10,
        retries=5,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="build -s +tag:sak",
        allowlist=[
            "dmv36-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-q2-pen_dataprodukt",
        db_environment="pen_q2",
    )

    sak_ufore_prod = dbt_operator(
        dag=dag,
        name="sak_ufore_prod",
        startup_timeout_seconds=60 * 10,
        retries=5,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="build -s +tag:sak",
        allowlist=[
            "dmv18-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-prod-pen_dataprodukt",
        db_environment="pen_prod",
    )

    sak_ufore_q2
    sak_ufore_prod