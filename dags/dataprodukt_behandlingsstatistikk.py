import pendulum
from airflow import DAG
from datetime import datetime
from dataverk_airflow import python_operator
from operators.dbt_operator import dbt_operator


with DAG(
    dag_id="dataprodukt_behandlingsstatistikk",
    start_date=datetime(2025, 12, 8, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="0 1,6,11,21 * * *",  # Morgen, lønsj og kveld
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

    datalast_ufore_q2 = python_operator(
        dag=dag,
        name="datalast_ufore_q2",
        script_path="scripts/dvh_sak_ufore.py",
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        slack_channel="#pensak-airflow-alerts",
        use_uv_pip_install=True,
        python_version="3.12",
        extra_envs={"ENVIRONMENT": "dev"},
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv36-scan.adeo.no:1521",  # q2
        ],
    )

    datalast_ufore_prod = python_operator(
        dag=dag,
        name="datalast_ufore_prod",
        script_path="scripts/dvh_sak_ufore.py",
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        slack_channel="#pensak-airflow-alerts",
        use_uv_pip_install=True,
        python_version="3.12",
        extra_envs={"ENVIRONMENT": "prod"},
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv14-scan.adeo.no:1521",  # prod lesekopien
        ],
    )

    sak_ufore_q2 >> datalast_ufore_q2
    sak_ufore_prod >> datalast_ufore_prod