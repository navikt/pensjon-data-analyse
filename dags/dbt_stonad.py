import pendulum
from airflow import DAG
from datetime import datetime
from dataverk_airflow import python_operator
from operators.dbt_operator import dbt_operator


with DAG(
    dag_id="dbt_stonad",
    start_date=datetime(2025, 6, 30, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="1 0 * * *",  # 00:01 every day - kjører som inkrementell på periode-feltet
    catchup=False,
) as dag:
    dbt_stonad_q2 = dbt_operator(
        dag=dag,
        name="dbt_stonad_q2",
        startup_timeout_seconds=60 * 10,
        retries=5,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="build --select +tag:stonad+ --select +tag:diagnosekoder+",
        allowlist=[
            "dmv36-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-q2-pen_dataprodukt",
        db_environment="pen_q2",
    )

    dbt_stonad_prod = dbt_operator(
        dag=dag,
        name="dbt_stonad_prod",
        startup_timeout_seconds=60 * 10,
        retries=5,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="build --select +tag:stonad+ --select +tag:diagnosekoder+",
        allowlist=[
            "dmv18-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-prod-pen_dataprodukt",
        db_environment="pen_prod",
    )

    datalast_stonad_q2 = python_operator(
        dag=dag,
        name="datalast_stonad_q2",
        script_path="scripts/dvh_stonad_alder.py",
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

    datalast_stonad_prod = python_operator(
        dag=dag,
        name="datalast_stonad_prod",
        script_path="scripts/dvh_stonad_alder.py",
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

    datalast_ufore_diagnosekoder_prod = python_operator(
        dag=dag,
        name="datalast_ufore_diagnosekoder_prod",
        script_path="scripts/ufore_diagnosekoder.py",
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        slack_channel="#pensak-airflow-alerts",
        use_uv_pip_install=True,
        python_version="3.12",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv14-scan.adeo.no:1521",  # prod lesekopien
        ],
    )

    dbt_stonad_q2 >> datalast_stonad_q2
    dbt_stonad_prod >> datalast_stonad_prod >> datalast_ufore_diagnosekoder_prod
