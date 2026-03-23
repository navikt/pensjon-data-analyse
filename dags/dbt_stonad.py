import pendulum
from airflow import DAG
from datetime import datetime
from operators.dbt_operator import dbt_operator


with DAG(
    dag_id="dbt_stonad",
    start_date=datetime(2025, 6, 30, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="1 0 * * *",  # 00:01 every day - kjører som inkrementell på periode-feltet
    catchup=False,
) as dag:
    stonad_alder_q2 = dbt_operator(
        dag=dag,
        name="stonad_alder_q2",
        startup_timeout_seconds=60 * 10,
        retries=5,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="build --exclude sql_pilot_original --exclude tag:analyse --exclude tag:sak",
        allowlist=[
            "dmv36-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-q2-pen_dataprodukt",
        db_environment="pen_q2",
    )

    stonad_alder_prod = dbt_operator(
        dag=dag,
        name="stonad_alder_prod",
        startup_timeout_seconds=60 * 10,
        retries=5,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="build --exclude sql_pilot_original --exclude tag:analyse --exclude tag:sak",
        allowlist=[
            "dmv18-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-prod-pen_dataprodukt",
        db_environment="pen_prod",
    )

    stonad_alder_q2
    stonad_alder_prod
