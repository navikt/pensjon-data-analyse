import pendulum
from datetime import datetime
from airflow import DAG

from operators.dbt_operator import dbt_operator


with DAG(
    dag_id="analyse_pen_dataprodukt",
    start_date=datetime(2026, 1, 14, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval=None,  # 00:01 every day - kjører som inkrementell på periode-feltet
    catchup=False,
) as dag:

    dbt_run_analyse_dev = dbt_operator(
        dag=dag,
        name="dbt_run_analyse_dev",
        startup_timeout_seconds=60 * 10,
        retries=0,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="run -s tag:analyse",
        allowlist=[
            "dmv36-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-q2-pen_dataprodukt",
        db_environment="pen_q2",
    )

    dbt_run_analyse_prod = dbt_operator(
        dag=dag,
        name="dbt_run_analyse_prod",
        startup_timeout_seconds=60 * 10,
        retries=0,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="run -s tag:analyse",
        allowlist=[
            "dmv18-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-prod-pen_dataprodukt",
        db_environment="pen_prod",
    )

    dbt_run_analyse_dev
    dbt_run_analyse_prod
