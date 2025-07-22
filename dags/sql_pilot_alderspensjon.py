import pendulum
from datetime import datetime
from airflow import DAG

from operators.dbt_operator import dbt_operator


with DAG(
    dag_id="sql_pilot_alderspensjon",
    start_date=datetime(2025, 6, 30, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="0 7 1 * *",  # “7 AM on the 1st day of every month”
    catchup=False,
) as dag:
    sql_pilot_q2 = dbt_operator(
        dag=dag,
        name="sql_pilot_q2",
        startup_timeout_seconds=60 * 10,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="run",
        allowlist=[
            "dmv36-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-q2-pen_dataprodukt",
        db_environment="pen_q2",
    )

    sql_pilot_prod = dbt_operator(
        dag=dag,
        name="sql_pilot_prod",
        startup_timeout_seconds=60 * 10,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="run",
        allowlist=[
            "dmv18-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-prod-pen_dataprodukt",
        db_environment="pen_prod",
    )

    sql_pilot_q2 >> sql_pilot_prod
