import pendulum
from datetime import datetime
from airflow import DAG

from operators.dbt_operator import dbt_operator


with DAG(
    dag_id="dbt_snapshot_analyse",
    start_date=datetime(2025, 9, 23, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="0 0,1,21,22,23 * * *",  # 9, 10, 11 PM and 0, 1 AM every day
    catchup=False,
) as dag:
    snapshot_q2 = dbt_operator(
        dag=dag,
        name="snapshot_q2",
        startup_timeout_seconds=60 * 10,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="snapshot --select tag:work-in-progress",
        allowlist=[
            "dmv36-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-q2-pen_dataprodukt",
        db_environment="pen_q2",
    )

    snapshot_prod = dbt_operator(
        dag=dag,
        name="snapshot_prod",
        startup_timeout_seconds=60 * 10,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="snapshot --select tag:work-in-progress",
        allowlist=[
            "dmv18-scan.adeo.no:1521",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-prod-pen_dataprodukt",
        db_environment="pen_prod",
    )

    snapshot_q2
    snapshot_prod
