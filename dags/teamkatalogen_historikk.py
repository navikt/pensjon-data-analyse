from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator


with DAG(
    dag_id="teamkatalogen_historikk_dbt",
    schedule_interval="0 7 * * *",
    start_date=datetime(2025, 4, 4, tzinfo=timezone("Europe/Oslo")),
    doc_md="Kjører daglig dbt snapshot av de fire tabellene fra teamkatalogen.",
    catchup=False,
) as dag:
    dbt_snapshot = python_operator(
        dag=dag,
        use_uv_pip_install=True,
        name="historiserer-fire-tabeller",
        repo="navikt/teamkatalogen-historikk",
        script_path="dbt/dbt_run_airflow.py",
        allowlist=["bigquery.googleapis.com"],
        # slack_channel="#pensak-airflow-alerts",
        requirements_path="requirements.txt",
    )

    dbt_snapshot
