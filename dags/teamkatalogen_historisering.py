from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator

with DAG(
    dag_id="teamkatalogen_historisering",
    schedule_interval="0 21 1 * *",
    start_date=datetime(2025, 4, 1, tzinfo=timezone("Europe/Oslo")),
    doc_md="Kjører en månedlig historisering av fire tabeller fra teamkatalogen.",
    catchup=False,
) as dag:
    historisering = python_operator(
        dag=dag,
        retries=0,
        use_uv_pip_install=True,
        name="historiserer-fire-tabeller",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/teamkatalogen_historisering.py",
        allowlist=["bigquery.googleapis.com"],
        slack_channel="#pensak-airflow-alerts",
        requirements_path="requirements.txt",
    )

    historisering
