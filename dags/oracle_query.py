from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator

with DAG(
    dag_id="oracle_query",
    description="Manuell kjøring av en Oracle-spørring som printer resultatet",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone("Europe/Oslo")),
    catchup=False,
) as dag:
    task = python_operator(
        dag=dag,
        name="task",
        script_path="scripts/manuell_oracle_query.py",
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "dmv14-scan.adeo.no:1521",  # prod lesekopien
        ],
        python_version="3.12",
    )

    task