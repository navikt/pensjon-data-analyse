from airflow import DAG
from datetime import datetime
from pendulum import timezone
from kubernetes import client as k8s
from dataverk_airflow import python_operator



def python_operator_wrapped(
    *,  # Enforce keyword-only arguments
    dag: DAG,
    name: str,
    script_path: str,
    resources: k8s.V1ResourceRequirements = None,
):
    """Wrapper dataverk_airflow.python_operator with default arguments."""
    return python_operator(
        dag=dag,
        name=name,
        script_path=script_path,
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "norg2.intern.nav.no",  # api'er som brukes
        ],
        resources=resources,
        python_version="3.12",
    )


with DAG(
    dag_id="last_fra_api",
    description="Daglig oppdatering av dataprodukter, altså BQ-tabeller",
    schedule_interval="0 6 1 * *", # den 1. hver måned kl 06
    start_date=datetime(2026, 6, 1, tzinfo=timezone("Europe/Oslo")),
    catchup=False,
) as dag:
    last_norg2_intern_nav = python_operator_wrapped(
        dag=dag,
        name="last_norg2_intern_nav",
        script_path="scripts/last_norg2_intern_nav.py",
    )

    last_norg2_intern_nav