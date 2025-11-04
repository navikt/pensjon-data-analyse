from airflow import DAG
from datetime import datetime
from pendulum import timezone
from kubernetes import client as k8s
from dataverk_airflow import python_operator

# DAG for ukentlig oppdaterte dataprodukter
# Kan være data som ikke trenger å oppdateres daglig
# eller spørringer som tar litt tid, eller begge


def python_operator_wrapped(
    *,  # Enforce keyword-only arguments
    dag: DAG,
    name: str,
    script_path: str,
    resources: k8s.V1ResourceRequirements = None, # type: ignore
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
            "dmv14-scan.adeo.no:1521",  # prod lesekopien
        ],
        resources=resources,
        python_version="3.12",
    )


with DAG(
    dag_id="dataprodukter_ukentlig",
    description="Ukentlig oppdatering av dataprodukter, altså BQ-tabeller",
    schedule_interval="15 5 * * sun",
    start_date=datetime(2025, 3, 12, tzinfo=timezone("Europe/Oslo")),
    catchup=False,
) as dag:
    feilutbetalinger = python_operator_wrapped(
        dag=dag,
        name="feilutbetalinger",
        script_path="scripts/feilutbetalinger.py",
    )
    etteroppgjoret = python_operator_wrapped(
        dag=dag,
        name="etteroppgjoret",
        script_path="scripts/eo_oversikt.py",
        # eo_oversikt.sql, eo_oversikt_per_dag.sql, eo_varselbrev_sluttresultat.sql, eo_varselbrev_tidslinje.sql
    )

    feilutbetalinger
    etteroppgjoret
