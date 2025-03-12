from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator
from kubernetes import client as k8s

# DAG for de tre dataproduktene Vebjørn lagde som oppdateres daglig.
# Bør vurderes om de kan saneres dersom de ikke er i bruk.
# De kan være bruk i Metabase.


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
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        # slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dm09-scan.adeo.no:1521",
        ],
        resources=resources,
    )


with DAG(
    dag_id="daglige_dataprodukter",
    description="Daglig oppdatering av (3 gamle) dataprodukter, altså BQ-tabeller",
    schedule_interval="15 6 * * *",
    start_date=datetime(2025, 3, 12, tzinfo=timezone("Europe/Oslo")),
    doc_md="De tre dataproduktene Vebjørn lagde som oppdateres daglig.",
    catchup=False,
) as dag:
    laaste_vedtak = python_operator_wrapped(
        dag=dag,
        name="laaste-vedtak",
        script_path="scripts/dataproduct_laaste_vedtak.py",
    )

    kravstatus = python_operator_wrapped(
        dag=dag,
        name="kravstatus",
        script_path="scripts/dataproduct_kravstatus.py",
    )

    kontrollpunkt = python_operator_wrapped(
        dag=dag,
        name="kontrollpunkt",
        script_path="scripts/dataproduct_kontrollpunkt.py",
        resources=k8s.V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "0.5", "ephemeral-storage": "1Gi"}
        ),
    )

    laaste_vedtak
    kravstatus
    kontrollpunkt
