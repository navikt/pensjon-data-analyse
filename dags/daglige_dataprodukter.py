from airflow import DAG
from datetime import datetime
from pendulum import timezone
from kubernetes import client as k8s
from dataverk_airflow import python_operator

# DAG for daglig oppdaterte dataprodukter

# Bør vudere om de tre dataproduktene Vebjørn lagde kan saneres.
# Først kartlegg om de er i bruk, feks i Metabase. Det gjelder:
# - laaste_vedtak
# - kravstatus
# - kontrollpunkt


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
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dm08db03-vip.adeo.no:1521",  # prod lesekopien
        ],
        resources=resources,
    )


with DAG(
    dag_id="daglige_dataprodukter",
    description="Daglig oppdatering av dataprodukter, altså BQ-tabeller",
    schedule_interval="15 5 * * *",
    start_date=datetime(2025, 3, 12, tzinfo=timezone("Europe/Oslo")),
    catchup=False,
) as dag:
    inntektsendring_og_autobrev = python_operator_wrapped(
        dag=dag,
        name="inntektsendring_og_autobrev",
        script_path="scripts/inntektsendring_og_autobrev.py",
    )
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

    inntektsendring_og_autobrev
    laaste_vedtak
    kravstatus
    kontrollpunkt
