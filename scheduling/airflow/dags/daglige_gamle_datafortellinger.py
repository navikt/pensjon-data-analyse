from airflow import DAG
from datetime import datetime
from pendulum import timezone
from airflow.models import Variable
from kubernetes import client as k8s
from dataverk_airflow import quarto_operator

# DAG for de tre datafortellingene Vebjørn lagde som oppdateres daglig.
# Bør vurderes om de kan saneres dersom de ikke er i bruk.


def quarto_operator_wrapped(
    *,  # Enforce keyword-only arguments
    dag: DAG,
    name: str,
    quarto_path: str,
    quarto_id: str,
):
    """Wrapper dataverk_airflow.quarto_operator with default arguments."""
    return quarto_operator(
        dag=dag,
        name=name,
        quarto={
            "path": quarto_path,
            "env": "prod",
            "id": quarto_id,
            "token": Variable.get("PENSAK_QUARTO_TOKEN"),
        },
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        # slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dm09-scan.adeo.no:1521",
        ],
        resources=k8s.V1ResourceRequirements(
            requests={
                "memory": "256Mi",
                "ephemeral-storage": "700Mi",
            }
        ),
    )


with DAG(
    dag_id="daglige_gamle_datafortellinger",
    description="Daglig oppdatering av (gamle) quarto-datafortellinger",
    schedule_interval="30 6 * * *",
    start_date=datetime(2025, 3, 12, tzinfo=timezone("Europe/Oslo")),
    doc_md="De tre datafortellingene Vebjørn lagde som oppdateres daglig.",
    catchup=False,
) as dag:
    bpen002_oppg = quarto_operator_wrapped(
        dag=dag,
        name="bpen002-oppg",
        quarto_path="quarto/bpen002_oppg.qmd",
        quarto_id="535c2989-db40-4c7a-a190-81a0c3f92e09",
    )

    fellesordningen = quarto_operator_wrapped(
        dag=dag,
        name="fellesordningen",
        quarto_path="quarto/fellesordningen.qmd",
        quarto_id="f0badb23-c06a-4c23-a45e-a3eee008f80a",
    )

    glemte_krav = quarto_operator_wrapped(
        dag=dag,
        name="glemte-krav",
        quarto_path="quarto/glemte_krav.qmd",
        quarto_id="2cc73eb9-36b4-47d4-a719-918236de37e6",
    )

    bpen002_oppg
    fellesordningen
    glemte_krav
