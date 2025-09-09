from airflow import DAG
from datetime import datetime
from pendulum import timezone
from airflow.models import Variable
from kubernetes import client as k8s
from dataverk_airflow import python_operator, quarto_operator


def quarto_operator_wrapped(
    *,  # Enforce keyword-only arguments
    dag: DAG,
    name: str,
    quarto_folder_path: str,
    quarto_id: str,
):
    """Wrapper dataverk_airflow.quarto_operator with default arguments."""
    return quarto_operator(
        dag=dag,
        name=name,
        quarto={
            "folder": quarto_folder_path,
            "env": "prod",
            "id": quarto_id,
            "token": Variable.get("WENDELBOE_QUARTO_TOKEN"),
        },
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "data.ssb.no",
        ],
        resources=k8s.V1ResourceRequirements(
            requests={
                "memory": "256Mi",
                "ephemeral-storage": "700Mi",
            }
        ),
        python_version="3.12",
    )


with DAG(
    dag_id="quarto_inforskjermer",
    description="Oppdatere quarto datafortellinger for infoskjermer",
    schedule_interval="0 6 * * 1",  # 06:00 hver mandag
    start_date=datetime(2025, 6, 14, tzinfo=timezone("Europe/Oslo")),
    doc_md="Oppdarer quarto datafortellinger for infoskjermer",
) as dag:
    oracle_til_bigquery_ytelser = python_operator(
        dag=dag,
        name="oracle_til_bigquery_ytelser",
        script_path="scripts/infoskjerm_ytelser.py",
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "bigquery.googleapis.com",
            "dmv14-scan.adeo.no:1521",  # prod lesekopien
        ],
        python_version="3.12",
    )

    infoskjerm = quarto_operator_wrapped(
        dag=dag,
        name="infoskjerm",
        quarto_folder_path="quarto/infoskjerm-i-A6/",
        quarto_id="672bf0f0-15f4-482e-8136-5b3c6096502a",
    )

    infoskjerm_plot = quarto_operator_wrapped(
        dag=dag,
        name="infoskjerm_plot",
        quarto_folder_path="quarto/infoskjerm-i-A6-plott/",
        quarto_id="3c7de1ff-2a5b-4a0b-91f8-d2e68a60bb0d",
    )

    oracle_til_bigquery_ytelser >> infoskjerm_plot
    infoskjerm
