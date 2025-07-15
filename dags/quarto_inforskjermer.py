from airflow import DAG
from datetime import datetime
from pendulum import timezone
from airflow.models import Variable
from kubernetes import client as k8s
from dataverk_airflow import quarto_operator
from images import get_image_name

WENDELBOE_IMAGE = get_image_name("wendelboe")


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
            "token": Variable.get("WENDELBOE_QUARTO_TOKEN"),
        },
        # image=WENDELBOE_IMAGE,
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
    )


with DAG(
    dag_id="quarto_inforskjermer",
    description="Oppdatere quarto datafortellinger for infoskjermer",
    schedule_interval=None,
    start_date=datetime(2025, 6, 14, tzinfo=timezone("Europe/Oslo")),
    doc_md="Oppdarer quarto datafortellinger for infoskjermer",
) as dag:
    infoskjerm = quarto_operator_wrapped(
        dag=dag,
        name="infoskjerm",
        quarto_path="quarto/infoskjerm-i-A6.qmd",
        quarto_id="672bf0f0-15f4-482e-8136-5b3c6096502a",
    )

    infoskjerm_plot = quarto_operator_wrapped(
        dag=dag,
        name="infoskjerm_plot",
        quarto_path="quarto/infoskjerm-i-A6-plott.qmd",
        quarto_id="3c7de1ff-2a5b-4a0b-91f8-d2e68a60bb0d",
    )

    infoskjerm
    infoskjerm_plot
