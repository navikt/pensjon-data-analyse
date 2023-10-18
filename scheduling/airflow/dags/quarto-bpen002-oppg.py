from airflow import DAG
from datetime import datetime
import pendulum
from common.podop_factory import create_pod_operator
from airflow.models import Variable
from kubernetes import client as k8s

with DAG(
    dag_id="quarto-bpen002-oppg",
    description="Installerer pakker ved oppstart og oppdaterer quarto",
    schedule_interval="55 3 * * *",
    start_date=datetime(2023, 8, 27, tzinfo=pendulum.timezone("Europe/Oslo")),
    catchup=False,
) as dag:
  podop = create_pod_operator(
    dag=dag, 
    name="update-quarto",
    repo="navikt/pensjon-data-analyse",
    branch="main",
    quarto={
        "path": "quarto/bpen002_oppg.qmd",
        "environment": "datamarkedsplassen.intern.nav.no",
        "id": "535c2989-db40-4c7a-a190-81a0c3f92e09",
        "token": Variable.get("PENSAK_QUARTO_TOKEN"),
    },
    requirements_file="scheduling/airflow/docker/requirements.txt",
    image="europe-north1-docker.pkg.dev/knada-gcp/knada-north/airflow:2023-09-22-0bb59f1",
    slack_channel="#pensak-airflow-alerts",
    resources=k8s.V1ResourceRequirements(
        requests={
            "memory": "256Mi"
        }
    )
  )