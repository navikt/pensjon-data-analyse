from airflow import DAG
from datetime import datetime
import pendulum
from common.podop_factory import create_pod_operator
from airflow.models import Variable
from kubernetes import client as k8s

with DAG(
    dag_id="quarto-glemte-krav-new",
    description="Installerer pakker ved oppstart og oppdaterer quarto",
    schedule_interval="8 3 * * *",
    start_date=datetime(2023, 8, 26, tzinfo=pendulum.timezone("Europe/Oslo")),
    catchup=False,
) as dag:
  podop = create_pod_operator(
    dag=dag, 
    name="update-quarto",
    repo="navikt/pensjon-data-analyse",
    branch="main",
    quarto={
        "path": "quarto/glemte_krav.qmd",
        "environment": "datamarkedsplassen.intern.nav.no",
        "id": "2cc73eb9-36b4-47d4-a719-918236de37e6",
        "token": Variable.get("PENSAK_QUARTO_TOKEN"),
    },
    requirements_file="scheduling/airflow/docker/requirements.txt",
    image="europe-north1-docker.pkg.dev/knada-gcp/knada-north/airflow:2023-09-22-0bb59f1",
    delete_on_finish=False,
    slack_channel="#pensak-airflow-alerts",
    resources=k8s.V1ResourceRequirements(
        requests={
            "memory": "256Mi"
        }
    )
  )