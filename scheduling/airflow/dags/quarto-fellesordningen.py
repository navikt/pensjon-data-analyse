from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator
from airflow.models import Variable
from kubernetes import client as k8s

with DAG(dag_id="quarto-fellesordningen", schedule_interval="22 2 * * *", start_date=days_ago(1), catchup=False) as dag:
  podop = quarto_operator(
    dag=dag, 
    name="update-quarto",
    repo="navikt/pensjon-data-analyse",
    branch="main",
    quarto={
        "path": "quarto/fellesordningen.qmd",
        "env": "prod",
        "id": "f0badb23-c06a-4c23-a45e-a3eee008f80a",
        "token": Variable.get("PENSAK_QUARTO_TOKEN"),
    },
    requirements_path="scheduling/airflow/docker/requirements_oracle.txt",
    slack_channel="#pensak-airflow-alerts",
    resources=k8s.V1ResourceRequirements(
        requests={
            "memory": "256Mi",
            "ephemeral-storage": "700Mi",
        }
    ),
    retries=0,
    allowlist=["secretmanager.googleapis.com", "bigquery.googleapis.com", "dm08db03-vip.adeo.no:1521"]
  )