from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator
from kubernetes import client as k8s

with DAG(dag_id="quarto-bpen002-oppg", schedule_interval="55 3 * * *", start_date=days_ago(1), catchup=False) as dag:
  t1 = quarto_operator(
    dag=dag, 
    name="update-quarto",
    repo="navikt/pensjon-data-analyse",
    branch="main",
    quarto={
        "path": "quarto/bpen002_oppg.qmd",
        "env": "prod",
        "id": "535c2989-db40-4c7a-a190-81a0c3f92e09",
        "token": Variable.get("PENSAK_QUARTO_TOKEN"),
    },
    requirements_path="requirements.txt",
    slack_channel="#pensak-airflow-alerts",
    resources=k8s.V1ResourceRequirements(
        requests={
            "memory": "256Mi",
            "ephemeral-storage": "300Mi",
        }
    ),
    retries=0,
    allowlist=["secretmanager.googleapis.com", "bigquery.googleapis.com", "dm08db03-vip.adeo.no:1521"]
  )