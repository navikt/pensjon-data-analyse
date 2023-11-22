from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator
from airflow.models import Variable
from kubernetes import client as k8s

with DAG(dag_id="quarto-glemte-krav", schedule_interval="8 3 * * *", start_date=days_ago(1), catchup=False) as dag:
  podop = quarto_operator(
    dag=dag, 
    name="update-quarto",
    repo="navikt/pensjon-data-analyse",
    branch="main",
    quarto={
        "path": "quarto/glemte_krav.qmd",
        "env": "prod",
        "id": "2cc73eb9-36b4-47d4-a719-918236de37e6",
        "token": Variable.get("PENSAK_QUARTO_TOKEN"),
    },
    requirements_path="scheduling/airflow/docker/requirements_oracle.txt",
    slack_channel="#pensak-airflow-alerts",
    resources=k8s.V1ResourceRequirements(
        requests={
            "memory": "256Mi"
        }
    ),
    retries=0,
    allowlist=["secretmanager.googleapis.com", "bigquery.googleapis.com", "a01dbfl041.adeo.no:1521", "dm08db03.adeo.no:1521"]
  )