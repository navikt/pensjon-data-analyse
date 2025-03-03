from airflow import DAG
from kubernetes import client as k8s
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('dataproduct-kontrollpunkt', start_date=days_ago(1), schedule_interval="10 06 * * *", catchup=False) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-kontrollpunkt",
        # slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_kontrollpunkt.py",
        requirements_path="requirements.txt",
        branch="main",
        retries=0,
        resources=k8s.V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "0.5", "ephemeral-storage": "1Gi"}
            ),
        allowlist=["secretmanager.googleapis.com", "bigquery.googleapis.com", "dm09-scan.adeo.no:1521"]
    )