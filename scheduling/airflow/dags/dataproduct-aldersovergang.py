from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('dataproduct-aldersovergang', start_date=days_ago(1), schedule_interval="10 06 * * *", catchup=False) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-aldersovergang",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_aldersovergang.py",
        requirements_path="requirements.txt",
        branch="main",
        retries=0,
        allowlist=["secretmanager.googleapis.com", "bigquery.googleapis.com", "dm09-scan.adeo.no:1521"]
    )