from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('aldersovergang', start_date=days_ago(1), schedule_interval="0 0 2-8 * tue", catchup=False) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-aldersovergang",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/aldersovergang.py",
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        allowlist=["secretmanager.googleapis.com", "bigquery.googleapis.com", "dm09-scan.adeo.no:1521"]
    )
