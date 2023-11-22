from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('dataproduct-laaste-vedtak', start_date=days_ago(1), schedule_interval="15 06 * * *", catchup=False) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-laaste-vedtak",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_laaste_vedtak.py",
        requirements_path="scheduling/airflow/docker/requirements_oracle.txt",
        branch="main",
        retries=0
    )
    