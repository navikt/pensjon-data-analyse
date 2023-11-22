from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('dataproduct-kravstatus', start_date=days_ago(1), schedule_interval="05 06 * * *", catchup=False) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-kravstatus",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_kravstatus.py",
        requirements_path="scheduling/airflow/docker/requirements_oracle.txt",
        branch="main",
        retries=0
    )