from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('dataproduct-psak-nytt-design', start_date=days_ago(1), schedule_interval="15 */1 * * *", catchup=False) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-psak-nytt-design",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_psak_nytt_design.py",
        requirements_path="scheduling/airflow/docker/requirements_postgres.txt",
        branch="main",
        retries=0
    )
    