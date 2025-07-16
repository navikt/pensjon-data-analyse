from airflow import DAG
from kubernetes import client as k8s
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator
from images import get_image_name

WENDELBOE_IMAGE = get_image_name("wendelboe")


with DAG(
    "dataproduct-automatisering-v2",
    start_date=days_ago(40),
    schedule_interval="22 4 1 * *",
    catchup=True,
) as dag:
    t1 = python_operator(
        dag=dag,
        name="dataproduct-automatisering-v2",
        slack_channel="#pensak-airflow-alerts",
        # image=WENDELBOE_IMAGE,
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/dataproduct_automatisering_v2.py",
        resources=k8s.V1ResourceRequirements(
            requests={"memory": "6Gi", "cpu": "1", "ephemeral-storage": "200Mi"},
        ),
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dm08db03-vip.adeo.no:1521",  # prod lesekopien
        ],
        python_version="3.12",
    )
