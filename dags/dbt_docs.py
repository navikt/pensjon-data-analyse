import pendulum
from datetime import datetime
from airflow import DAG
from airflow.models import Variable

from operators.dbt_operator import dbt_operator


# db_adresse = "dmv36db01.adeo.no:1521"
# docs_adresse = "dbt.intern.nav.no"


with DAG(
    dag_id="dbt_docs",
    start_date=datetime(2025, 6, 30, tzinfo=pendulum.timezone("Europe/Oslo")),
    schedule_interval="0 6 * * 1",  # “6 AM monday every week”
    catchup=False,
) as dag:
    pen_dataprodukt = dbt_operator(
        dag=dag,
        name="pen_dataprodukt",
        startup_timeout_seconds=60 * 10,
        repo="navikt/pensjon-pen-dataprodukt",
        script_path="dbt/dbt_run.py",
        dbt_command="docs generate",
        allowlist=[
            "dmv36-scan.adeo.no:1521",
            "dbt.intern.nav.no",
            "hub.getdbt.com",
        ],
        dbt_secret_name="pen-q2-pen_dataprodukt",
    )

    pen_dataprodukt
