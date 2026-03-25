from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator

with DAG(
    dag_id="oracle_til_bq_stonad",
    description="Overføring av dataprodukter fra oracle til BQ for stønadsstatistikken til Team Pensjon DVH",
    start_date=datetime(2026, 3, 24, tzinfo=timezone("Europe/Oslo")),
    schedule_interval="0 4 * * *",  # 4:00 hver dag
    catchup=False,
) as dag:
    datalast_stonad_alder_q2 = python_operator(
        dag=dag,
        name="datalast_stonad_alder_q2",
        script_path="scripts/dvh_stonad_alder.py",
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        slack_channel="#pensak-airflow-alerts",
        use_uv_pip_install=True,
        python_version="3.12",
        extra_envs={"ENVIRONMENT": "dev"},
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv36-scan.adeo.no:1521",  # q2
        ],
    )

    datalast_stonad_alder_prod = python_operator(
        dag=dag,
        name="datalast_stonad_alder_prod",
        script_path="scripts/dvh_stonad_alder.py",
        repo="navikt/pensjon-data-analyse",
        requirements_path="requirements.txt",
        slack_channel="#pensak-airflow-alerts",
        use_uv_pip_install=True,
        python_version="3.12",
        extra_envs={"ENVIRONMENT": "prod"},
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv14-scan.adeo.no:1521",  # prod lesekopien
        ],
    )

    datalast_stonad_alder_q2
    datalast_stonad_alder_prod
