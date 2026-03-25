from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator

# DAG for overføring av data fra oracle til BQ for saksbehandlingsstatistikk
with DAG(
    dag_id="oracle_til_bq_sak",
    description="overføring av data fra oracle til BQ for saksbehandlingsstatistikk",
    start_date=datetime(2026, 1, 11, tzinfo=timezone("Europe/Oslo")),
    schedule_interval="30 6,11,21 * * *",  # 30 min etter dbt_behandlingsstatistikk
    catchup=False,
) as dag:
    datalast_ufore_q2 = python_operator(
        dag=dag,
        name="datalast_ufore_q2",
        script_path="scripts/dvh_sak_ufore.py",
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

    datalast_ufore_prod = python_operator(
        dag=dag,
        name="datalast_ufore_prod",
        script_path="scripts/dvh_sak_ufore.py",
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

    datalast_ufore_q2
    datalast_ufore_prod
