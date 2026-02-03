from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator

# DAG for overføring av data fra oracle til BQ for saksbehandlingsstatistikk


with DAG(
    dag_id="dvh_sak",
    description="overføring av data fra oracle til BQ for saksbehandlingsstatistikk",
    start_date=datetime(2026, 1, 11, tzinfo=timezone("Europe/Oslo")),
    schedule_interval="0 1,9,13,17 * * *",  # Kl 01:00, 09:00, 13:00 og 17:00 hver dag
    catchup=False,
) as dag:
    datalast_ufore_q2 = python_operator(
        dag=dag,
        name='datalast_ufore_q2',
        script_path="scripts/dvh_sak_ufore.py",
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv36-scan.adeo.no:1521",  # q2
        ],
        python_version="3.12",
        extra_envs={"ENVIRONMENT": "dev"},  # eller "prod"
    )
    
    datalast_ufore_prod = python_operator(
        dag=dag,
        name='datalast_ufore_prod',
        script_path="scripts/dvh_sak_ufore.py",
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        repo="navikt/pensjon-data-analyse",
        slack_channel="#pensak-airflow-alerts",
        allowlist=[
            "secretmanager.googleapis.com",
            "bigquery.googleapis.com",
            "dmv14-scan.adeo.no:1521",  # prod lesekopien
        ],
        python_version="3.12",
        extra_envs={"ENVIRONMENT": "prod"},  # eller "prod"
    )

    datalast_ufore_q2
    datalast_ufore_prod
