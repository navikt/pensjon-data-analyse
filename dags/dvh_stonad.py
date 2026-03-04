from airflow import DAG
from datetime import datetime
from pendulum import timezone
from dataverk_airflow import python_operator

# DAG for overføring av stønadsdata fra Oracle til BQ for stønadsstatistikk
# Overfører tre tabeller fra pen_dataprodukt schema til stonadsstatistikk dataset:
# - dataprodukt_alderspensjon_vedtak -> stonad_alder_vedtak
# - dataprodukt_alderspensjon_beregning -> stonad_alder_beregning
# - dataprodukt_alderspensjon_belop -> stonad_alder_belop


with DAG(
    dag_id="dvh_stonad",
    description="Overføring av stønadsdata fra Oracle til BQ for stønadsstatistikk",
    start_date=datetime(2026, 3, 4, tzinfo=timezone("Europe/Oslo")),
    schedule_interval="0 6 1 * *",  # Kl 06:00 den første dagen i hver måned
    catchup=False,
) as dag:
    datalast_alder_q2 = python_operator(
        dag=dag,
        name="datalast_alder_q2",
        script_path="scripts/dvh_stonad_alder.py",
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
        extra_envs={"ENVIRONMENT": "dev"},
    )

    datalast_alder_prod = python_operator(
        dag=dag,
        name="datalast_alder_prod",
        script_path="scripts/dvh_stonad_alder.py",
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
        extra_envs={"ENVIRONMENT": "prod"},
    )

    datalast_alder_q2  # type: ignore
    datalast_alder_prod  # type: ignore
