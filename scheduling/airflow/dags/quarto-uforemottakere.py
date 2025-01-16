from airflow import DAG
from datetime import datetime
from pendulum import timezone
from airflow.models import Variable
from dataverk_airflow import quarto_operator

with DAG(
    dag_id="quarto-uforemottakere",
    schedule_interval="30 6 * * 1",
    start_date=datetime(2025, 1, 16, tzinfo=timezone("Europe/Oslo")),
    doc_md="Datafortelling med månedlige uføremottakere fra dvhi.",
    catchup=False,
) as dag:
    update_quarto = quarto_operator(
        dag=dag,
        retries=0,
        name="update-quarto-uforemottakere",
        repo="navikt/pensjon-data-analyse",
        quarto={
            "path": "quarto/uforemottakere.qmd",
            "env": "prod",
            "id": "1168253f-0bfb-4a8c-964c-e1796582f72c",
            "token": Variable.get("PENSAK_QUARTO_TOKEN"),
        },
        use_uv_pip_install=True,
        allowlist=["dvh.adeo.no"],
        slack_channel="#pensak-airflow-alerts",
        requirements_path="scheduling/airflow/docker/requirements_dvhi.txt",
    )

    update_quarto
