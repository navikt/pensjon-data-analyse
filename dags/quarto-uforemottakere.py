from airflow import DAG
from datetime import datetime
from pendulum import timezone
from airflow.models import Variable
from dataverk_airflow import quarto_operator

with DAG(
    dag_id="quarto-uforemottakere",
    schedule_interval="30 6 * * 1",
    start_date=datetime(2025, 1, 16, tzinfo=timezone("Europe/Oslo")),
    doc_md="Datafortelling med månedlige uføremottakere fra datavarehuset. Tall oppdateres vel kvartalsvis.",
    catchup=False,
) as dag:
    update_quarto = quarto_operator(
        dag=dag,
        name="update-quarto-uforemottakere",
        repo="navikt/pensjon-data-analyse",
        quarto={
            "path": "quarto/uforemottakere.qmd",
            "env": "prod",
            "id": "1168253f-0bfb-4a8c-964c-e1796582f72c",
            "token": Variable.get("PENSAK_QUARTO_TOKEN"),
        },
        use_uv_pip_install=True,
        allowlist=["secretmanager.googleapis.com", "dmv09-scan.adeo.no:1521"], # DVHP
        # slack_channel="#pensak-airflow-alerts",
        requirements_path="requirements.txt",
    )

    update_quarto
