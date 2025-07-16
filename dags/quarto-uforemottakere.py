from airflow import DAG
from datetime import datetime
from pendulum import timezone
from airflow.models import Variable
from dataverk_airflow import quarto_operator
from images import get_image_name

WENDELBOE_IMAGE = get_image_name("wendelboe")

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
            "id": "d1e4cefc-2658-4519-a8c0-0f29db301d9d",
            "token": Variable.get("PENSAK_QUARTO_TOKEN"),
        },
        # image=WENDELBOE_IMAGE,
        requirements_path="requirements.txt",
        use_uv_pip_install=True,
        allowlist=["secretmanager.googleapis.com", "dmv09-scan.adeo.no:1521"],  # DVHP
        # slack_channel="#pensak-airflow-alerts",
        extra_envs={"RUNNING_IN_AIRFLOW": "true"},
        python_version="3.12",
    )

    update_quarto
