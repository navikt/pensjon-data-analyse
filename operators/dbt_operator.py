from typing import Optional

from airflow.models import Variable
from airflow import DAG

from dataverk_airflow import python_operator
from images import get_image_name

DBT_IMAGE = get_image_name("dbt")


def dbt_operator(
    dag: DAG,
    name: str,
    repo: str,
    script_path: str,
    dbt_secret_name: str,
    branch: str = "main",
    retries: int = 3,
    startup_timeout_seconds: int = 60 * 10,
    dbt_command: str = "build",
    dbt_image: str = DBT_IMAGE,
    dbt_models: Optional[str] = None,
    do_xcom_push: bool = False,
    allowlist: list = [],
    slack_channel: str = Variable.get("SLACK_OPS_CHANNEL"),
    db_environment: str = "pen_q2",  # pen_q2, pen_q1, pen_prod_lesekopi, pen_prod
):
    env_vars = {
        "DBT_COMMAND": dbt_command,
        "DBT_DB_TARGET": db_environment,
        "TEAM_GCP_SECRET_PATH": f"projects/{Variable.get('TEAM_GCP_PROJECT')}/secrets/{dbt_secret_name}/versions/latest",
    }
    if dbt_models:
        env_vars["DBT_MODELS"] = dbt_models

    return python_operator(
        dag=dag,
        name=name,
        startup_timeout_seconds=startup_timeout_seconds,
        repo=repo,
        branch=branch,
        script_path=script_path,
        allowlist=allowlist,
        slack_channel=slack_channel,
        retries=retries,
        extra_envs=env_vars,
        image=dbt_image,
        do_xcom_push=do_xcom_push,
    )
