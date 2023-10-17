from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from kubernetes import client as k8s
from datetime import datetime


def run_update_datastory():
    from scripts.datastory_etterlatte import update_datastory
    
    update_datastory()
    

with DAG('datastory-etterlatte', start_date=datetime(2023, 6, 1), schedule_interval="22 3 1 * *") as dag:
    slack = SlackAPIPostOperator(
        task_id="error",
        dag=dag,
        slack_conn_id="slack_connection",
        text=f":red_circle: Error.",
        channel="#pensak-airflow-alerts",
        attachments=[
        {
            "fallback": "min attachment",
            "color": "#2eb886",
            "pretext": "pensjon-saksbehandling",
            "author_name": "Airflow alert",
            "title": f"{dag.dag_id}",
            "text": "Gå til https://pensjon-saksbehandling.airflow.knada.io/home for mer info.",
            "fields": [
                {
                    "title": "Priority",
                    "value": "High",
                    "short": False
                }
            ],
            "footer": "pensjon-saksbehandling"
        }
        ]
    )

    run_this = PythonOperator(
        task_id='datastory-etterlatte',
        python_callable=run_update_datastory,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                    k8s.V1Container(
                        name="base",
                        image="ghcr.io/navikt/pensak-airflow-images:2023-09-08-531834a-main",
                        working_dir="/dags/scripts",
                    )
                    ]
                ),
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "dm08db03.adeo.no:1521,data.intern.nav.no"})
            )
        },
    dag=dag)

    slack >> run_this