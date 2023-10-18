from airflow import DAG

from common.podop_factory import create_pod_operator
from common.notifications import create_slack_notification
from datetime import datetime


def run_update_datastory():
    from scripts.datastory_etterlatte import update_datastory
    
    update_datastory()
    

# with DAG('datastory-etterlatte', start_date=datetime(2023, 6, 1), schedule_interval="22 3 1 * *") as dag:
#     def on_failure(context):
#             slack_notification = create_slack_notification(dag._dag_id, "#pensak-airflow-alerts", dag.name, namespace, dag)
#             slack_notification.execute()

#     run_this = PythonOperator(
#         task_id='datastory-etterlatte',
#         python_callable=run_update_datastory,
#         on_failure_callback=on_failure,
        
#         executor_config={
#             "pod_override": k8s.V1Pod(
#                 spec=k8s.V1PodSpec(
#                     containers=[
#                     k8s.V1Container(
#                         name="base",
#                         image="ghcr.io/navikt/pensak-airflow-images:2023-09-08-531834a-main",
#                         working_dir="/dags/scripts",
#                     )
#                     ]
#                 ),
#                 metadata=k8s.V1ObjectMeta(annotations={"allowlist": "dm08db03.adeo.no:1521,data.intern.nav.no"})
#             )
#         },
#     dag=dag)

with DAG('datastory-etterlatte', start_date=datetime(2023, 6, 1), schedule_interval="30 5 * * 1") as dag:
    t1 = create_pod_operator(
        dag=dag,
        name="datastory-etterlatte",
        slack_channel="#pensak-airflow-alerts",
        repo="navikt/pensjon-data-analyse",
        script_path="scripts/datastory_etterlatte.py",
        branch="main",
        retries=0
    )