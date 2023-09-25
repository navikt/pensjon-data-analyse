import os

from datetime import timedelta
from pathlib import Path

from airflow import DAG
from kubernetes.client.models import V1Volume, V1SecretVolumeSource, V1ConfigMapVolumeSource, V1VolumeMount, V1PodSecurityContext, V1SeccompProfile
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes import client

from common.initcontainers import create_git_clone_init_container
from common.notifications import create_email_notification, create_slack_notification


POD_WORKSPACE_DIR = "/workspace"
CA_BUNDLE_PATH = "/etc/pki/tls/certs/ca-bundle.crt"
CA_BUNDLE_CM = "ca-bundle-pem"


def create_pod_operator(
    dag: DAG,
    name: str,
    repo: str,
    script_path: str = None,
    nb_path: str = None,
    quarto: dict = None,
    namespace: str = None,
    email: str = None,
    slack_channel: str = None,
    branch: str = "main",
    resources: client.V1ResourceRequirements = None,
    retries: int = 3,
    extra_envs: dict = None,
    delete_on_finish: bool = True,
    image: str = None,
    log_output: bool = False,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=5),
    nls_lang: str = "NORWEGIAN_NORWAY.AL32UTF8",
    do_xcom_push: bool = False,
    requirements_file: str = None,
    allowlist: list = [],
):
    """ Factory function for creating KubernetesPodOperator for executing knada python scripts

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param script_path: str: Path to python script in repo
    :param nb_path: str: Path to the notebook in repo
    :param namespace: str: K8S namespace for pod
    :param email: str: Email of owner
    :param slack_channel: Name of slack channel, default None (no slack notification)
    :param branch: str: Branch in repo, default "master"
    :param resources: dict: Specify required cpu and memory requirements (keys in dict: request_memory, request_cpu, limit_memory, limit_cpu), default None
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param image: str: Dockerimage the pod should use
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param nls_lang: str: Configure locale and character sets with NLS_LANG environment variable in k8s pod, defaults to Norwegian
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json'
    :param allowlist: list: list of hosts and port the task needs to reach on the format host:port
    :return: KubernetesPodOperator
    """

    env_vars = {
        "TZ": os.environ["TZ"],
        "REQUESTS_CA_BUNDLE": CA_BUNDLE_PATH,
        "NLS_LANG": nls_lang,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"]
    }

    allowlist_str = "hooks.slack.com,slack.com"

    if len(allowlist):
        allowlist_str += "," + ",".join(allowlist)

    namespace = namespace if namespace else os.getenv("NAMESPACE")

    if extra_envs:
        env_vars = dict(env_vars, **extra_envs)

    def on_failure(context):
        if email:
            send_email = create_email_notification(dag._dag_id, email, name, namespace, dag)
            send_email.execute(context)

        if slack_channel:
            slack_notification = create_slack_notification(dag._dag_id, slack_channel, name, namespace, dag)
            slack_notification.execute()

    if not image:
        image = os.getenv("KNADA_PYTHON_POD_OP_IMAGE", "europe-west1-docker.pkg.dev/knada-gcp/knada/airflow:2023-03-08-d3684b7")

    return KubernetesPodOperator(
        init_containers=[create_git_clone_init_container(
            repo, branch, POD_WORKSPACE_DIR)],
        image_pull_secrets=os.environ["K8S_IMAGE_PULL_SECRETS"],
        dag=dag,
        labels={"component": "worker", "release": "airflow"},
        annotations={"allowlist": allowlist_str},
        on_failure_callback=on_failure,
        startup_timeout_seconds=startup_timeout_seconds,
        name=name,
        cmds=create_container_cmd(requirements_file, script_path, nb_path, quarto, log_output),
        namespace=namespace,
        task_id=name,
        is_delete_operator_pod=delete_on_finish,
        image=image,
        env_vars=env_vars,
        volume_mounts=[
            V1VolumeMount(
                name="dags-data",
                mount_path=POD_WORKSPACE_DIR,
                sub_path=None,
                read_only=False
            ),
            V1VolumeMount(
                name="ca-bundle-pem",
                mount_path=CA_BUNDLE_PATH,
                read_only=True,
                sub_path="ca-bundle.pem"
            )
        ],
        service_account_name=os.getenv("TEAM", "default"),
        volumes=[
            V1Volume(
                name="dags-data"
            ),
            V1Volume(
                name="airflow-git-secret",
                secret=V1SecretVolumeSource(
                    default_mode=448,
                    secret_name=os.getenv("K8S_GIT_CLONE_SECRET", "github-app-secret"),
                )
            ),
            V1Volume(
                name="ca-bundle-pem",
                config_map=V1ConfigMapVolumeSource(
                    default_mode=420,
                    name=CA_BUNDLE_CM,
                )
            ),
        ],
        container_resources=resources,
        retries=retries,
        retry_delay=retry_delay,
        do_xcom_push=do_xcom_push,
        security_context=V1PodSecurityContext(
            fs_group=0,
            seccomp_profile=V1SeccompProfile(
                type="RuntimeDefault"
            )
        ),
    )

def create_container_cmd(requirements_file, script_path, nb_path, quarto, log_output) -> list:
    command = ""
    if requirements_file:
        command = f"pip install -r {POD_WORKSPACE_DIR}/{requirements_file} --user &&"

    if script_path:
        command += f"cd {POD_WORKSPACE_DIR}/{Path(script_path).parent} && python {Path(script_path).name}"
    elif nb_path:
        command += f"cd {POD_WORKSPACE_DIR}/{Path(nb_path).parent} && papermill {Path(nb_path).name} output.ipynb"
        if log_output:
            command += " --log-output"
    elif quarto:
        try:
            command += f"cd {POD_WORKSPACE_DIR}/{Path(quarto['path']).parent} && quarto render {Path(quarto['path']).name} --to html --execute --output index.html -M self-contained:True &&" + \
                        f"""curl -X PUT -F index.html=@index.html https://{quarto['environment']}/quarto/update/{quarto['id']} -H "Authorization:Bearer {quarto['token']}" """
        except KeyError:
            raise KeyError("path, environment, id and token must be provided for quarto")
    else:
        raise ValueError("Either script_path, nb_path or quarto parameter must be provided")

    return ["/bin/bash", "-c", command]
