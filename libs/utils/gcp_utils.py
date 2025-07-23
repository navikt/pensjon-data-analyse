from google.cloud.bigquery import Client
from google.auth import impersonated_credentials, default


def get_bigquery_client(project: str, target_principal: str = None):
    """
    Returns a BigQuery client with optional impersonation.

    :param project: The GCP project ID for the BigQuery client.
    :param target_principal: Optional service account to impersonate.
    :return: A BigQuery client instance.
    """
    if target_principal:
        default_creds, _ = default()
        target_credentials = impersonated_credentials.Credentials(
            source_credentials=default_creds,
            target_principal=target_principal,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return Client(project=project, credentials=target_credentials)

    return Client(project=project)
