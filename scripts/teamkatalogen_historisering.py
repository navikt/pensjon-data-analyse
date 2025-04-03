import json
import logging
from google.cloud import secretmanager
from datetime import datetime, timezone
from google.oauth2 import service_account
from google.cloud.bigquery import Client, LoadJobConfig
logging.basicConfig(level=logging.INFO)

# lager månedlig historikk for teamkatalogen

teamkatalogen_dataset = 'org-prod-1016.teamkatalogen_federated_query_updated_dataset'
target_dataset = 'pensjon-saksbehandli-prod-1f83.teamkatalogen_historisert'
table_teams = 'Teams'
table_klynger = 'Klynger'
table_personer = 'Personer'
table_produktomraader = 'Produktomraader'

# må lese hemmelighet serviceuser-bq-historisere-teamkatalogen fra GSM
client = secretmanager.SecretManagerServiceClient()
secret_name = f"projects/230094999443/secrets/serviceuser-bq-historisere-teamkatalogen/versions/latest"
response = client.access_secret_version(request={"name": secret_name})
bq_secret = json.loads(response.payload.data.decode("UTF-8"))
bq_creds = service_account.Credentials.from_service_account_info(bq_secret)
bq_client = Client(credentials=bq_creds, project=bq_creds.project_id)

# bq_client = Client(project="pensjon-saksbehandli-prod-1f83")  # lokalt holder denne
job_config = LoadJobConfig(write_disposition="WRITE_APPEND")

historisert_tidspunkt = datetime.now(timezone.utc).isoformat()
for table in [table_teams, table_klynger, table_personer, table_produktomraader]:
    sql_read = f"""
    select
        '{historisert_tidspunkt}' as historisert_tidspunkt,
        *
    from `{teamkatalogen_dataset}.{table}`
    """
    
    query_job = bq_client.query(sql_read)
    results = query_job.result()
    job = bq_client.load_table_from_dataframe(
        results.to_dataframe(),
        f'{target_dataset}.{table}_historisert',
        job_config=job_config
    )
    job.result()
    logging.info(f"Data skrevet til {target_dataset}.{table}_historisert, tid: {historisert_tidspunkt}")
