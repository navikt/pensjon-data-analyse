import logging
from datetime import datetime, timezone
from google.cloud.bigquery import Client, LoadJobConfig

logging.basicConfig(level=logging.INFO)


# makes a monthly status of the teamkatalogen table

target_dataset = 'pensjon-saksbehandli-prod-1f83.teamkatalogen_historisert'
teamkatalogen_dataset = 'org-prod-1016.teamkatalogen_federated_query_updated_dataset'
table_teams = 'Teams'
table_klynger = 'Klynger'
table_personer = 'Personer'
table_produktomraader = 'Produktomraader'


client = Client(project="pensjon-saksbehandli-prod-1f83")
job_config = LoadJobConfig(write_disposition="WRITE_APPEND")


historisert_tidspunkt = datetime.now(timezone.utc).isoformat()
for table in [table_teams, table_klynger, table_personer, table_produktomraader]:
    sql_read = f"""
    select
        '{historisert_tidspunkt}' as historisert_tidspunkt,
        *
    from `{teamkatalogen_dataset}.{table}`
    """
    
    query_job = client.query(sql_read)
    results = query_job.result()
    job = client.load_table_from_dataframe(
        results.to_dataframe(),
        f'{target_dataset}.{table}_historisert',
        job_config=job_config
    )
    job.result()
    logging.info(f"Data skrevet til {target_dataset}.{table}_historisert, tid: {historisert_tidspunkt}")
