import logging
import pandas as pd
from time import time
from google.cloud.bigquery import LoadJobConfig, SchemaField, enums, Dataset
from google.api_core.exceptions import NotFound

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

GCP_PROJECT_ID = "pensjon-saksbehandli-dev-cb76"
DATASET_NAME = "saksbehandlingsstatistikk"
TABLE_NAME = "saksbehandlingsstatistikk_ufore"

dev_table_id = f"{GCP_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
dev_view_id = f"{GCP_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}_view"
# dataset på datamarkedplassen

logging.basicConfig(level=logging.INFO)
pesys_utils.set_db_secrets(secret_name="pen-q2-pen_dataprodukt")

# bigquery
# client = gcp_utils.get_bigquery_client(project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com")
client = gcp_utils.get_bigquery_client(
    project=GCP_PROJECT_ID, target_principal="bq-airflow-dev@pensjon-saksbehandli-dev-cb76.iam.gserviceaccount.com"
)

# Sjekk om datasettet finnes
dataset = Dataset(f"{GCP_PROJECT_ID}.{DATASET_NAME}")
dataset.location = "europe-north1"
try:
    client.get_dataset(dataset)
    logging.info(f"Datasettet {DATASET_NAME} finnes.")
except NotFound:
    logging.info(f"Datasettet {DATASET_NAME} finnes ikke. Oppretter datasett.")
    dataset = client.create_dataset(dataset)
    logging.info(f"Datasettet {DATASET_NAME} er opprettet.")

# Sjekk om tabellen finnes og hent maks teknisk_tid
create_disposition = "CREATE_NEVER"  # endres til CREATE_IF_NEEDED under hvis tabellen ikke finnes
maks_kjoretidspunkt_bq = "1900-01-01 00:00:00"
try:  # hvis tabellen finnes, hent alle nye rader
    query = f"select max(teknisk_tid) as maks_kjoretidspunkt_bq from `{dev_table_id}`"
    results = client.query(query).result()
    df = results.to_dataframe()
except NotFound:
    logging.info(f"Tabellen {dev_table_id} finnes ikke i BQ, så oppretter ny tabell med alle rader fra oracle.")
    create_disposition = "CREATE_IF_NEEDED"
    df = pd.DataFrame()

if not df.empty and pd.notnull(df.iloc[0]["maks_kjoretidspunkt_bq"]):
    maks_kjoretidspunkt_bq = df.iloc[0]["maks_kjoretidspunkt_bq"]
    logging.info(f"Maks kjoretidspunkt i BQ: {maks_kjoretidspunkt_bq}")
sql_pen_dev = f"select * from pen_dataprodukt.behandlingsstatistikk_meldinger where teknisk_tid > to_date('{maks_kjoretidspunkt_bq}', 'YYYY-MM-DD HH24:MI:SS')"

con = pesys_utils.connect_to_oracle()
df_bq = pesys_utils.df_from_sql(sql_pen_dev, con)

schema: list[SchemaField] = [
    SchemaField("sekvensnummer", enums.SqlTypeNames.INT64),
    SchemaField("behandling_id", enums.SqlTypeNames.STRING),
    SchemaField("relatertbehandling_id", enums.SqlTypeNames.STRING),
    SchemaField("relatert_fagsystem", enums.SqlTypeNames.STRING),
    SchemaField("sak_id", enums.SqlTypeNames.STRING),
    SchemaField("aktor_id", enums.SqlTypeNames.STRING),
    SchemaField("mottatt_tid", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("registrert_tid", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("ferdigbehandlet_tid", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("utbetalt_tid", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("endret_tid", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("forventetoppstart_tid", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("teknisk_tid", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("sak_ytelse", enums.SqlTypeNames.STRING),
    SchemaField("sak_utland", enums.SqlTypeNames.STRING),
    SchemaField("behandling_type", enums.SqlTypeNames.STRING),
    SchemaField("behandling_status", enums.SqlTypeNames.STRING),
    SchemaField("behandling_resultat", enums.SqlTypeNames.STRING),
    SchemaField("behandling_metode", enums.SqlTypeNames.STRING),
    SchemaField("behandling_arsak", enums.SqlTypeNames.STRING),
    SchemaField("opprettet_av", enums.SqlTypeNames.STRING),
    SchemaField("saksbehandler", enums.SqlTypeNames.STRING),
    SchemaField("ansvarlig_beslutter", enums.SqlTypeNames.STRING),
    SchemaField("ansvarlig_enhet", enums.SqlTypeNames.STRING),
    SchemaField("tilbakekrev_belop", enums.SqlTypeNames.FLOAT64),
    SchemaField("funksjonell_periode_fom", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("funksjonell_periode_tom", enums.SqlTypeNames.TIMESTAMP),
    SchemaField("fagsystem_navn", enums.SqlTypeNames.STRING),
    SchemaField("fagsystem_versjon", enums.SqlTypeNames.STRING),
]

job_config = LoadJobConfig(
    write_disposition="WRITE_APPEND",
    create_disposition=create_disposition,
    schema=schema,
)


start = time()
job = client.load_table_from_dataframe(df_bq, dev_table_id, job_config=job_config)
job.result()
end = time()

print(f"{len(df_bq)} rader ble skrevet til bigquery etter {end - start} sekunder.")

view_query = f"""
CREATE OR REPLACE VIEW `{dev_view_id}` as
select * from `{dev_table_id}`
"""
try:
    client.query(view_query).result()
    logging.info(f"View {dev_view_id} opprettet/oppdatert.")
except Exception as e:
    logging.error(f"Feil ved oppretting/oppdatering av view {dev_view_id}: {e}")
