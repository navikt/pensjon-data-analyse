import logging
import pandas as pd
from time import time
from google.cloud.bigquery import LoadJobConfig, SchemaField, enums
from google.api_core.exceptions import NotFound

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

dev_table_id = "pensjon-saksbehandli-prod-1f83.dvh_sak_dev.saksbehandlingsstatistikk"
# dataset på datamarkedplassen

logging.basicConfig(level=logging.INFO)
pesys_utils.set_db_secrets(secret_name="pen-q2-pen_dataprodukt")

# bigquery
client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
create_disposition = "CREATE_NEVER"  # endres til CREATE_IF_NEEDED under hvis tabellen ikke finnes
sql_pen_dev = "select * from pen_dataprodukt.behandlingsstatistikk_meldinger"
# prøver å finne maks kjoretidspunkt i BQ, og hvis det finnes legges det til som en where
try:  # hvis tabellen finnes, hent alle nye rader
    query = f"select max(teknisk_tid) as maks_kjoretidspunkt_bq from `{dev_table_id}`"
    results = client.query(query).result()
    df = results.to_dataframe()
    if not df.empty and pd.notnull(df.iloc[0]["maks_kjoretidspunkt_bq"]):
        maks_kjoretidspunkt_bq = df.iloc[0]["maks_kjoretidspunkt_bq"]
        sql_pen_dev += f" where teknisk_tid > to_date('{maks_kjoretidspunkt_bq}', 'YYYY-MM-DD HH24:MI:SS')"
        logging.info(f"Maks kjoretidspunkt i BQ: {maks_kjoretidspunkt_bq}")
except NotFound:
    logging.info("Tabellen finnes ikke i BQ, så oppretter ny tabell med alle rader fra oracle.")
    create_disposition = "CREATE_IF_NEEDED"

con = pesys_utils.connect_to_oracle()
df_bq = pesys_utils.df_from_sql(sql_pen_dev, con)

schema: list[SchemaField] = [
    SchemaField("behandling_id", enums.SqlTypeNames.STRING),
    SchemaField("relatertbehandling_id", enums.SqlTypeNames.STRING),
    SchemaField("relatert_fagsystem", enums.SqlTypeNames.STRING),
    SchemaField("sak_id", enums.SqlTypeNames.STRING),
    SchemaField("aktor_id", enums.SqlTypeNames.STRING),
    SchemaField("mottatt_tid", enums.SqlTypeNames.DATETIME),
    SchemaField("registrert_tid", enums.SqlTypeNames.DATETIME),
    SchemaField("ferdigbehandlet_tid", enums.SqlTypeNames.DATETIME),
    SchemaField("utbetalt_tid", enums.SqlTypeNames.DATETIME),
    SchemaField("endret_tid", enums.SqlTypeNames.DATETIME),
    SchemaField("forventetoppstart_tid", enums.SqlTypeNames.DATETIME),
    SchemaField("teknisk_tid", enums.SqlTypeNames.DATETIME),
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
    SchemaField("funksjonell_periode_fom", enums.SqlTypeNames.DATETIME),
    SchemaField("funksjonell_periode_tom", enums.SqlTypeNames.DATETIME),
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
