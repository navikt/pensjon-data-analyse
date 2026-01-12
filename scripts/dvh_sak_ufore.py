import logging
from time import time
from google.cloud.bigquery import LoadJobConfig

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

full_table_id = "pensjon-saksbehandli-prod-1f83.dvh_sak_dev.saksbehandlingsstatistikk"
# dataset på datamarkedplassen

logging.basicConfig(level=logging.INFO)
pesys_utils.set_db_secrets(secret_name="pen-q2-pen_dataprodukt")

# bigquery
client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)

# # finner maks kjoretidspunkt fra bigquery-tabellen
# query = f"select max(kjoretidspunkt) as maks_kjoretidspunkt_bq from `{full_table_id}`"


sql_dev = """select * from pen_dataprodukt.snapshot_saksbehandlingsstatistikk
where dbt_valid_to is not null"""
# where kjoretidspunkt > to_timestamp('{maks_kjoretidspunkt_bq}')"""

tuning = 1000
con = pesys_utils.connect_to_oracle()
df_bq = pesys_utils.df_from_sql(sql_dev, con)

job_config = LoadJobConfig(create_disposition="CREATE_IF_NEEDED")

start = time()
job = client.load_table_from_dataframe(df_bq, full_table_id, job_config=job_config)
job.result()
end = time()

print(f"{len(df_bq)} rader ble skrevet til bigquery etter {end - start} sekunder.")

