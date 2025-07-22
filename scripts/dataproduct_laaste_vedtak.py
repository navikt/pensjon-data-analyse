import logging
from time import time
from google.cloud.bigquery import Client, LoadJobConfig

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils

full_table_id = "pensjon-saksbehandli-prod-1f83.vedtak.laast_data_handling"
# Metabase, se https://metabase.ansatt.nav.no/reference/databases/370/tables/3462/questions


logging.basicConfig(level=logging.INFO)
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_airflow")

tuning = 1000
con = pesys_utils.connect_to_oracle()
df_bq = pesys_utils.pandas_from_sql(
    sqlfile="../sql/laaste_vedtak.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()

client = Client(project="pensjon-saksbehandli-prod-1f83")
job_config = LoadJobConfig(write_disposition="WRITE_TRUNCATE")

start = time()
job = client.load_table_from_dataframe(df_bq, full_table_id, job_config=job_config)
job.result()
end = time()

print(f"{len(df_bq)} rader ble skrevet til bigquery etter {end - start} sekunder.")
