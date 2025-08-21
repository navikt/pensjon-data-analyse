import logging
from time import time
from google.cloud.bigquery import LoadJobConfig
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

table_id = "pensjon-saksbehandli-prod-1f83.kontrollpunkt.kontrollpunkt_daglig"
# Metabase, se https://metabase.ansatt.nav.no/reference/databases/353/tables/3320/questions


logging.basicConfig(level=logging.INFO)

pesys_utils.set_db_secrets(
    secret_name="pen-prod-pen_dataprodukt"
)  # TODO: bytt tilbake til lesekopien etter brannmuråpning
tuning = 10000
con = pesys_utils.connect_to_oracle()
df_kontrollpunkt = pesys_utils.pandas_from_sql(
    sqlfile="../sql/kontrollpunkt.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()

# bigquery
client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
job_config = LoadJobConfig(write_disposition="WRITE_TRUNCATE")

start = time()
job = client.load_table_from_dataframe(df_kontrollpunkt, table_id, job_config=job_config)
job.result()
end = time()

print(f"{len(df_kontrollpunkt)} rad(er) ble skrevet til bigquery etter {end - start} sekunder.")
