import logging
from time import time
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pesys_utils


logging.basicConfig(level=logging.INFO)

pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
tuning = 10000
con = pesys_utils.connect_to_oracle()
df_kontrollpunkt = pesys_utils.pandas_from_sql(
    sqlfile="../sql/kontrollpunkt.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()

client = Client(project="pensjon-saksbehandli-prod-1f83")
table_id = "pensjon-saksbehandli-prod-1f83.kontrollpunkt.kontrollpunkt_daglig"
job_config = LoadJobConfig(write_disposition="WRITE_TRUNCATE")

start = time()
job = client.load_table_from_dataframe(df_kontrollpunkt, table_id, job_config=job_config)
job.result()
end = time()

print(f"{len(df_kontrollpunkt)} rad(er) ble skrevet til bigquery etter {end-start} sekunder.")
