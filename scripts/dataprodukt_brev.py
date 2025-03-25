import logging
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pesys_utils

logging.basicConfig(level=logging.INFO)

# oracle
pesys_utils.set_db_secrets(secret_name='pen-PROD-dvh_dataprodukt')
tuning = 10000
con = pesys_utils.connect_to_oracle()
df_autobrev_inntektsendring = pesys_utils.pandas_from_sql(
    sqlfile="../sql/autobrev_inntektsendring.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()

# bigquery
client = Client(project="pensjon-saksbehandli-prod-1f83")
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)

bq_datasett = "pensjon-saksbehandli-prod-1f83.brev"
bq_autobrev_inntektsendring = f"{bq_datasett}.autobrev_inntektsendring"


run_job = client.load_table_from_dataframe(
    df_autobrev_inntektsendring, bq_autobrev_inntektsendring, job_config=job_config
)
run_job.result()
