import logging
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pesys_utils

logging.basicConfig(level=logging.INFO)

# oracle
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
tuning = 10000
con = pesys_utils.connect_to_oracle()
df_vedtakstyper = pesys_utils.pandas_from_sql(
    sqlfile="../sql/vedtakstyper.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()

df_vedtakstyper = df_vedtakstyper.sort_values(by=["armaned", "vedtakstype"], ascending=[False, False])
df_vedtakstyper["ar"] = df_vedtakstyper["ar"].astype(int)


# bigquery
bq_prosjekt = "pensjon-saksbehandli-prod-1f83"
client = Client(project=bq_prosjekt)
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)
bq_target = f"{bq_prosjekt}.vedtak.vedtakstyper"
run_job = client.load_table_from_dataframe(df_vedtakstyper, bq_target, job_config=job_config)
run_job.result()
