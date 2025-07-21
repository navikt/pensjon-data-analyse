import logging
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pesys_utils

logging.basicConfig(level=logging.INFO)

bq_aldersovergang_behandle_bruker = "pensjon-saksbehandli-prod-1f83.aldersovergang.aldersovergang_behandle_bruker"
bq_aldersovergang_brev = "pensjon-saksbehandli-prod-1f83.aldersovergang.aldersovergang_brev"
# Metabase for team Alder, se https://metabase.ansatt.nav.no/dashboard/665-aldersovergang-dashbord

# oracle PEN
tuning = 10000
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()
df_aldersovergang_behandle_bruker = pesys_utils.pandas_from_sql(
    "../sql/aldersovergang_behandle_bruker.sql", con=con, tuning=tuning, lowercase=True
)
df_aldersovergang_brev = pesys_utils.pandas_from_sql(
    "../sql/aldersovergang_brev.sql", con=con, tuning=tuning, lowercase=True
)
con.close()


# bigquery
client = Client(project="pensjon-saksbehandli-prod-1f83")
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)



job1 = client.load_table_from_dataframe(
    df_aldersovergang_behandle_bruker, bq_aldersovergang_behandle_bruker, job_config=job_config
)
job1.result()

job2 = client.load_table_from_dataframe(df_aldersovergang_brev, bq_aldersovergang_brev, job_config=job_config)
job2.result()
