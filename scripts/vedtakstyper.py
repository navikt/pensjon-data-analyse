import logging
from google.cloud.bigquery import LoadJobConfig

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

logging.basicConfig(level=logging.INFO)

bq_target = "pensjon-saksbehandli-prod-1f83.vedtak.vedtakstyper"
# Metabase, se https://metabase.ansatt.nav.no/reference/databases/1416/tables/22180/questions
# Datamarkedsplassen / Krav og vedtak i Pesys / Månedlig antall uførevedtak


# oracle
pesys_utils.set_db_secrets(
    secret_name="pen-prod-pen_dataprodukt"
)  # TODO: bytt tilbake til lesekopien etter brannmuråpning
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
client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)
run_job = client.load_table_from_dataframe(df_vedtakstyper, bq_target, job_config=job_config)
run_job.result()
