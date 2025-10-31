import sys
import logging
from pathlib import Path
from google.cloud.bigquery import LoadJobConfig
sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

logging.basicConfig(level=logging.INFO)

# Metabase-dashbord for feilutbetalinger, se https://metabase.ansatt.nav.no/dashboard/903-feilutbetalinger-alderspensjon
bq_feilutbetalinger = "pensjon-saksbehandli-prod-1f83.feilutbetalinger.feilutbetalinger"
bq_feilutbetalinger_dodsalder = "pensjon-saksbehandli-prod-1f83.feilutbetalinger.feilutbetalinger_dodsalder"
bq_dodsfall_alderspensjon = "pensjon-saksbehandli-prod-1f83.feilutbetalinger.dodsfall_alderspensjon"
bq_feilutbetalinger_med_avtaleland = "pensjon-saksbehandli-prod-1f83.feilutbetalinger.feilutbetalinger_med_avtaleland"

# oracle PEN
tuning = 10000
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()
df_feilutbetalinger = pesys_utils.pandas_from_sql(
    "../sql/feilutbetalinger_alderspensjonister/feilutbetalinger.sql", con=con, tuning=tuning, lowercase=True
)
df_feilutbetalinger_dodsalder = pesys_utils.pandas_from_sql(
    "../sql/feilutbetalinger_alderspensjonister/feilutbetalinger_dodsalder.sql", con=con, tuning=tuning, lowercase=True
)
df_dodsfall_alderspensjon = pesys_utils.pandas_from_sql(
    "../sql/feilutbetalinger_alderspensjonister/dodsfall_alderspensjon.sql", con=con, tuning=tuning, lowercase=True
)
df_feilutbetalinger_med_avtaleland = pesys_utils.pandas_from_sql(
    "../sql/feilutbetalinger_alderspensjonister/feilutbetalinger_med_avtaleland.sql", con=con, tuning=tuning, lowercase=True
)
con.close()


client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)


job1 = client.load_table_from_dataframe(df_feilutbetalinger, bq_feilutbetalinger, job_config=job_config)
job1.result()

job2 = client.load_table_from_dataframe(df_feilutbetalinger_dodsalder, bq_feilutbetalinger_dodsalder, job_config=job_config)
job2.result()

job3 = client.load_table_from_dataframe(df_dodsfall_alderspensjon, bq_dodsfall_alderspensjon, job_config=job_config)
job3.result()

job4 = client.load_table_from_dataframe(df_feilutbetalinger_med_avtaleland, bq_feilutbetalinger_med_avtaleland, job_config=job_config)
job4.result()
