import logging
from datetime import datetime
from google.cloud.bigquery import LoadJobConfig, SchemaField, enums

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

# OBS! Dette kjøres som en append i BQ, så ved dobbeltkjøring vil det bli duplikater
# Det betyr også at endring på tabellen vil fjerne historikk

table_id = "pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus"
table_id_med_kravarsak = "pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus_med_kravarsak"
# Metabase, se https://metabase.ansatt.nav.no/reference/databases/1394/tables/21738/questions
# Metabase, se https://metabase.ansatt.nav.no/reference/databases/212/tables/1701/questions

logging.basicConfig(level=logging.INFO)
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")

tuning = 10000
con = pesys_utils.connect_to_oracle()
df_kravstatus = pesys_utils.pandas_from_sql(
    sqlfile="../sql/kravstatus.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
df_kravstatus_med_kravarsak = pesys_utils.pandas_from_sql(
    sqlfile="../sql/kravstatus_med_kravarsak.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()
df_kravstatus.columns = map(str.lower, df_kravstatus.columns)
df_kravstatus["dato"] = datetime.now()
df_kravstatus_med_kravarsak.columns = map(str.lower, df_kravstatus_med_kravarsak.columns)
df_kravstatus_med_kravarsak["dato"] = datetime.now()

job_config = LoadJobConfig(
    schema=[
        SchemaField("sakstype", enums.SqlTypeNames.STRING),
        SchemaField("kravtype", enums.SqlTypeNames.STRING),
        SchemaField("kravstatus", enums.SqlTypeNames.STRING),
        SchemaField("antall", enums.SqlTypeNames.INTEGER),
        SchemaField("dato", enums.SqlTypeNames.TIMESTAMP),
    ],
    write_disposition="WRITE_APPEND",
)

job2_config = LoadJobConfig(
    schema=[
        SchemaField("sakstype", enums.SqlTypeNames.STRING),
        SchemaField("kravtype", enums.SqlTypeNames.STRING),
        SchemaField("kravstatus", enums.SqlTypeNames.STRING),
        SchemaField("kravarsak", enums.SqlTypeNames.STRING),
        SchemaField("antall", enums.SqlTypeNames.INTEGER),
        SchemaField("dato", enums.SqlTypeNames.TIMESTAMP),
    ],
    write_disposition="WRITE_APPEND",
)

client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)

job = client.load_table_from_dataframe(df_kravstatus, table_id, job_config=job_config)
job.result()
print(f"Table {table_id} successfully updated")

job2 = client.load_table_from_dataframe(df_kravstatus_med_kravarsak, table_id_med_kravarsak, job_config=job2_config)
job2.result()
print(f"Table {table_id_med_kravarsak} successfully updated")
