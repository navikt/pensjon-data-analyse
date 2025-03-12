import logging
from datetime import datetime
from google.cloud.bigquery import Client, LoadJobConfig, SchemaField, enums

from lib import pesys_utils


logging.basicConfig(level=logging.INFO)
pesys_utils.set_pen_secrets_as_env()


con = pesys_utils.open_pen_connection()
df_kravstatus = pesys_utils.pandas_from_sql("../sql/kravstatus.sql", con)
con.close()
df_kravstatus.columns = map(str.lower, df_kravstatus.columns)
df_kravstatus["dato"] = datetime.now()

table_id = f"pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus"
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

client = Client(project="pensjon-saksbehandli-prod-1f83")

job = client.load_table_from_dataframe(df_kravstatus, table_id, job_config=job_config)
job.result()
print(f"Table {table_id} successfully updated")
