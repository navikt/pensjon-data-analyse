import logging
import sys
from google.cloud.bigquery import LoadJobConfig
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

logging.basicConfig(level=logging.INFO)

# BigQuery table names
bq_manedlig_omregning_manuelt = "pensjon-saksbehandli-prod-1f83.manedlig_omregning.manedlig_omregning_manuelt"
bq_manedlig_omregning_okning = "pensjon-saksbehandli-prod-1f83.manedlig_omregning.manedlig_omregning_okning"
bq_manedlig_omregning_reduksjon = "pensjon-saksbehandli-prod-1f83.manedlig_omregning.manedlig_omregning_reduksjon"
bq_manedlig_omregning_oppgave = "pensjon-saksbehandli-prod-1f83.manedlig_omregning.manedlig_omregning_oppgave"

# Oracle PEN
tuning = 10000
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()

df_manuelt = pesys_utils.pandas_from_sql(
    "../sql/manedlig_omregning_manuelt.sql", con=con, tuning=tuning, lowercase=True
)
df_okning = pesys_utils.pandas_from_sql(
    "../sql/manedlig_omregning_okning.sql", con=con, tuning=tuning, lowercase=True
)
df_reduksjon = pesys_utils.pandas_from_sql(
    "../sql/manedlig_omregning_reduksjon.sql", con=con, tuning=tuning, lowercase=True
)
df_oppgave = pesys_utils.pandas_from_sql(
    "../sql/manedlig_omregning_oppgave.sql", con=con, tuning=tuning, lowercase=True
)
con.close()

# bigquery
client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83",
    target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)

# Load DataFrames to BigQuery
client.load_table_from_dataframe(df_manuelt, bq_manedlig_omregning_manuelt, job_config=job_config).result()
client.load_table_from_dataframe(df_okning, bq_manedlig_omregning_okning, job_config=job_config).result()
client.load_table_from_dataframe(df_reduksjon, bq_manedlig_omregning_reduksjon, job_config=job_config).result()
client.load_table_from_dataframe(df_oppgave, bq_manedlig_omregning_oppgave, job_config=job_config).result()
