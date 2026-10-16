import logging
import sys
from google.cloud.bigquery import LoadJobConfig
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

logging.basicConfig(level=logging.INFO)

# BigQuery table names
bq_aarlig_omregning_manuelt = "pensjon-saksbehandli-prod-1f83.aarlig_omregning.aarlig_omregning_manuelt"
bq_aarlig_omregning_oppgavetyper = "pensjon-saksbehandli-prod-1f83.aarlig_omregning.aarlig_omregning_oppgavetyper"

# Oracle PEN
tuning = 10000
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()

df_manuelt = pesys_utils.pandas_from_sql(
    "../sql/aarlig_omregning_manuelt.sql", con=con, tuning=tuning, lowercase=True
)
df_oppgavetyper = pesys_utils.pandas_from_sql(
    "../sql/aarlig_omregning_oppgavetyper.sql", con=con, tuning=tuning, lowercase=True
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
job1 = client.load_table_from_dataframe(df_manuelt, bq_aarlig_omregning_manuelt, job_config=job_config).result()
job1.result()

job2 = client.load_table_from_dataframe(df_oppgavetyper, bq_aarlig_omregning_oppgavetyper, job_config=job_config).result()
job2.result()
