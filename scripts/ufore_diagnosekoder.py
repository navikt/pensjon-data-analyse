import sys
import logging
from pathlib import Path
from google.cloud.bigquery import LoadJobConfig
sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils
logging.basicConfig(level=logging.INFO)

bq_target = "pensjon-saksbehandli-prod-1f83.stonadsstatistikk.dataprodukt_ufore_diagnosekoder"

pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()
df_diagnosekoder = pesys_utils.df_from_sql(
    sql = "select * from pen_dataprodukt.dataprodukt_ufore_diagnosekoder",
    connection=con,
    arraysize=10000,
)
con.close()

client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)
run_job = client.load_table_from_dataframe(df_diagnosekoder, bq_target, job_config=job_config)
run_job.result()
