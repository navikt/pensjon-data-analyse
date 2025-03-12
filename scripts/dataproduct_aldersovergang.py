import logging
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pesys_utils

logging.basicConfig(level=logging.INFO)
pesys_utils.set_pen_secrets_as_env()

def main():
    # oracle
    tuning = 10000
    con = pesys_utils.open_pen_connection()
    df_aldersovergang_behandle_bruker = pesys_utils.pandas_from_sql('../sql/aldersovergang_behandle_bruker.sql', con=con, tuning=tuning, lowercase=True)
    df_aldersovergang_brev = pesys_utils.pandas_from_sql('../sql/aldersovergang_brev.sql', con=con, tuning=tuning, lowercase=True)
    con.close()


    # bigquery
    client = Client(project="pensjon-saksbehandli-prod-1f83")
    job_config = LoadJobConfig(write_disposition="WRITE_TRUNCATE", create_disposition="CREATE_IF_NEEDED",)

    bq_datasett = 'pensjon-saksbehandli-prod-1f83.aldersovergang'
    bq_aldersovergang_behandle_bruker = f'{bq_datasett}.aldersovergang_behandle_bruker'
    bq_aldersovergang_brev = f'{bq_datasett}.aldersovergang_brev'


    job1 = client.load_table_from_dataframe(df_aldersovergang_behandle_bruker, bq_aldersovergang_behandle_bruker, job_config=job_config)
    job1.result()

    job2 = client.load_table_from_dataframe(df_aldersovergang_brev, bq_aldersovergang_brev, job_config=job_config)
    job2.result()

if __name__ == "__main__":
    main()
    