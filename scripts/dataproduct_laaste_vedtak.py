import pandas as pd
import os

from datetime import datetime
from time import time
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pandas_utils, pesys_utils, utils


def overwrite_dataproduct():
    utils.set_secrets_as_env(split_on=":", secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    df = make_df()
    df_to_bq(
        project_id='pensjon-saksbehandli-prod-1f83',
        full_table_id='pensjon-saksbehandli-prod-1f83.vedtak.laast_data_handling',
        dataframe=df,
        write_disposition='WRITE_TRUNCATE'        
    )


def make_df():
    tuning = 1000
    con = pesys_utils.open_pen_connection()
    df_bq = pandas_utils.pandas_from_sql('../sql/laaste_vedtak.sql', con=con, tuning=tuning, lowercase=True)
    con.close()


def df_to_bq(project_id, full_table_id, dataframe, write_disposition):
    client = Client(project="pensjon-saksbehandli-prod-1f83")
    job_config = LoadJobConfig(write_disposition=write_disposition)
    
    start = time()
    job = client.load_table_from_dataframe(dataframe, full_table_id, job_config=job_config)
    job.result()

    end = time()

    print(f'{len(dataframe)} rader ble skrevet til bigquery etter {end-start} sekunder.')
