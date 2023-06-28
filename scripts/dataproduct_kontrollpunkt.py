import pandas as pd
import os

from datetime import datetime
from time import time
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pandas_utils, pesys_utils, utils


def overwrite_dataproduct():
    utils.set_secrets_as_env(split_on=":", secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    df = make_df()
    write_to_bq(df)


def make_df():
    tuning = 10000
    con = pesys_utils.open_pen_connection()
    df_kontrollpunkt = pandas_utils.pandas_from_sql('../sql/kontrollpunkt.sql', con=con, tuning=tuning, lowercase=True)
    con.close()
    return df_kontrollpunkt


def write_to_bq(df):
    client = Client(project="pensjon-saksbehandli-prod-1f83")

    table_id = 'pensjon-saksbehandli-prod-1f83.kontrollpunkt.kontrollpunkt_daglig'
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    start = time()
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    end = time()

    print(f'{len(df_kontrollpunkt)} rad(er) ble skrevet til bigquery etter {end-start} sekunder.')
