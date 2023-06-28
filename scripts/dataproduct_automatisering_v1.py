import pandas as pd
import os

from datetime import datetime
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pandas_utils, pesys_utils, utils


def overwrite_dataproduct():
    utils.set_secrets_as_env(split_on=":", secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    df = make_df()
    write_to_bq(df)

def make_df():
    con = pesys_utils.open_pen_connection()
    df = pandas_utils.pandas_from_sql('../sql/v7.sql', con)
    con.close()

    # Kan vurdere å sortere nullverdier i auto/man:
    df["KRAV_BEHANDLING"] = df.KRAV_BEHANDLING.fillna("USPESIFISERT")
    df["MAANED"] = df.MAANED.astype('string').apply(lambda x: pesys_utils.add_zero_to_mnd(x))
    df["AAR_MAANED"] = df.AAR_MAANED.apply(lambda x: pesys_utils.add_zero_to_aar_mnd(x))

    current_aar_maaned = datetime.today().strftime('%Y-%m')
    return df[df.AAR_MAANED != current_aar_maaned]

def write_to_bq(df):
    table_id = f'pensjon-saksbehandli-prod-1f83.vedtak.vedtak_automatisering'
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    client = Client(project="pensjon-saksbehandli-prod-1f83")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Table {table_id} successfully updated")
