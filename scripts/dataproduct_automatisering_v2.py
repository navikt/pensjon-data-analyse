import pandas as pd
import numpy as np
import os
import plotly.express as px
import plotly.graph_objects as go

from datetime import datetime
from datastory import DataStory
from google.cloud.bigquery import Client, LoadJobConfig

from lib import pandas_utils, pesys_utils, utils


def overwrite_dataproduct():
    utils.set_secrets_as_env(split_on=":", secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    
    current_year = datetime.now().year
    N = (current_year + 1) - 2008
    years = [str(current_year-(i-1)) for i in range(N)]

    overwrite_vedtak(N, years)
    overwrite_krav(N, years)
    

def overwrite_vedtak(N, years):
    con = pesys_utils.open_pen_connection()
    with open('../sql/forslag_auto.sql') as sql:
        query = sql.read()

    client = Client(project="pensjon-saksbehandli-prod-1f83")

    table_id = 'pensjon-saksbehandli-prod-1f83.vedtak.vedtak_automatisering_v2'
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    tuning = 100000

    with con.cursor() as cursor:
        cursor.prefetchrows = tuning
        cursor.arraysize = tuning

        for i in range(1,N):
        
            start = time()

            cursor.execute(query.replace('x_year', years[i]).replace('y_year', years[i-1]))

            df_one_year = pd.DataFrame(cursor.fetchall())

            end = time()

            print(f'{len(df_one_year)} rad(er) ble returnert etter {end-start} sekunder for perioden {years[i]}-{years[i-1]}.')
            
            start = time()
            
            if len(df_one_year) > 0:
                    df_one_year.columns = [x[0].lower() for x in cursor.description]
            df_one_year["dato_virk_fom"] = df_one_year["dato_virk_fom"].dt.floor('D')

            job = client.load_table_from_dataframe(df_one_year, table_id, job_config=job_config)
            job.result()
            
            end = time()
            
            print(f'{len(df_one_year)} rad(er) ble skrevet til bigquery etter {end-start} sekunder for perioden {years[i]}-{years[i-1]}.')
            
            job_config = LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
    con.close()
    print(f"Table {table_id} successfully overwritten")


def overwrite_krav(N, years):
    con = pesys_utils.open_pen_connection()
        
    with open('../sql/forslag_auto_krav.sql') as sql:
        query = sql.read()

    client = Client(project="pensjon-saksbehandli-prod-1f83")

    table_id = 'pensjon-saksbehandli-prod-1f83.saksstatistikk.krav_automatisering'
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    tuning = 100000

    with con.cursor() as cursor:
        cursor.prefetchrows = tuning
        cursor.arraysize = tuning

        for i in range(1,N):
        
            start = time()

            cursor.execute(query.replace('x_year', years[i]).replace('y_year', years[i-1]))

            df_one_year = pd.DataFrame(cursor.fetchall())

            end = time()

            print(f'{len(df_one_year)} rad(er) ble returnert etter {end-start} sekunder for perioden {years[i]}-{years[i-1]}.')

            if len(df_one_year) > 0:
                    df_one_year.columns = [x[0].lower() for x in cursor.description]
            df_one_year["dato_opprettet"] = df_one_year["dato_opprettet"].dt.floor('D')

            job = client.load_table_from_dataframe(df_one_year, table_id, job_config=job_config)
            job.result()

            job_config = LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
    con.close()
    print(f"Table {table_id} successfully overwritten")
