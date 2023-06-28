import pandas as pd
import numpy as np
import os

import plotly.express as px
from datetime import datetime
from datastory import DataStory

from lib import pandas_utils, pesys_utils, utils


def update_datastory():
    utils.set_secrets_as_env(split_on=":", secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    df = make_df()
    story = make_datastory(fig)
    story.update(url="https://nada.intern.nav.no/api", token=os.environ["GLEMTE_KRAV_STORY_TOKEN"])


def make_df():
    con = pesys_utils.open_pen_connection()
    df = pandas_utils.pandas_from_sql('../sql/levetid_krav.sql', con, tuning=1000)
    con.close()
    df.columns = map(str.lower, df.columns)
    
    df["dato"] = datetime.utcnow()
    df["dato_opprettet"] = df.dato_opprettet.apply(pd.Timestamp.to_pydatetime)
    df["dato_endret"] = df.dato_endret.apply(pd.Timestamp.to_pydatetime)

    df["levetid"] = df.dato.subtract(df.dato_opprettet).dt.days
    df["uendret"] = df.dato.subtract(df.dato_endret).dt.days

    df_krav_over_ett_ar = df[df.uendret > 365]
    anke_filter =  ~((df_krav_over_ett_ar.sakstype == "Uføretrygd") & (df_krav_over_ett_ar.kravtype == "Anke"))
    return df_krav_over_ett_ar[anke_filter]


def make_fig(df):
    return px.sunburst(df, path=["sakstype", "kravtype", "kravstatus"], color_discrete_sequence=px.colors.qualitative.Dark2, height=1000)


def make_datastory(fig):
    ds = DataStory('"Glemte krav" i pesys')
    ds.markdown("Oversikt over krav i pesys hvor 'endret_dato' er over et år siden, og status ikke er 'FERDIG' eller 'AVBRUTT'. Anker på Uføretrygd er ikke inkludert.")
    ds.plotly(fig.to_json())
    return ds