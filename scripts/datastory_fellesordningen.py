import pandas as pd
import numpy as np
import os
import sys
import importlib
import plotly.express as px

from datetime import datetime
from datastory import DataStory

from lib import pandas_utils, pesys_utils, utils

def update_datastory():
    utils.set_secrets_as_env(split_on=":", secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    df = make_df()
    figs = make_figs(df)
    story = make_datastory(figs)
    story.update(url="https://datamarkedsplassen.intern.nav.no/api", token=os.environ["AFP_PRIV_STORY_TOKEN"])


def make_df():
    con = pesys_utils.open_pen_connection()
    df = pandas_utils.pandas_from_sql('../sql/afp_priv_res.sql', con, tuning=1000)
    con.close()

    df.columns = map(str.lower, df.columns)

    df = df.dropna()
    df["vedtaksdato"] = pd.to_datetime(df.vedtaksdato)

    df.set_index('vedtaksdato',inplace=True)

    df['year']=df.index.year
    df['month']=df.index.month
    df['day']=df.index.day

    return df[df.year >= 2022]


def make_figs(df):
    figs = {}
    figs["daglige_vedtak"] = px.bar(df, x=df.index, y="antall", color="resultat", color_discrete_map={"INNVILGET": 'green', "FEILREG": 'orange', "AVSLATT": 'red', "TRUKKET": 'purple'})

    df_mnd = df.groupby("resultat")[["antall"]].resample('MS').sum().reset_index()
    figs["månedlige_vedtak"] = px.bar(df_mnd, df_mnd.vedtaksdato, "antall", color="resultat", color_discrete_map={"INNVILGET": 'green', "FEILREG": 'orange', "AVSLATT": 'red', "TRUKKET": 'purple'})

    return figs


def make_datastory(figs):
    ds = DataStory("AFP Privat resultater")
    ds.markdown("*Dette er ikke offentlig statistikk*. Tall hentet fra `PEN.T_AFP_PRIV_RES_FK`")
    ds.markdown("Vedtak per dag")
    ds.plotly(figs["daglige_vedtak"].to_json())
    ds.markdown("Vedtak per måned")
    ds.plotly(figs["månedlige_vedtak"].to_json())
    
    return ds