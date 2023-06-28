import cx_Oracle
import datastory
import pandas as pd
import numpy as np
import os
import plotly.express as px
import plotly.graph_objects as go

from datetime import datetime
from google.cloud import secretmanager

from lib import pandas_utils, pesys_utils, utils


def update_datastory():
    utils.set_secrets_as_env(split_on=':', secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    figs = {}
    figs.update(make_figs_krav_utland())
    figs.update(make_figs_ledetid())
    
    story = make_datastory()
    story.update(url="https://nada.intern.nav.no/api", token=os.environ["ETTERLATTE_STORY_TOKEN"])


def make_figs_krav_utland():
    con = pesys_utils.open_pen_connection()
    df = pandas_utils.pandas_from_sql('../sql/GP_BP/kravcount.sql', con)
    con.close()
    figs = {} 

    df["MANED"] = df.MANED.astype('string').apply(lambda x: pandas_utils.add_zero_to_mnd(x))
    df["AR_MANED"] = df.AR_MANED.apply(lambda x: pandas_utils.add_zero_to_aar_mnd(x))
    figs["gjenlev_mmd"] = px.bar(df[df.STONAD == "Gjenlevendeytelse"], "AR_MANED", "ANTALL", color="KRAVTYPE", labels={"AR_MANED": "Måned", "ANTALL":"Antall", "KRAVTYPE": "Kravtype"})
    figs["barnep_mmd"] = px.bar(df[df.STONAD == "Barnepensjon"], "AR_MANED", "ANTALL", color="KRAVTYPE", labels={"AR_MANED": "Måned", "ANTALL":"Antall", "KRAVTYPE": "Kravtype"})

    df_ar = (df
        .groupby(["AR", "STONAD", "KRAVTYPE"], as_index=False)
        .ANTALL
        .sum())
    df_ar["AR"] = df_ar["AR"].apply(str)

    figs["barnep_ar"] = px.bar(df_ar[df_ar.STONAD == "Barnepensjon"], "AR", "ANTALL", color="KRAVTYPE", labels={"AR": "År", "ANTALL":"Antall", "KRAVTYPE": "Kravtype"})
    figs["gjenlev_ar"] = px.bar(df_ar[df_ar.STONAD == "Gjenlevendeytelse"], "AR", "ANTALL", color="KRAVTYPE", labels={"AR": "År", "ANTALL":"Antall", "KRAVTYPE": "Kravtype"})

    return figs


def make_figs_ledetid():
    con = pesys_utils.open_pen_connection()
    df = pandas_utils.pandas_from_sql('../sql/GP_BP/ledetid.sql', con)
    con.close()
    figs = {}

    labels = {
    "GJ_DAGER": "Gjennomsnittlig antall dager",
    "AR": "År",
    "STOENAD": "Stønad",
    "KRAVTYPE": "Kravtype",
    "MED_DAGER": "Median av antall dager",
    "ANTALL": "Antall krav",
    "RESULTAT": "Resultat"
    }

    df_bp = df[df.STOENAD == "Barnepensjon"]
    df_gjenlev = df[df.STOENAD == "Gjenlevendeytelse"]

    figs["barnep_gj_dager"] = px.bar(
        df_bp[df_bp.ANTALL >= 5], 
        "AR", 
        "GJ_DAGER", 
        color="KRAVTYPE", 
        barmode="group",
        hover_data=df_bp.columns[[2,3,4,5]],
        labels=labels
        )

    figs["barnep_med_dager"] = px.bar(
        df_bp[df_bp.ANTALL >= 5], 
        "AR", 
        "MED_DAGER", 
        color="KRAVTYPE", 
        barmode="group", 
        hover_data=df_bp.columns[[2,3,4,5]],
        labels=labels
        )

    figs["gjenlev_gj_dager"] = px.bar(
        df_gjenlev[df_gjenlev.ANTALL >= 5], 
        "AR", 
        "GJ_DAGER", 
        color="KRAVTYPE", 
        barmode="group", 
        hover_data=df_gjenlev.columns[[2,3,4,5]],
        labels=labels
        )

    figs["gjenlev_med_dager"] = px.bar(
        df_gjenlev[df_gjenlev.ANTALL >= 5], 
        "AR", 
        "MED_DAGER", 
        color="KRAVTYPE", 
        barmode="group", 
        hover_data=df_gjenlev.columns[[2,3,4,5]],
        labels=labels
        )

    return figs

def make_datastory():
    story = datastory.DataStory("Gjenlevendepensjon og barnepensjon")

    # Krav innland utland
    story.header("Antall opprettede krav fordelt på innland/utland", level=1)

    story.header("Gjenlevendepensjon", level=2)
    story.header("Førstegangsbehandling gjenlevendepensjon per måned", level=3)
    story.plotly(figs["gjenlev_mmd"].to_json())
    story.header("Førstegangsbehandling gjenlevendepensjon per år", level=3)
    story.plotly(figs["gjenlev_ar"].to_json())

    story.header("Barnepensjon", level=2)
    story.header("Førstegangsbehandling barnepensjon per måned", level=3)
    story.plotly(figs["barnep_mmd"].to_json())
    story.header("Førstegangsbehandling barnepensjon per år", level=3)
    story.plotly(figs["barnep_ar"].to_json())

    # Tid fra mottatt krav til iverksatt vedtak
    story.header("Tid fra mottatt krav til iverksatt vedtak", level=1)
    story.markdown("År bestemmes av vedtaksdato. Inkluderer kun iverksatte vedtak.")
    story.header("Gjenlevendepensjon", level=2)
    story.markdown("Gjennomsnittlig antall dager. Hold pekeren over en stolpe for å se antall vedtak i denne kategorien.")
    story.plotly(figs["gjenlev_gj_dager"].to_json())
    story.markdown("Median av antall dager. Hold pekeren over en stolpe for å se antall vedtak i denne kategorien.")
    story.plotly(figs["gjenlev_med_dager"].to_json())

    story.header("Barnepensjon", level=2)
    story.markdown("Gjennomsnittlig antall dager. Hold pekeren over en stolpe for å se antall vedtak i denne kategorien.")
    story.plotly(figs["barnep_gj_dager"].to_json())
    story.markdown("Median av antall dager. Hold pekeren over en stolpe for å se antall vedtak i denne kategorien.")
    story.plotly(figs["barnep_med_dager"].to_json())

    return story