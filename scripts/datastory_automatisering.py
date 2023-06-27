import sys
import os
import cx_Oracle
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from plotly.subplots import make_subplots
from datastory import DataStory
from google.cloud import secretmanager

from lib import pandas_utils, pesys_utils, utils


def update_datastory():
    utils.set_secrets_as_env(secret_name="projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest")
    df = df_from_pen()
    df = add_aar_maaned(df)
    df["STØNADSOMRÅDE"] = df.STØNADSOMRÅDE.apply(pesys_utils.map_stonad)

    df_ap = df.copy().loc[(df.STØNADSOMRÅDE=="Alderspensjon") & (df.BATCH_FLAGG == "ikke opprettet av batch")]
    df_ap = df_ap.merge(df_ap.groupby("ÅR-MÅNED", as_index=False).ANTALL.sum(),
                    left_on="ÅR-MÅNED",
                    right_on="ÅR-MÅNED",
                    suffixes=(""," TOTALT")
            )
    df_ap["ANDEL"] = df_ap.ANTALL.divide(df_ap["ANTALL TOTALT"])

    pastel = px.colors.qualitative.Pastel
    
    figs = {}

    df_auto = make_df_auto(df_ap)
    figs["autograd"] = make_fig_autograd(df_auto, pastel[0])

    df_selv = make_df_selv(df_ap)
    figs["selvbetjening"] = make_fig_selvbetjening(df_selv, pastel[1])

    story = make_datastory(title="Automatiserings- og selvbetjeningsgrad for alderspensjon")
    story.update(token=os.environ["AUTOMATISERING_STORY_TOKEN"], url="https://nada.intern.nav.no/api", )



def df_from_pen():
    con = pesys_utils.open_pen_connection()
    df_v3 = pandas_utils.pandas_from_sql('../sql/v3.sql', con)
    con.close()
    return df_v3


def add_aar_maaned(df):
    current_år = df.ÅR.max()
    current_måned = df.loc[df.ÅR==df.ÅR.max()].MÅNED.max()
    current_år_måned = '-'.join([str(current_år), str(current_måned)])
    df["ÅR-MÅNED"] = df[["ÅR", "MÅNED"]].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
    cols = list(df.columns.values)
    cols.insert(0, cols.pop())
    df = df[cols]
    return df.loc[df["ÅR-MÅNED"] != current_år_måned]


def make_df_auto(df_in):
    df_auto = df_in.groupby(["ÅR", "MÅNED", "ÅR-MÅNED", "AUTOMATISERING", "ANTALL TOTALT"], as_index=False)[["ANTALL", "ANDEL"]].sum()
    df_auto = df_auto[df_auto.AUTOMATISERING == "AUTO"].reset_index(drop=True)
    df_auto["ANDEL_PROSENT"] = df_auto["ANDEL"].apply(lambda x: round(x*100, 0)).astype(int).astype(str) + '%'
    return df_auto


def make_fig_autograd(df_in, barcolor):
    df_plot = df_in.copy()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df_plot["ÅR-MÅNED"], y=df_plot["ANTALL TOTALT"], marker_color=barcolor, name="Antall saker"), secondary_y=False)

    fig.add_trace(go.Scatter(x=df_plot["ÅR-MÅNED"], y=df_plot["ANDEL"], text=df_plot["ANDEL_PROSENT"], mode='lines+markers+text', marker_color="black", name="Automatisk", textposition='top center'), secondary_y=True)
    fig.update_yaxes(title_text="Andel automatisert", secondary_y=True, range=[0.2,0.8], tickformat='.0%')
    fig.update_yaxes(title_text="Antall saker", secondary_y=False)
    fig.layout.yaxis2.showgrid = False
    fig.update_layout(
        autosize=False,
        width=1300,
        height=500,)

    return fig


def make_df_selv(df_in):
    df_selv = df_in.groupby(["ÅR", "MÅNED", "ÅR-MÅNED", "OPPRETTET_AV", "ANTALL TOTALT"], as_index=False)[["ANTALL", "ANDEL"]].sum()
    df_selv = df_selv[df_selv.OPPRETTET_AV == "bruker"].reset_index(drop=True)
    df_selv["ANDEL_PROSENT"] = df_selv["ANDEL"].apply(lambda x: round(x*100, 0)).astype(int).astype(str) + '%'
    return df_selv


def make_fig_selvbetjening(df_in, barcolor):
    df_plot = df_in.copy()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df_plot["ÅR-MÅNED"], y=df_plot["ANTALL TOTALT"], marker_color=barcolor, name="Antall saker"), secondary_y=False)

    fig.add_trace(go.Scatter(x=df_plot["ÅR-MÅNED"], y=df_plot["ANDEL"], text=df_plot["ANDEL_PROSENT"], mode='lines+markers+text', marker_color="black", name="Selvbetjent", textposition='top center'), secondary_y=True)
    fig.update_yaxes(title_text="Andel selvbetjent", secondary_y=True, range=[0.5,1], tickformat='.0%')
    fig.update_yaxes(title_text="Antall saker", secondary_y=False)
    fig.layout.yaxis2.showgrid = False
    fig.update_layout(
        autosize=False,
        width=1300,
        height=500,)

    return fig

def make_datastory(title):
    ds = DataStory(title)

    ds.markdown("**Dette er ikke offisiell statistikk**")
    ds.markdown("**Merk at dette er en utdatert og unøyaktig måte å måle automatiseringsgrad.**")
    ds.header(f"Automatisering av førstegangsbehandlingssaker for alderspensjon i pesys", level=2)
    ds.markdown(f"Denne figuren gjelder kun førstegangsbehandling av alderspensjonssaker. Saker opprettet av batch er eksludert. For noen saker er deler av behandlingen automatisert, men disse regnes som manuelt behandlet.")
    ds.plotly(figs["autograd"].to_json())

    ds.header(f"Selvbetjening av førstegangsbehandlingssaker for alderspensjon i pesys", level=2)
    ds.markdown(f"En sak regnes som selvbetjent dersom den er opprettet av en bruker (ikke saksbehandler/batch) i selvbetjeningsløsningen. Denne figuren gjelder kun førstegangsbehandling av alderspensjonssaker. Saker opprettet av batch er eksludert.")
    ds.plotly(figs["selvbetjening"].to_json())

    return ds