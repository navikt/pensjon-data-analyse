import pandas as pd
from dataclasses import dataclass, field
import plotly
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict

names = ["Nullpunktsmåling", "Måling T1", "Måling T2", "Måling T3"]
categorical_colors = (
    "#0067C5",
    "#BA3A26",
    "#06893A",
    "#634689",
    "#FF9100",
    "#66CBEC",
    "#F0C419",
)
FIG_HEIGHT = 400

def make_fig(col: pd.core.series.Series, n: int):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=col.index[[3,4,6,8]],
                             y=col[names].values,
                             text=["", col['% endring mot NP'], col['% endring mot NP.1'], col['% endring mot NP.2']],
                             mode='lines+markers+text',
                             marker=dict(size=10, color=categorical_colors[n]),
                             textposition = 'top left'
                            ))
    fig.update_xaxes(title=None)
    fig.update_layout(
        autosize=False,
        height=FIG_HEIGHT,)
    if col.prosent_flagg:
        fig.update_yaxes(title=None, range=[0,1], tickformat='.0%')
    else:
        fig.update_yaxes(title=None, range=[0,max(col[names].values)*1.2])
    return fig


def make_subfig(col: pd.core.series.Series, n: int):
    fig = go.Scatter(x=col.index[[3,4,6,8]],
                             y=col[names].values,
                             text=["", col['% endring mot NP'], col['% endring mot NP.1'], col['% endring mot NP.2']],
                             mode='lines+markers+text',
                             marker=dict(size=10, color=categorical_colors[n]),
                             textposition = 'top left',
                             #name=col.Måleparameter
                            )
    return fig


@dataclass
class MP:
    name: str
    i: int
    fig: go.Figure
    col: pd.core.series.Series
    subfig: go.Scatter
    
    def __init__(self, name: str, i: int, fig: go.Figure, col: pd.core.series.Series, subfig: go.Scatter):
        self.name = name
        self.i = i
        self.fig = fig
        self.col = col
        self.subfig = subfig
        
    def show_fig(self):
        print(self.name)
        self.fig.show()
        sub = make_subplots().add_trace(self.subplot).show()
        

    @classmethod
    def from_series(cls, col: pd.core.series.Series, i: int, n: int):
        return MP(col.Måleparameter,
                  i,
                  make_fig(col, n),
                  col,
                  make_subfig(col, n))

    
@dataclass
class Operative:
    name: str
    n_mp: int
    MPs: list
    big_fig: go.Figure
    
    def __init__(self, name: str, n_mp: int):
        self.name = name
        self.n_mp = n_mp
        self.MPs = []
        self.big_fig = None
    
    def show_figs(self):
        print(self.name)
        for mp in self.MPs:
            print(mp.name)
            mp.show_fig()
            
    def make_big_fig(self):
        fig = make_subplots(rows=((self.n_mp+1) // 2), 
                            cols=min(self.n_mp, 2),
                            subplot_titles=([MP.name for MP in self.MPs]))
        for MP in self.MPs:
            row = (MP.i + 1) // 2
            col = -(MP.i % 2 - 2)
            fig.add_trace(MP.subfig, row=row, col=col)
            if MP.col.prosent_flagg:
                fig.update_yaxes(title=None, range=[0,1], tickformat='.0%', row=row, col=col)
            else:
                fig.update_yaxes(title=None, range=[0,max(MP.col[names].values)*1.2], row=row, col=col)
        fig.update_layout(height=((self.n_mp+1) // 2) * FIG_HEIGHT)
        #fig.update_annotations(font_size=12)
        fig.update_layout(showlegend=False)
        return fig
        