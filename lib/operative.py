import pandas as pd
from dataclasses import dataclass, field
import plotly
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict

names = ["Nullpunktsmåling", "Måling T1", "Måling T2", "Måling T3"]

def make_fig(col: pd.core.series.Series):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=col.index[[3,4,6,8]], y=col[names].values, marker=dict( size = [10 for i in range(3)] )))
    fig.update_xaxes(title=None)
    if col.prosent_flagg:
        fig.update_yaxes(title=None, range=[0,1], tickformat='.0%')
    else:
        fig.update_yaxes(title=None, rangemode='nonnegative')
    return fig


@dataclass
class MP:
    name: str
    fig: go.Figure
    col: pd.core.series.Series
    
    def __init__(self, name: str, fig: go.Figure, col: pd.core.series.Series):
        self.name = name
        self.fig = fig
        self.col = col
        
    def show_fig(self):
        print(self.name)
        self.fig.show()
        

    @classmethod
    def from_series(cls, col: pd.core.series.Series):
        return MP(col.Måleparameter,
                  make_fig(col),
                  col)

@dataclass
class Operative:
    name: str
    MPs: Dict[str, MP]
    
    def __init__(self, name: str):
        self.name = name
        self.MPs = {}
        self.nMPS = 0
    
    def show_figs(self):
        for _, mp in self.MPs.items():
            print(self.name)
            mp.show_fig()