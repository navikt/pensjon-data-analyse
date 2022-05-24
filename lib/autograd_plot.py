import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots



nav_colors = (
    "#0067C5",
    "#BA3A26",
    "#06893A",
    "#634689",
    "#FF9100",
    "#66CBEC",
    "#F0C419",
)

def plot_automatisering1(df, box_color=px.colors.qualitative.Pastel[0]):
    '''
    df: en verdi per måned og automatiseringsgrad
    box_color: Farge på boxplot
    fig: Box for antall saker, line for automatiseringsgrad
    '''
    df_tot = df.groupby("År-måned", as_index=False)["Antall innkomne saker"].sum()
    
    _automatisk = df["Automatiseringsgrad"] == "Automatisk"
    _manuell = df["Automatiseringsgrad"] == "Manuell"
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(go.Bar(x=df_tot["År-måned"], y=df_tot["Antall innkomne saker"], 
                         name="Antall saker", marker_color=box_color), 
                  secondary_y=False)
    
    fig.add_trace(go.Scatter(x=df[_automatisk]["År-måned"], y=df[_automatisk]["Andel"], 
                             name="Automatisk", mode="lines+markers",
                             line={'dash': 'solid', 'color': 'black'}),
                  secondary_y=True)
    
    fig.add_trace(go.Scatter(x=df[~_automatisk & ~_manuell]["År-måned"], y=df[~_automatisk & ~_manuell]["Andel"], 
                             name="Del-automatisk", mode="lines",
                             line={'dash': 'solid', 'color': 'grey'}), 
                  secondary_y=True)
    
    fig.update_yaxes(title_text="Andel automatisert", secondary_y=True, range=[0,1], tickformat='.0%')
    fig.update_yaxes(title_text="Antall saker", range=[0,max(df_tot["Antall innkomne saker"])*1.1], secondary_y=False)
    fig.layout.yaxis2.showgrid = False
    fig.update_layout(
        autosize=False,
        width=1300,
        height=500,)
    return fig


def plot_automatisering2(df):
    '''
    df: en verdi per måned, automatiseringsgrad
    fig: Stacked box for automatiseringsgrad, line for totalt antall
    '''
    df_tot = df.groupby("År-måned", as_index=False)["Antall innkomne saker"].sum()
    
    _automatisk = df["Automatiseringsgrad"] == "Automatisk"
    _manuell = df["Automatiseringsgrad"] == "Manuell"
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(go.Scatter(x=df_tot["År-måned"], y=df_tot["Antall innkomne saker"], mode="lines", marker_color="rgb(0,0,0)", name="Totalt antall saker"), secondary_y=True)
    
    fig.add_trace(go.Bar(x=df[_automatisk]["År-måned"], y=df[_automatisk]["Andel"], marker_color=automatiserings_colors[0], name="Automatisk"), secondary_y=False)
    fig.add_trace(go.Bar(x=df[~_automatisk & ~_manuell]["År-måned"], y=df[~_automatisk & ~_manuell]["Andel"], marker_color=automatiserings_colors[1], name="Del-automatisk"), secondary_y=False)
    fig.add_trace(go.Bar(x=df[_manuell]["År-måned"], y=df[_manuell]["Andel"], marker_color=automatiserings_colors[2], name="Manuell"), secondary_y=False)
    
    fig.update_layout(yaxis_tickformat = '.0%', barmode='stack')
    fig.update_yaxes(title_text="Andel", secondary_y=False)
    fig.update_yaxes(title_text="Antall saker", range=[0,max(df_tot["Antall innkomne saker"]*1.1)], secondary_y=True)
    
    return fig


def hide_secondary_axis(ax):
    if ax.side=="right": 
        ax.showgrid=False


def plot_automatisering_subplots(df_list, titles, box_color=px.colors.qualitative.Pastel[0]):
    '''
    df: en verdi per måned og automatiseringsgrad
    box_color: Farge på boxplot
    fig: Box for antall saker, line for automatiseringsgrad
    '''
    N = len(df_list)
    M = len(df_list[0])
    fig = make_subplots(rows=N, cols=M, specs=[[{"secondary_y": True} for i in range(N*M)]], subplot_titles=titles)
    showlegend = True
    for i in range(N):
        for j in range(M):
            if i+j > 0:
                showlegend = False
            df = df_list[i][j]
            df_tot = df.groupby("År-måned", as_index=False)["Antall innkomne saker"].sum()

            _automatisk = df["Automatiseringsgrad"] == "Automatisk"
            _manuell = df["Automatiseringsgrad"] == "Manuell"
    
            fig.add_trace(go.Bar(x=df_tot["År-måned"], y=df_tot["Antall innkomne saker"], 
                                 name="Antall saker", legendgroup="Antall saker", showlegend=showlegend, marker_color=box_color),
                          row=i+1, col=j+1, secondary_y=False)

            fig.add_trace(go.Scatter(x=df[_automatisk]["År-måned"], y=df[_automatisk]["Andel"], 
                                     name="Automatisk", legendgroup="Automatisk", showlegend=showlegend, mode="lines+markers",
                                     line={'dash': 'solid', 'color': 'black'}),
                          row=i+1, col=j+1, secondary_y=True)
            
            fig.add_trace(go.Scatter(x=df[~_automatisk & ~_manuell]["År-måned"], y=df[~_automatisk & ~_manuell]["Andel"], 
                                     name="Del-automatisk", legendgroup="Del-automatisk", showlegend=showlegend, mode="lines",
                                     line={'dash': 'solid', 'color': 'grey'}), 
                          row=i+1, col=j+1, secondary_y=True)
    
            fig.update_yaxes(title_text="Andel automatisert", secondary_y=True, range=[0,1], tickformat='.0%', row=i+1, col=j+1)
            fig.update_yaxes(title_text="Antall saker", range=[0,max(df_tot["Antall innkomne saker"])*1.1], secondary_y=False, row=i+1, col=j+1)
    
    #fig.layout.yaxis2.showgrid = False
    #fig.update_layout(yaxis2.showgrid = False)
    #fig.for_each_yaxis(lambda x: x.update(showgrid=False))
    fig.for_each_yaxis(lambda ax: hide_secondary_axis(ax))
    fig.update_layout(
        autosize=False,
        width=1300,
        height=500,)
    return fig


def plot_selvbetjening1(df, box_color=px.colors.qualitative.Pastel[2]):

    df_tot = df.groupby("År-måned", as_index=False)["Antall innkomne saker"].sum()
    
    _selvbetjent = df["Selvbetjening"] == "Selvbetjent"
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(go.Bar(x=df_tot["År-måned"], y=df_tot["Antall innkomne saker"], 
                         name="Antall saker", marker_color=box_color), 
                  secondary_y=False)
    
    fig.add_trace(go.Scatter(x=df[_selvbetjent]["År-måned"], y=df[_selvbetjent]["Andel"], 
                             name="Selvbetjening", mode="lines+markers",
                             line={'dash': 'solid', 'color': 'black'}),
                  
                  secondary_y=True)
    
    fig.update_yaxes(title_text="Andel selvbetjening", secondary_y=True, range=[0,1], tickformat='.0%')
    fig.update_yaxes(title_text="Antall saker", range=[0,max(df_tot["Antall innkomne saker"])*1.1], secondary_y=False)
    fig.layout.yaxis2.showgrid = False
    fig.update_layout(
        autosize=False,
        width=1300,
        height=500,)    
    return fig


def plot_selvbetjening2(df):
    
    df_tot = df.groupby("År-måned", as_index=False)["Antall innkomne saker"].sum()
    
    _selvbetjent = df["Selvbetjening"] == "Selvbetjent"
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(go.Scatter(x=df_tot["År-måned"], y=df_tot["Antall innkomne saker"], mode="lines", marker_color="rgb(0,0,0)", name="Totalt antall saker"), secondary_y=True)
    
    fig.add_trace(go.Bar(x=df[_selvbetjent]["År-måned"], y=df[_selvbetjent]["Andel"], marker_color=automatiserings_colors[0], name="Selvbetjent"), secondary_y=False)
    fig.add_trace(go.Bar(x=df[~_selvbetjent]["År-måned"], y=df[~_selvbetjent]["Andel"], marker_color=automatiserings_colors[2], name="Ikke selvbetjent"), secondary_y=False)
    
    fig.update_layout(yaxis_tickformat = '.0%', barmode='stack')
    fig.update_yaxes(title_text="Andel selvbetjening", secondary_y=False)
    fig.update_yaxes(title_text="Antall saker", range=[0,max(df_tot["Antall innkomne saker"])*1.1], secondary_y=True)
    
    return fig


def plot_selvbetjening_subplots(df_list, titles, box_color=px.colors.qualitative.Pastel[2]):
    '''
    '''
    N = len(df_list)
    M = len(df_list[0])
    fig = make_subplots(rows=N, cols=M, specs=[[{"secondary_y": True} for i in range(N*M)]], subplot_titles=titles)
    showlegend = True
    for i in range(N):
        for j in range(M):
            if i+j > 0:
                showlegend = False
            df = df_list[i][j]
            df_tot = df.groupby("År-måned", as_index=False)["Antall innkomne saker"].sum()

            _selvbetjent = df["Selvbetjening"] == "Selvbetjent"
            
            fig.add_trace(go.Bar(x=df_tot["År-måned"], y=df_tot["Antall innkomne saker"], 
                         name="Antall saker", legendgroup="Antall saker", showlegend=showlegend, marker_color=box_color), 
                  row=i+1, col=j+1, secondary_y=False)
    
            fig.add_trace(go.Scatter(x=df[_selvbetjent]["År-måned"], y=df[_selvbetjent]["Andel"], 
                             name="Selvbetjening", legendgroup="Selvbetjening", showlegend=showlegend, mode="lines+markers",
                             line={'dash': 'solid', 'color': 'black'}),
                  row=i+1, col=j+1, secondary_y=True)
            
            fig.update_yaxes(title_text="Andel selvbetjening", secondary_y=True, range=[0,1], tickformat='.0%', row=i+1, col=j+1)
            fig.update_yaxes(title_text="Antall saker", range=[0,max(df_tot["Antall innkomne saker"])*1.1], secondary_y=False, row=i+1, col=j+1)
    
    #fig.layout.yaxis2.showgrid = False
    fig.for_each_yaxis(lambda ax: hide_secondary_axis(ax))
    fig.update_layout(
        autosize=False,
        width=1300,
        height=500,)
    return fig
