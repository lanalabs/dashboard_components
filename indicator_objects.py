import plotly.graph_objects as go

import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from functools import lru_cache


remove_bar_config = {
    'displayModeBar': False,
    'displaylogo': False
}


def get_indicator_graph(value, mode="number", title=None, height=250):
    temp_fig = go.Figure(go.Indicator(
        mode=mode,
        value=value,
        title=title,
        domain={'x': [0, 1], 'y': [0, 1]}))
    temp_fig.update_layout(paper_bgcolor="lavender", height=height)
    return temp_fig


@lru_cache(maxsize=5)
def indicator_div(value, mode="number", height=250, title=None,
                  width=3, id="indicator_graph"):
    fig = get_indicator_graph(value, mode, title=title, height=height)

    return html.Div(children=[dcc.Graph(
        figure=fig,
        config=remove_bar_config,
    )], id=id, n_clicks=0)


@lru_cache(maxsize=5)
def indicator_col(value, mode="number", height=250, title=None,
                  width=3, id="indicator_graph"):
    div = indicator_div(value, mode, height, title, width, id=id)
    return dbc.Col(
        div,
        width={"size": width}
    )
