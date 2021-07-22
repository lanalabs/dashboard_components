import plotly.graph_objects as go
import pandas as pd


def day_of_week_chart(df: pd.DataFrame) -> go.Figure:
    """Receives the output of 'cases_per_weekday' to plot a barchart
       that compares on which weekday processes stard and end."""

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df.byDayOfWeek,
        y=df.casesStarted,
        name='Case Start'
    ))
    fig.add_trace(go.Bar(
        x=df.byDayOfWeek,
        y=df.casesEnded,
        name='Case Ende'
    ))

    fig.update_layout(
        title='Verteilung von Case Start und Ende auf Wochentage',
        xaxis_tickfont_size=14,
        legend=dict(yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.86
                    ),
        barmode='group',
        bargap=0.2,  # gap between bars of adjacent location coordinates.
        bargroupgap=0.05  # gap between bars of the same location coordinate.
    )

    return fig
