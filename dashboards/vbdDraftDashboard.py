##### importing custom modules from the projects folder
import sys, os
from pathlib import Path
# Add project root to sys.path - search backwards through folders to find config.py
cwd = Path.cwd()
# Search upwards until a "config*" file is found
for parent in [cwd, *cwd.parents]:
    match = next(parent.glob('config*'), None)
    if match:
        PROJECT_ROOT = match.parent
        break
sys.path.append(str(PROJECT_ROOT))
import modules.vbdDraftBoardBuilder as vbd
# ====================================================
import dash
from dash import Dash, dcc, html, Input, Output, State, dash_table
import pandas as pd
import numpy as np
import plotly.graph_objects as go


# build data set to populate draft board
draftBoard = vbd.vbdDraftBoard(
        pick_cutoff = 120,
        season_cutoff = 2021,
        n_teams = 12,
        needed_starters = {'WR': 2, 'RB': 2, 'QB': 1, 'TE': 1, 'FLEX': 1}
    )
draftBoard.get_replacement_player_score()
draftBoard.get_stat_aggregations()
df = draftBoard.df_allstats_agg.copy()

window = 22
n = len(df)

# Convert positions to categorical codes for efficient comparison
codes = pd.Categorical(df['pos']).codes

# Create a 2D array: rows x window
# Each row will look ahead up to 22 positions
idx = np.arange(n)[:, None] + np.arange(1, window+1)  # shape: n x window
idx[idx >= n] = n-1  # clip to last index

# Compare positions
lookahead = codes[idx] == codes[:, None]  # shape: n x window
df['availnext22'] = lookahead.sum(axis=1)

# move the col 
cols = list(df.columns)
cols.insert(5, cols.pop(cols.index('availnext22')))  # move to index 5
df = df[cols]

# Compute static x-axis range at startup
x_min_global = 0
x_max_global = df['VBD_historical'].max()
x_range_global = np.linspace(x_min_global, x_max_global, 200)

app = Dash(__name__)

app.layout = html.Div([
    html.H2("KTB VBD DRAFTBOARD"),

    dash_table.DataTable(
        id='table',
        data=df.to_dict('records'),
        columns=[{'name': c, 'id': c} for c in df.columns],
        filter_action='native',
        sort_action='native',
        row_deletable=True,
        style_table={'height': '400px', 'overflowY': 'auto', 'width': '100%'},
        style_data={'whiteSpace': 'normal', 'height': 'auto'},
        style_cell={'textAlign': 'left', 'padding': '5px'},
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(0, 0, 0, 0.45)'}
        ],
        page_size=20
    ),
 
    html.Div([
        html.Button("Undo Delete", id="undo-btn", n_clicks=0),
        html.Button("Reset Table", id="reset-btn", n_clicks=0)
    ], style={'marginTop': '10px', 'marginBottom': '10px'}),

    dcc.Graph(id='density-plot')
])

deleted_stack = []

def recalc_pos_count(table_data, window=22):
    """Vectorized calculation of pos_count_next_22"""
    dff = pd.DataFrame(table_data)
    n = len(dff)
    if n == 0:
        return table_data
    codes = pd.Categorical(dff['pos']).codes
    idx = np.arange(n)[:, None] + np.arange(1, window+1)
    idx[idx >= n] = n-1
    lookahead = codes[idx] == codes[:, None]
    dff['availnext22'] = lookahead.sum(axis=1)

    # Move availnext22 to 6th column if exists
    if 'availnext22' in dff.columns:
        cols = list(dff.columns)
        cols.insert(5, cols.pop(cols.index('availnext22')))
        dff = dff[cols]

    return dff.to_dict('records')


@app.callback(
    Output('table', 'data'),
    Input('undo-btn', 'n_clicks'),
    Input('reset-btn', 'n_clicks'),
    Input('table', 'data'),
    State('table', 'data_previous'),
    prevent_initial_call=False
)
def update_table(undo_clicks, reset_clicks, table_data, previous_data):
    """Handles undo, reset, and pos_count_next_22 recalculation"""
    global deleted_stack
    ctx = dash.callback_context

    # Determine which input triggered the callback
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    if trigger_id == 'reset-btn':
        # Reset to original dataframe
        deleted_stack = []
        current_data = df.to_dict('records')
    else:
        current_data = table_data.copy() if table_data else []

        # Track deleted rows
        if previous_data and len(previous_data) > len(current_data):
            deleted_rows = [row for row in previous_data if row not in current_data]
            deleted_stack.extend(deleted_rows)

        # Undo delete
        if trigger_id == 'undo-btn' and deleted_stack:
            current_data.append(deleted_stack.pop())

    # Recalculate availnext22
    return recalc_pos_count(current_data)


@app.callback(
    Output('density-plot', 'figure'),
    Input('table', 'data')
)
def update_density(table_data):
    dff = pd.DataFrame(table_data)
    if dff.empty:
        return go.Figure()

    # Filter like R example
    dff = dff[(dff['VBD_historical'] >= 0) & (dff['pos'].isin(['QB','RB','WR','TE']))][['pos','VBD_historical']]
    if dff.empty:
        return go.Figure()

    fig = go.Figure()

    for pos in dff['pos'].unique():
        subset = dff[dff['pos']==pos]['VBD_historical'].values
        if len(subset) > 1:
            # Histogram density
            hist, bin_edges = np.histogram(subset, bins=30, range=(x_min_global, x_max_global), density=True)
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
            smooth = np.convolve(hist, np.ones(3)/3, mode='same')
            fig.add_trace(go.Scatter(
                x=bin_centers,
                y=smooth,
                mode='lines',
                fill='tonexty',
                opacity=0.3,
                name=pos
            ))

    fig.update_layout(
        title="Density Plot of Projected VBD",
        xaxis_title="Player's Value Over Replacement",
        yaxis_title="Density",
        legend_title_text='',
        xaxis=dict(range=[x_min_global, x_max_global])
    )
    return fig


if __name__ == "__main__":
    #app.run(debug=True)
    app.run()