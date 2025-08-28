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


# build data set to populate draft board
draftBoard = vbd.vbdDraftBoard(
        pick_cutoff = 120,
        season_cutoff = 2021,
        n_teams = 12,
        needed_starters = {'WR': 2, 'RB': 2, 'QB': 1, 'TE': 1, 'FLEX': 1}
    )
draftBoard.get_replacement_player_score()
draftBoard.get_stat_aggregations()
df_allstats_agg = draftBoard.df_allstats_agg.copy()


app = Dash(__name__)

app.layout = html.Div([
    html.H2('Interactive Table Dashboard'),

    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df_allstats_agg.columns],
        data=df_allstats_agg.to_dict('records'),
        filter_action='native',     # enables filtering
        sort_action='native',       # enables sorting
        #page_action='native',       # enables pagination
        page_action='none',   # show all rows instead of pagination
        #style_table={'overflowX': 'auto'},
        style_table={'height': '80vh', 'overflowY': 'auto'},  # scrollable
        #page_size=5,
        row_deletable=True,         # allows deleting rows
        style_data_conditional=[
            {'if': {'row_index': 'even'}, 'backgroundColor': '#e0e0e0'},
            {'if': {'row_index': 'odd'}, 'backgroundColor': 'white'}
        ]
    ),

    html.Br(),
    html.Button('Undo Delete', id='undo-btn', n_clicks=0),
    html.Button('Reset Table', id='reset-btn', n_clicks=0),

    # store for deleted rows
    dcc.Store(id='deleted-store', data=[])
])

# Callback to capture deleted rows
@app.callback(
    Output('deleted-store', 'data'),
    Input('table', 'data_previous'),
    State('table', 'data'),
    State('deleted-store', 'data'),
    prevent_initial_call=True
)
def store_deleted(previous, current, deleted_store):
    if previous is None:
        raise dash.exceptions.PreventUpdate
    # find deleted rows
    deleted_rows = [row for row in previous if row not in current]
    return deleted_store + deleted_rows

# Callback to handle undo + reset
@app.callback(
    Output('table', 'data'),
    Input('undo-btn', 'n_clicks'),
    Input('reset-btn', 'n_clicks'),
    State('table', 'data'),
    State('deleted-store', 'data')
)
def undo_or_reset(undo_clicks, reset_clicks, current_data, deleted_store):
    ctx = dash.callback_context

    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'reset-btn':  # reset table
        return df_allstats_agg.to_dict('records')

    if button_id == 'undo-btn' and deleted_store:  # restore last deleted
        last_deleted = deleted_store[-1]  # get most recent
        return current_data + [last_deleted]

    return current_data

if __name__ == '__main__':
    app.run(debug=True)