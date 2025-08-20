import re, json, traceback
import pandas as pd
from nfl_data_py import import_ids
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from sqlalchemy import create_engine, text
from config import ESPN_COOKIES_SWID, ESPN_COOKIES_S2, PYMYSQL_NFL, BROWSER_DIR, ESPN_LEAGUE_ID, ESPN_HEADERS_NAME, ESPN_HEADERS, DATA_DIR


folderpath_data = DATA_DIR
with open(str(folderpath_data) + '/json/mapping_dicts.json', 'r', encoding='utf-8') as f:
    lookups = json.load(f)

# =========================
# ESPN FANTASY LEAGUE DATA
# =========================
league_id = ESPN_LEAGUE_ID
league_cookies = {
    "SWID":ESPN_COOKIES_SWID,
    "espn_s2":ESPN_COOKIES_S2
}
league_headers = {
    ESPN_HEADERS_NAME: ESPN_HEADERS
}
map_team_ids_to_name =  lookups['map_team_ids_to_name']
lineupSlotID = lookups['lineupSlotID']
espn_urls = lookups['espn_urls']

# ======================================
# columns for scraped raw data
projection_columns = [
    "outlet","date", "season", "week", "playerId", "name", "shortName", "pos", "team", 'GamesPlayed',
    'PassAttempts','PassCompletions','PassingYards', 'PassingYardsPerGame', 'TouchdownsPasses', 
    'InterceptionsThrown', 'PasserRating',
    'RushingAttempts','RushingYards', 'AverageYardsPerRush', 'RushingTouchdowns',
    'Targets', 'Receptions', 'ReceivingYards', 'YardsPerGame', 'AverageYardsPerReception','ReceivingTouchdowns',
    'FumblesLost',
    'FieldGoalsMade','FieldGoalAttempts','LongestFieldGoal','FieldGoals119Yards','FieldGoals119YardAttempts',
    'FieldGoals2029Yards','FieldGoals2029YardAttempts','FieldGoals3039Yards','FieldGoals3039YardAttempts',
    'FieldGoals4049Yards','FieldGoals4049YardAttempts','FieldGoals50Yards','FieldGoals50YardsAttempts',
    'ExtraPointsMade','ExtraPointsAttempted',
    'Interceptions','Safeties','Sacks','Tackles','DefensiveFumblesRecovered','ForcedFumbles','DefensiveTouchdowns', 
    'ReturnTouchdowns','PointsAllowed','PointsAllowedPerGame','NetPassingYardsAllowed','RushingYardsAllowed', 
    'TotalYardsAllowed', 'YardsAgainstPerGame', 'twoPt','FantasyPoints','FantasyPointsPerGame'
]

projection_filter_cols = [
    'playerId',
    'date',
    'season',
    'week',
    'outlet',
    'GamesPlayed',
    'PassAttempts',
    'PassCompletions',
    'PassingYards',
    'PassingYardsPerGame',
    'TouchdownsPasses',
    'InterceptionsThrown',
    'PasserRating',
    'RushingAttempts',
    'RushingYards',
    'AverageYardsPerRush',
    'RushingTouchdowns',
    'Targets',
    'Receptions',
    'ReceivingYards',
    'YardsPerGame',
    'AverageYardsPerReception',
    'ReceivingTouchdowns',
    'FumblesLost',
    'FieldGoalsMade',
    'FieldGoalAttempts',
    'LongestFieldGoal',
    'FieldGoals119Yards',
    'FieldGoals119YardAttempts',
    'FieldGoals2029Yards',
    'FieldGoals2029YardAttempts',
    'FieldGoals3039Yards',
    'FieldGoals3039YardAttempts',
    'FieldGoals4049Yards',
    'FieldGoals4049YardAttempts',
    'FieldGoals50Yards',
    'FieldGoals50YardsAttempts',
    'ExtraPointsMade',
    'ExtraPointsAttempted',
    'Interceptions',
    'Safeties',
    'Sacks',
    'Tackles',
    'DefensiveFumblesRecovered',
    'ForcedFumbles',
    'DefensiveTouchdowns',
    'ReturnTouchdowns',
    'PointsAllowed',
    'PointsAllowedPerGame',
    'NetPassingYardsAllowed',
    'RushingYardsAllowed',
    'TotalYardsAllowed',
    'YardsAgainstPerGame',
    'twoPt',
    'FantasyPoints',
    'FantasyPointsPerGame'
]

ranking_columns = [
    "outlet", "date", "season", "week", "group", "expert", "rank","name","playerId","team","pos", "high", "low"
]

adp_columns = [
    'outlet', 'date', 'playerId', 'name', 'shortName' , 'pos', 'team', 'adp', 'high', 'low'
]

# ======================================
# columns for processed data loading into db
bettingCols = [
    'date', 'season', 'week', 'overUnder', 'overUnderCost', 'awayTeamId',
    'awaySpread', 'awayCost', 'awayMoneyline', 'homeTeamId', 'homeSpread',
    'homeCost', 'homeMoneyLine'
]

rankingCols = [
    'outletId','date','season','week','rankGroup','analystId','ranking','high','low','playerId'
 ]

projectionCols = [
    'playerId', 'date', 'season', 'week', 'outletId', 'gp', 'att', 'comp', 'passYd', 'passYdPg', 
    'passTd', 'pInt', 'passRtg', 'rush', 'rushYd', 'ydPerRush', 'rushTd', 'target', 'rec', 'recYd', 
    'recYdPg', 'ydPerRec', 'recTd', 'fmb', 'fgM', 'fgA', 'fgLong', 'fgM0119', 'fgA0119', 'fgM2029', 
    'fgA2029', 'fgM3039', 'fgA3039', 'fgM4049', 'fgA4049', 'fgM5099', 'fgA5099', 'xpM', 'xpA', 
    'defInt', 'sfty', 'sack', 'tckl', 'defFmbRec', 'defFmbFor', 'defTd', 'retTd', 'ptsAllowed', 
    'ptsAllowedPg', 'pYdAllowedPg', 'rYdAllowedPg', 'totalYdAllowed', 'totalYdAllowedPg', 'twoPt', 
    'fantasyPoints', 'fantasyPointsPg'
]

map_projInput_to_projOut = lookups['map_projInput_to_projOut']

adpCols = [
    'outletId', 'date', 'playerId', 'adp', 'high', 
]
# ======================================
# team naming maps
team_map = lookups['team_map']
team_mascot_map = lookups['team_mascot_map']
team_map_abbrevs = lookups['team_map_abbrevs']
team_map_ids = lookups['team_map_ids']
team_map_nfldatapy_to_db = lookups['team_map_nfldatapy_to_db']
team_map_nfldatapy_to_dbTid = lookups['team_map_nfldatapy_to_dbTid']

del lookups
# ====================
# general helper funcs
# ====================
def refresh_id_table():
    '''
    this pulls the most recent ID lookup table from nfl-data-py and overwrites my db table
    '''
    id_table = import_ids()
    export_database(
        dataframe = id_table, 
        database_table = 'playeridlookupimport', 
        connection_string=PYMYSQL_NFL, 
        if_exists='replace'
    )

    return

def export_database(dataframe, database_table, connection_string=None, if_exists='append'):

    try:
        if connection_string == None:
            dataframe.to_sql(
                name=database_table, 
                con=PYMYSQL_NFL, 
                if_exists=if_exists, 
                index=False
            )

        else:
            dataframe.to_sql(
                name=database_table, 
                con=connection_string, 
                if_exists=if_exists, 
                index=False
            )
        
        print('successfully added data to', database_table)
        return 
        
    except Exception as ex:
        message = 'database load failed to ' + database_table
        print(message)
        print(ex)
        return 

def query_database(query, connection_string=None, params=None):
    
    try:
        if connection_string == None:
            df = pd.read_sql_query(
                sql = query,
                con=PYMYSQL_NFL,
                params=params
            )

        else:
            df = pd.read_sql_query(
                sql = query,
                con=connection_string,
                params=params
            )
        
        print('query successful')
        return df
        
    except:
        message = 'query failed'
        print(message)
        return 

def open_browser(browser_filepath = None, retry_delay = 5, retry_attempts = 3):
    
    # an override browswer path can be provided but normally use the one provided whe nthe class is created 
    if browser_filepath is None:
        browser_filepath = BROWSER_DIR / "geckodriver.exe"
    
    service = Service(browser_filepath)
    driver = webdriver.Firefox(service=service)

    # start browser
    return driver

def apply_regex_replacements(value):
    """
    used to format names into their most joinable form
    """
    # regex replacement mapping used to make more joinable names
    suffix_replace = {
        "\\.":"", "`":"", "'":"",
        " III$":"", " IV$":"", " II$":"", " iii$":"", " ii$":"", " iv$":"", 
        " v$":"", " V$":"", " jr$":"", " sr$":"", " jr.$":"", " sr.$":"", 
        " Jr$":"", " Sr$":"", " Jr.$":"", " Sr.$":"", " JR$":"", " SR$":"", 
        " JR.$":"", " SR.$":""
    }
    for pattern, replacement in suffix_replace.items():
        value = re.sub(pattern, replacement, value, flags=re.IGNORECASE)
    return value

def add_new_players_to_db(df_missing_players):

    # missing players will be one of two ways 
    # A) not in my db at all - INSERTS
    # B) in the db but not populated for the outlet yet - UPDATES

    #refreshes the id lookup table from nfl-data-py
    #this will be used to populate player position and team id 
    #for players being added to my player database 
    #refresh_id_table()

    # get the list of players from my database so i can check if the missing players are missing
    # from my database or just missing the outlet id
    dfplayer = query_database(
        query="SELECT * FROM player"
    )
    # formatting imported data and prepping maps and list
    dfplayer['joinName']  = dfplayer['name'].str.lower().apply(apply_regex_replacements)
    map_joinName_to_dbPid = pd.Series(dfplayer.playerId.values, index=dfplayer.joinName).to_dict()
    players_in_db = list(dfplayer['joinName'])#.str.lower().apply(apply_regex_replacements))

    map_source = {
        'espn':'espnId', 
        'fantasyPros':'fpId', 
        'nfl':'nflId',
        'cbs':'cbsId'
    }

    # prepping the missing player data for updates or inserts into my db
    cols_keep = [
        'outlet', 'sourceId', 'name'
    ]
    df_missing_players = df_missing_players[cols_keep]
    df_missing_players['joinName'] = df_missing_players['name'].str.lower().apply(apply_regex_replacements)
    # drop rows where sourceId contains alphabetic characters then force to int to match db type
    mask = df_missing_players['sourceId'].astype(str).str.contains(r'[A-Za-z]', na=False)
    df_missing_players = df_missing_players[~mask]
    df_missing_players['sourceId'] = df_missing_players['sourceId'].astype(float).astype(int)
    # will be used to loop thru db updates
    missing_outlets = df_missing_players['outlet'].unique()

    # ====================
    #       INSERTS
    # ====================
    # get players that are not present in my databased and prep for an insert
    try:
        inserts = df_missing_players[
            (~df_missing_players['joinName'].isin(players_in_db))
        ]
        
        inserts.loc[:,'outlet'] = inserts['outlet'].replace('fantasyPros', 'fp')
        inserts = inserts.drop_duplicates(subset=['name', 'outlet'])

        # making single records f   or each player with all the ids
        inserts = (
            inserts.pivot(index='joinName', columns='outlet', values='sourceId')
            .add_suffix('Id')        # add "Id" to each outlet column
            .reset_index()           # bring 'name' back as a column
        )
        inserts = inserts.copy()
        # adding joinName back so that it can me used to add posId and teamId
        #inserts['joinName'] = inserts['name'].str.lower().apply(apply_regex_replacements)

        # retrieve position ID and team ID for the players being added
        id_lookups = query_database(
            query = '''
                SELECT 
                    pid.name as name, pos.posId as posId, team as teamId
                FROM
                    playeridlookupimport pid
                LEFT JOIN pos ON pos.pos = pid.position
            '''
        )

        id_lookups['teamId'] = id_lookups['teamId'].map(team_map_nfldatapy_to_dbTid)
        id_lookups['joinName'] = id_lookups['name'].str.lower().apply(apply_regex_replacements)
        id_lookups.drop('name', axis=1, inplace=True)

        # merge posId and teamId to inserts
        inserts = inserts.merge(id_lookups, on='joinName', how = 'left')

        # python or pandas is converting some to float so making sure the ids are ints
        cols_to_int = [i for i in inserts.columns if 'id' in i.lower()]
        # drop rows where ANY of those columns contain alphabetic characters
        inserts[cols_to_int] = (
            inserts[cols_to_int]
            .apply(pd.to_numeric, errors='coerce')  # non-numeric -> NaN
            .astype('Int64')                        # nullable int dtype, keeps NaN
        )

        if inserts.shape[0] == 0: pass
        else:
            inserts = inserts.rename(columns={'joinName':'name'})
            export_database(
                dataframe = inserts,
                database_table = 'player',
                connection_string = None,
                if_exists='append'
            )
            print('inserts completed..')

    except Exception as ex:
        print(inserts.head())
        print("Exception type:", type(ex).__name__)
        print("Exception message:", ex)
        traceback.print_exc()

    # ====================
    #       UPDATES
    # ====================    
    try:
        # loop through the missing players one outlet at a time to build the dataset for updating db
        for i in missing_outlets:
            # look up the column name to use based on the outlet 
            id_col_name = map_source[i]

            # get players that are present in my db but missing a pid for an outlet
            updates = (
                df_missing_players[
                    (df_missing_players['outlet'] == i) &
                    (df_missing_players['joinName'].isin(players_in_db))
                ]
                #.loc[:, ~df_edit.columns.isin(['posId', 'teamId', 'joinName'])]
            )
            if i == 'fantasyPros':
                updates.loc[:,'outlet'] = updates['outlet'].replace('fantasyPros', 'fp')

            updates = updates.copy()
            updates.loc[:,'playerId'] = updates['joinName'].map(map_joinName_to_dbPid).astype(int)

            cols_to_update = [
                'playerId', 'sourceId', 'name'
            ]
            updates = updates[cols_to_update].rename(
                columns={
                    'name':id_col_name.replace('Id', 'Name'),
                    'sourceId':id_col_name
                })

            # skip the db call if the dataframe wa reduced to zero
            if updates.shape[0] == 0: continue
            else:
                query = text(
                    f"UPDATE player SET {id_col_name} = :{id_col_name}, {id_col_name.replace('Id', 'Name')} = :{id_col_name.replace('Id', 'Name')} WHERE playerId = :playerId"
                )
                engine = create_engine(PYMYSQL_NFL)
                with engine.begin() as conn:  # handles commit/rollback automatically
                    conn.execute(
                        query,
                        updates.to_dict(orient='records')
                    )
        print('updates completed...')

    except Exception as ex:
        print('failed on', i, '...')
        print(updates.head())
        print("Exception type:", type(ex).__name__)
        print("Exception message:", ex)
        traceback.print_exc()
    return




