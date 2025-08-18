import re
import pandas as pd
from nfl_data_py import import_ids
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from sqlalchemy import create_engine, text
from config import ESPN_COOKIES_SWID, ESPN_COOKIES_S2, PYMYSQL_NFL, BROWSER_DIR, ESPN_LEAGUE_ID, ESPN_HEADERS_NAME, ESPN_HEADERS, DATA_DIR


folderpath_data = DATA_DIR

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
map_team_ids_to_name =  {
    1: 'John', 2: 'Gomer', 3: 'Pope', 4: "Jamie", 
    5: "Geik", 6: "Bryan", 7: "Chaunce", 8: "Sam", 
    9: "Chris", 10: "Murphy", 11: "Colin", 12: 'Ethan'
}
lineupSlotID = {
    17: 'K', 0: 'QB', 20: 'bench', 15: 'DP', 6: 'TE', 
    23: 'FLEX', 4: 'WR', 2: 'RB', 21: 'IR'
}
espn_urls = {
    'league_history':'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/{}?view=mLiveScoring&view=mMatchupScore&view=mRoster&view=mSettings&view=mStandings&view=mStatus&view=mTeam&view=modular&view=mNav',
    'player_info':'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/{}?scoringPeriodId=19&view=kona_player_info',
    'draft_results':'https://fantasy.espn.com/football/league/draftrecap?leagueId={lid}&seasonId={sid}',
    'base_player_url':'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=kona_player_info&scoringPeriodId={}',
    'base_league_url':'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=mLiveScoring&view=mMatchupScore&view=mPositionalRatings&view=mTeam&view=modular&view=mNav&view=mMatchupScore&scoringPeriodId={}',
    'base_boxscore_url':'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=mBoxscore&scoringPeriodId={}'
}

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
    'outlet','date','season','week','group','expert','rank','high','low','playerId'
 ]

projectionCols = [
    "playerId", "date", "season", "week", "outlet",'GamesPlayed',
    'PassAttempts','PassCompletions','PassingYards', 'PassingYardsPerGame', 'TouchdownsPasses', 
    'InterceptionsThrown', 'PasserRating','RushingAttempts','RushingYards', 'AverageYardsPerRush', 'RushingTouchdowns',
    'Targets', 'Receptions', 'ReceivingYards', 'YardsPerGame', 'AverageYardsPerReception','ReceivingTouchdowns',
    'FumblesLost','FieldGoalsMade','FieldGoalAttempts','LongestFieldGoal','FieldGoals119Yards','FieldGoals119YardAttempts',
    'FieldGoals2029Yards','FieldGoals2029YardAttempts','FieldGoals3039Yards','FieldGoals3039YardAttempts',
    'FieldGoals4049Yards','FieldGoals4049YardAttempts','FieldGoals50Yards','FieldGoals50YardsAttempts',
    'ExtraPointsMade','ExtraPointsAttempted','Interceptions','Safeties','Sacks','Tackles','DefensiveFumblesRecovered',
    'ForcedFumbles','DefensiveTouchdowns', 'ReturnTouchdowns','PointsAllowed','PointsAllowedPerGame','NetPassingYardsAllowed',
    'RushingYardsAllowed','TotalYardsAllowed', 'YardsAgainstPerGame', 'twoPt', 'FantasyPoints','FantasyPointsPerGame'
]

# ======================================
# team naming maps
team_map = {
    "Jacksonville Jaguars":"JAX","Los Angeles Rams":"LA","Philadelphia Eagles":"PHI","Minnesota Vikings":"MIN",
    "Houston Texans":"HOU","Los Angeles Chargers":"LAC","Baltimore Ravens":"BAL","New England Patriots":"NE",
    "Carolina Panthers":"CAR","Denver Broncos":"DEN","Arizona Cardinals":"ARI","New Orleans Saints":"NO",
    "Detroit Lions":"DET","Pittsburgh Steelers":"PIT","Chicago Bears":"CHI","Seattle Seahawks":"SEA",
    "Buffalo Bills":"BUF","Tennessee Titans":"TEN","Atlanta Falcons":"ATL","Cincinnati Bengals":"CIN",
    "Kansas City Chiefs":"KC","Washington Redskins":"WAS","Dallas Cowboys":"DAL","Tampa Bay Buccaneers":"TB",
    "Green Bay Packers":"GB","New York Giants":"NYG","San Francisco 49ers":"SF","Cleveland Browns":"CLE",
    "Oakland Raiders":"OAK","Indianapolis Colts":"IND","Miami Dolphins":"MIA","New York Jets":"NYJ"
}
team_mascot_map = {
    "Jaguars":"JAX","Rams":"LA","Eagles":"PHI","Vikings":"MIN",
    "Texans":"HOU","Chargers":"LAC","Ravens":"BAL","Patriots":"NE",
    "Panthers":"CAR","Broncos":"DEN","Cardinals":"ARI","Saints":"NO",
    "Lions":"DET","Steelers":"PIT","Bears":"CHI","Seahawks":"SEA",
    "Bills":"BUF","Titans":"TEN","Falcons":"ATL","Bengals":"CIN",
    "Chiefs":"KC","Redskins":"WAS","Cowboys":"DAL","Buccaneers":"TB",
    "Packers":"GB","Giants":"NYG","49ers":"SF","Browns":"CLE",
    "Raiders":"OAK","Colts":"IND","Dolphins":"MIA","Jets":"NYJ"
}
team_map_abbrevs = {
    'ATL': 'ATL',
    'BUF': 'BUF',
    'CHI': 'CHI',
    'CIN': 'CIN',
    'CLE': 'CLE',
    'DAL': 'DAL',
    'DEN': 'DEN',
    'DET': 'DET',
    'GB': 'GB',
    'TEN': 'TEN',
    'IND': 'IND',
    'KC': 'KC',
    'LV': 'LV',
    'LAR': 'LA',
    'MIA': 'MIA',
    'MIN': 'MIN',
    'NE': 'NE',
    'NO': 'NO',
    'NYG': 'NYG',
    'NYJ': 'NYJ',
    'PHI': 'PHI',
    'ARI': 'ARI',
    'PIT': 'PIT',
    'LAC': 'LAC',
    'SF': 'SF',
    'SEA': 'SEA',
    'TB': 'TB',
    'WSH': 'WAS',
    'CAR': 'CAR',
    'JAX': 'JAX',
    'BAL': 'BAL',
    'HOU': 'HOU',
    'FA': 'FA',
    'STL': 'SL',
    'SD': 'SD',
    'OAK': 'OAK'
 }
team_map_ids = {
    'ATL': '3800',
    'BUF': '0610',
    'CHI': '0810',
    'CIN': '0920',
    'CLE': '1050',
    'DAL': '1200',
    'DEN': '1400',
    'DET': '1540',
    'GB': '1800',
    'TEN': '2100',
    'IND': '2200',
    'KC': '2310',
    'LV': '2520',
    'LAR': '2510',
    'MIA': '2700',
    'MIN': '3000',
    'NE': '3200',
    'NO': '3300',
    'NYG': '3410',
    'NYJ': '3430',
    'PHI': '3700',
    'ARI': '3800',
    'PIT': '3900',
    'LAC': '4400',
    'SF': '4500',
    'SEA': '4600',
    'TB': '4900',
    'WSH': '5110',
    'CAR': '0750',
    'JAX': '2250',
    'BAL': '0325',
    'HOU': '2120',
    'FA': '0',
    'STL': '0',
    'SD': '0',
    'OAK': '0'
 }
team_map_nfldatapy_to_db = {
    'ARI':'ARI',
    'ATL':'ATL',
    'BAL':'BAL',
    'BUF':'BUF',
    'CAR':'CAR',
    'CHI':'CHI',
    'CIN':'CIN',
    'CLE':'CLE',
    'DAL':'DAL',
    'DEN':'DEN',
    'DET':'DET',
    'FA':'FA',
    'FA*':'FA',
    'GBP':'GB',
    'HOU':'HOU',
    'IND':'IND',
    'JAC':'JAX',
    'KCC':'KC',
    'LAC':'LAC',
    'LAR':'LAR',
    'LVR':'LV',
    'MIA':'MIA',
    'MIN':'MIN',
    'NEP':'NE',
    'NOS':'NO',
    'NYG':'NYG',
    'NYJ':'NYJ',
    'OAK':'OAK',
    'PHI':'PHI',
    'PIT':'PIT',
    'RAM':'LAR',
    'SDC':'SD',
    'SEA':'SEA',
    'SFO':'SF',
    'STL':'STL',
    'TBB':'TB',
    'TEN':'TEN',
    'WAS':'WSH'
}
team_map_nfldatapy_to_dbTid = {
    'ARI':22,
    'ATL':1,
    'BAL':31,
    'BUF':2,
    'CAR':29,
    'CHI':3,
    'CIN':4,
    'CLE':5,
    'DAL':6,
    'DEN':7,
    'DET':8,
    'FA':33,
    'FA*':33,
    'GBP':9,
    'HOU':32,
    'IND':11,
    'JAC':30,
    'KCC':12,
    'LAC':24,
    'LAR':14,
    'LVR':13,
    'MIA':15,
    'MIN':16,
    'NEP':17,
    'NOS':18,
    'NYG':19,
    'NYJ':20,
    'OAK':36,
    'PHI':21,
    'PIT':23,
    'RAM':14,
    'SDC':35,
    'SEA':26,
    'SFO':25,
    'STL':34,
    'TBB':27,
    'TEN':10,
    'WAS':28
}
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
    players_in_db = list(dfplayer['joinName'].str.lower().apply(apply_regex_replacements))

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
            inserts.pivot(index='name', columns='outlet', values='sourceId')
            .add_suffix('Id')        # add "Id" to each outlet column
            .reset_index()           # bring 'name' back as a column
        )
        inserts = inserts.copy()
        # adding joinName back so that it can me used to add posId and teamId
        inserts['joinName'] = inserts['name'].str.lower().apply(apply_regex_replacements)

        # retrieve position ID and team ID for the players being added
        id_lookups = query_database(
            query = '''
                SELECT 
                    pid.name, pos.posId as posId, team as teamId
                FROM
                    playeridlookupimport pid
                LEFT JOIN pos ON pos.pos = pid.position
            '''
        )
        id_lookups['teamId'] = id_lookups['teamId'].map(team_map_nfldatapy_to_dbTid)
        id_lookups['joinName'] = id_lookups['name'].str.lower().apply(apply_regex_replacements)
        id_lookups.drop(['name'], axis=1, inplace=True)

        # merge posId and teamId to inserts
        inserts = inserts.merge(id_lookups, on='joinName', how = 'left')

        # python or pandas is converting some to float so making sure the ids are ints
        cols_to_int = [i for i in inserts.columns if 'id' in i.lower()]
        # drop rows where ANY of those columns contain alphabetic characters
        mask = inserts[cols_to_int].apply(lambda col: col.astype(str).str.contains(r'[A-Za-z]', na=False))
        inserts = inserts[~mask.any(axis=1)]
        inserts[cols_to_int] = inserts[cols_to_int].astype(float).astype(int)

        if inserts.shape[0] == 0: pass
        else:
            export_database(
                dataframe = inserts,
                database_table = 'player',
                connection_string = None,
                if_exists='append'
            )
            print('inserts completed..')

    except Exception as ex:
        print(inserts.head())
        print(ex)

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
            if i == 'fantasypros':
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
        print(ex)
    return




