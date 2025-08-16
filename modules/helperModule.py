import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from sqlalchemy import create_engine
from config import ESPN_COOKIES_SWID, ESPN_COOKIES_S2, PYMYSQL_NFL, BROWSER_DIR, ESPN_LEAGUE_ID, ESPN_HEADERS_NAME, ESPN_HEADERS, DATA_DIR


folderpath_data = DATA_DIR        
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

# column structures for stat projections
projection_columns = ["outlet","date", "season", "week", "playerId", "name", "shortName", "pos", "team", 'GamesPlayed',
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

ranking_columns = ["outlet", "date", "season", "week", "group", "expert", "rank","name","playerId","team","pos", "high", "low"]

adp_columns = ['outlet', 'date', 'playerId', 'name', 'shortName' , 'pos', 'team', 'adp', 'high', 'low']

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
#############
# general helper funcs
def export_database(dataframe, database_table, connection_string=None):

    try:
        if connection_string == None:
            dataframe.to_sql(
                name=database_table, 
                con=PYMYSQL_NFL, 
                if_exists='append', 
                index=False
            )

        else:
            dataframe.to_sql(
                name=database_table, 
                con=connection_string, 
                if_exists='append', 
                index=False
            )
        
        print('successfully added data')
        return 
        
    except Exception as ex:
        message = 'database load failed'
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
        
        print('query successfully')
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
