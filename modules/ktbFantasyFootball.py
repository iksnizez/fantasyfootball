import json, requests, re, time, math
import pandas as pd 
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

###############################################################################
### AFTER DRAFT
# get_draft_results() 

### WEEKLY, DURING SEASON
# get_weekly_data() >>> same night after last MNF game finishes

### AFTER CHAMPIONSHIP
# get_league_history()  
    # db table ktbteams:rankFinalKtb is a manual update, inverse of the draft order next year
# get_player_history() after championship #> only saving raw json

class ktb():

    def __init__(            
            self, 
            browser_path,
            database_export = False, 
            store_locally=True, 
            config_path = '..\\..\\..\\Notes-General\\config.txt',
            league_id = 245118,
            league_cookies = {
                "SWID":"{53904938-E572-4FD1-9F30-10A8A39DA775}",
                "espn_s2":"AEBvpGk1SP6ibJZS7FOb%2Fj96G4TZWJrrUKPGNpWRagAthxCljUQkzY7J6sxAt%2Bezfk9fygiXuoY1aBehpYE0m4n7L%2BwbOJXPBOVp10dpJ3BXGwLy9RB5HI%2FNAOnL4czsqizbi%2FPdMexDbiLunx35kXtV4JWzlc3UqS%2Fkvqw6%2F58pzotJFoF%2FTK3mCgUyherJPFl26P5ItBVe2dGf6Y0%2B2WDjiDOLAh4xeXDggKVyoOxBoGZT4ODtKLk9agSEmSG9zwz73MGF6JerNaTg%2BcPKxRZex6f9caDHD%2BSxj2zcwh3q6w%3D%3D"
            },
            league_headers = {
                'X-Fantasy-Filter': '{"players": {"limit": 1500, "sortPercOwned":{"sortAsc":false,"sortPriority":1}}}'
            }
    
        ):

        self.browser_path = browser_path
        self.database_export = database_export
        self.store_locally = store_locally
        self.config_path = config_path
        
        self.league_id = league_id
        self.cookies = league_cookies
        self.headers = league_headers
        self.map_team_ids_to_name =  {
            1: 'John', 2: 'Gomer', 3: 'Pope', 4: "Jamie", 
            5: "Geik", 6: "Bryan", 7: "Chaunce", 8: "Sam", 
            9: "Chris", 10: "Murphy", 11: "Colin", 12: 'Ethan'
        }
        self.lineupSlotID = {
            17: 'K', 0: 'QB', 20: 'bench', 15: 'DP', 6: 'TE', 
            23: 'FLEX', 4: 'WR', 2: 'RB', 21: 'IR'
        }

        # regex replacement mapping used to make more joinable names
        self.suffix_replace = {
            "\\.":"", "`":"", "'":"",
            " III$":"", " IV$":"", " II$":"", " iii$":"", " ii$":"", " iv$":"", " v$":"", " V$":"",
            " jr$":"", " sr$":"", " jr.$":"", " sr.$":"", " Jr$":"", " Sr$":"", " Jr.$":"", " Sr.$":"", 
            " JR$":"", " SR$":"", " JR.$":"", " SR.$":""
        }

        self.pymysql_conn_str = self.get_pymysql_conn_str(self.config_path)
    
    
    #############
    # general helper funcs
    def get_pymysql_conn_str(self, config_path = None):
        """
        return pymysql connection string from local config file
        """
        if config_path == None:
            config_path = self.config_path

        with open(config_path, 'r') as f:
            creds = f.read()
 
        creds = json.loads(creds)
        pymysql_conn_str = creds['pymysql']['nfl']
        del creds

        return pymysql_conn_str

    def export_database(self, dataframe, database_table, connection_string=None):

        if connection_string == None:
            connection_string = self.pymysql_conn_str

        try:
            dataframe.to_sql(
                name=database_table, 
                con=connection_string, 
                if_exists='append', 
                index=False
            )
            
            print('successfully added data')
            return 
            
        except:
            message = 'database load failed'
            print(message)
            return 
    
    def open_browser(self, browser_path = None, retry_delay = 5, retry_attempts = 3):
        
        # an override browswer path can be provided but normally use the one provided whe nthe class is created 
        if browser_path is None:
            browser_path = self.browser_path
        
        service = Service(browser_path)
        driver = webdriver.Firefox(service=service)

        # start browser
        return driver
    
    def apply_regex_replacements(self, value):
        """
        used to format names into their most joinable form
        """
        for pattern, replacement in self.suffix_replace.items():
            value = re.sub(pattern, replacement, value, flags=re.IGNORECASE)
        return value
    
    #############
    # data collection and storage
    def get_league_history(
            self, 
            season, 
            endpoints = ['mTeam','mBoxscore', 'mRoster','mSettings','kona_player_info','player_wl','mSchedule'],
            base_url = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/{}?view=mLiveScoring&view=mMatchupScore&view=mRoster&view=mSettings&view=mStandings&view=mStatus&view=mTeam&view=modular&view=mNav',
            json_output_path = '..\\data\\League History\\{s}-{f}_league_history.txt',
            save_json = False,
            process_json_to_df = True,
            database_table_teams = 'ktbteams' 
        ):
        """
        this aims to grab as much data as possible from the league history

        it appears that the only keys in the returned json with non-meta data are ['schedule', 'teams']
        """
        year = str(season)
        prev_year = str(int(season) - 1)
        
        url = base_url.format(year, self.league_id)
        response = requests.get(url, headers=self.headers, cookies=self.cookies)

        data = json.loads(response.content)

        if save_json:
            with open(json_output_path.format(s=year, f=year), 'w') as outfile:
                json.dump(data, outfile)


        if process_json_to_df:
            
            ##### TEAM END PT AND TABLE
            allTeamData = []

            # teams are dictionaries in a single list
            teams = data['teams']

            # looping through team to create df for db load
            for t in teams:
                teamId = t['id']
                #teamName = t['location'] + " " +  t['nickname'] 
                teamName = t['name']
                abbr = t['abbrev']
                pfAllSeason = t['points']
                rankRegSeason = t['playoffSeed']
                rankFinalCalc = t['rankCalculatedFinal']
                draftDayProjRank = t['draftDayProjectedRank']
                wins = t['record']['overall']['wins']
                losses = t['record']['overall']['losses']
                ties = t['record']['overall']['ties']
                pf = t['record']['overall']['pointsFor']
                pa = t['record']['overall']['pointsAgainst']
                streak = t['record']['overall']['streakLength']
                streakType = t['record']['overall']['streakType']
                budgetSpent = t['transactionCounter']['acquisitionBudgetSpent']
                acqs = t['transactionCounter']['acquisitions']
                drops = t['transactionCounter']['drops']
                
                
                team = [int(season), teamId, teamName, abbr, wins, losses, ties, pf, pa, pfAllSeason,
                        rankRegSeason, rankFinalCalc,  streak, streakType, 
                        budgetSpent, acqs, drops, draftDayProjRank]
                allTeamData.append(team)
                    
            cols = ['season', 'teamId', 'teamName', 'abbr', 'wins', 'losses', 'ties', 'pf', 'pa', 'pfAllSeason',
                    'rankRegSeason', 'rankFinalCalc', 'streak', 'streakType', 
                    'budgetSpent', 'acqs', 'drops', 'draftDayProjRank']
            teams = pd.DataFrame(allTeamData, columns = cols)

            # retrieve draft order for the season
            query_draft_order = 'SELECT teamId, pick FROM ktbdrafts WHERE season = ? and round = 1;'
            draft_order = pd.read_sql_query(
                            sql ='SELECT teamId, pick FROM ktbdrafts WHERE season = %s and round = 1',
                            con = self.pymysql_conn_str,
                            params= (season,)
                        )
            map_draft_order = dict(zip(draft_order['teamId'], draft_order['pick']))
            
            # map the draft order to the teams df
            teams['pick'] = teams['teamId'].map(map_draft_order)
            del draft_order, query_draft_order, map_draft_order

            # TODO manually update in db
            teams['rankFinalKtb'] = None

            if self.database_export:
                self.export_database(
                dataframe = teams, 
                database_table = database_table_teams, 
                connection_string = self.pymysql_conn_str
            )
        
            ##### BOXSCORE END PT AND TABLE

        return [data, teams]

    def get_player_info(
            self, 
            season, 
            base_url = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/{}?scoringPeriodId=19&view=kona_player_info',
            json_output_path = '..\\data\\League History\\{s}-{f}_player_history.txt',
            save_json = False
        ):
        """ 
        this returns a json dump of all the data they ahve on the players - scoring, news, teams, etc..
        """
        year = str(season)

        url = base_url.format(year, self.league_id)
        response = requests.get(url, headers=self.headers, cookies=self.cookies)

        data = json.loads(response.content)

        if save_json:
            with open(json_output_path.format(s=year, f=year), 'w') as outfile:
                json.dump(data, outfile)

        return data
    
    def get_draft_results(
            self,
            draft_year, 
            base_url = 'https://fantasy.espn.com/football/league/draftrecap?leagueId={lid}&seasonId={sid}',
            nTeams = 12,
            database_table = 'ktbdrafts',
            map_team_id = {
                'free breece':12, 
                'Team Gomer':2, 
                'Big Baby Nate':9,
                'Poopstained Warriors':4, 
                'Purdy Chubby':6, 
                'Jamo no Chaser':1,
                'CeeDeez Nutz':10, 
                'JB got last in 23':5, 
                'Touchdown My Pants':8,
                'Team Chaunce':7, 
                'The Suavin Scoregasms':11, 
                'DPD DannyDimes':3
            }
        ):
        """
        saves the leagues draft results from the webpage - 
        
        **map_team_id needs to be updated for each year manually
                the team name at the time of the draft need the update
        
        """
        year = str(draft_year)
        url = base_url.format(lid = str(self.league_id), sid = str(year))

        # will hold the data
        drafts = pd.DataFrame(columns=['season', 'pick', 'round', 'overallPick',
                                   'name', 'playerTeam', 'pos',
                                   'teamName'])

        driver = self.open_browser(self.browser_path)

        driver.get(url)
        time.sleep(3)
        ps = driver.page_source
        soup = bs(ps, "html.parser")
        driver.close()
        
        picks = soup.find_all("td", class_="Table__TD")

        o = 1  # overall pick count tracker

        for p in range(0, len(picks), 3):
            # p = pick number; p+1= player name, team, pos; p+2 = fantasy team

            # splitting the player info <td>
            playerInfo = picks[p + 1].text.split()

            # checking for name suffixes and collapsing them into the last name when they exist
            if len(playerInfo) == 5:
                suffix = playerInfo.pop(2)
                playerInfo[1] += " " + suffix

            firstName = playerInfo[0]
            lastName = playerInfo[1]
            name = firstName + ' ' + lastName
            team = playerInfo[2].replace(",", "")
            pos = playerInfo[3]

            # round details
            n = int(picks[p].text)  # pick number in the round
            r = math.ceil((o / nTeams))  # round number

            # fantasy team making the pick
            fTeam = picks[p + 2].text

            pick = [year, n, r, o, name, team, pos, fTeam]
            # drafts = pd.concat([drafts, pick], ignore_index=True)
            drafts.loc[len(drafts.index)] = pick
            o += 1
        
        # add team league id to data
        drafts['teamId'] = drafts['teamName'].map(map_team_id)
        
        drafts = drafts[[
            'teamName', 'season', 'pick', 'round', 'overallPick', 'name', 'playerTeam', 'pos', 'teamId'
        ]]

        if self.database_export:
            self.export_database(
                dataframe = drafts, 
                database_table = database_table, 
                connection_string = self.pymysql_conn_str
            )

        return drafts

    def get_weekly_data(
            self,
            season,
            week,
            base_player_url = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=kona_player_info&scoringPeriodId={}',
            base_league_url = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=mLiveScoring&view=mMatchupScore&view=mPositionalRatings&view=mTeam&view=modular&view=mNav&view=mMatchupScore&scoringPeriodId={}',
            base_boxscore_url = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=mBoxscore&scoringPeriodId={}',
            save_json = False
        ):
        """
        weekly score retrieval

        when this is ran after the last game of the weekly schedule
        it will pull all of the player stats and scores for the week
        once ESPN updates the current scoring period the full load of data
        is lost so it has to be run after the final game and before the
        week change in their system.
        """
        # fantasy.espn seems to be deprecated for lm-api-reads. 
        #url = "https://fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=kona_player_info&scoringPeriodId={}"
        #url = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=modular&view=mNav&view=mMatchupScore&view=mScoreboard&view=mSettings&view=mTopPerformers&view=mTeam"
        
        season = str(season)
        week = str(week)

        url_player = base_player_url.format(season, week)
        response = requests.request("GET", url_player, headers=self.headers, cookies=self.cookies)
        print('weekly player data hit:', response)
        player_json = json.loads(response.content)         

        url_league = base_league_url.format(season,week)
        response = requests.request("GET", url_league, headers=self.headers, cookies=self.cookies)
        print('weekly league data hit:', response)
        league_json = json.loads(response.content)

        url_boxscore = base_boxscore_url.format(season, week)
        response = requests.request("GET", url_boxscore, headers=self.headers, cookies=self.cookies)
        print('weekly boxscore data hit:', response)
        boxscore_json = json.loads(response.content)
        


        if save_json:
            with open('..\Data\Season\{}week{}_player.txt'.format(season, week), 'w') as outfile:
                json.dump(player_json, outfile)

            with open('..\Data\Season\{}week{}_league.txt'.format(season, week), 'w') as outfile:
                json.dump(league_json, outfile)

            with open('..\Data\Season\{}week{}_boxscore.txt'.format(season, week), 'w') as outfile:
                json.dump(boxscore_json, outfile)

        return [player_json, league_json, boxscore_json]

        