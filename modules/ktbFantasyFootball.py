import json, requests, re, time, math
import pandas as pd 
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

import helperModule as hf

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
            league_id=None,
            league_cookies=None,
            league_headers=None,
            db_connection_str = None,
            database_export = False, 
            store_locally=True
        ):
        
        self.league_id = league_id or hf.league_id
        self.league_cookies = league_cookies or hf.league_cookies
        self.league_headers = league_headers or hf.league_headers
        self.pymysql_conn_str = db_connection_str

        self.database_export = database_export
        self.store_locally = store_locally

        self.map_team_ids_to_name = hf.map_team_ids_to_name
        self.lineupSlotID = hf.lineupSlotID    
    
    #############
    # data collection and storage
    def get_league_history(
            self, 
            season, 
            endpoints = ['mTeam','mBoxscore', 'mRoster','mSettings','kona_player_info','player_wl','mSchedule'],
            base_url = hf.espn_urls['league_history'],
            json_output_path = hf.folderpath_data + '\\League History\\{s}-{f}_league_history.txt',
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
        response = requests.get(url, headers=self.league_headers, cookies=self.league_cookies)

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
                hf.export_database(
                    dataframe = teams, 
                    database_table = database_table_teams, 
                    connection_string = self.pymysql_conn_str
                )
        
            ##### BOXSCORE END PT AND TABLE

        return [data, teams]

    def get_player_info(
            self, 
            season, 
            base_url = hf.espn_urls['player_info'],
            json_output_path = hf.folderpath_data + '\\League History\\{s}-{f}_player_history.txt',
            save_json = False
        ):
        """ 
        this returns a json dump of all the data they ahve on the players - scoring, news, teams, etc..
        """
        year = str(season)

        url = base_url.format(year, self.league_id)
        response = requests.get(url, headers=self.league_headers, cookies=self.league_cookies)

        data = json.loads(response.content)

        if save_json:
            with open(json_output_path.format(s=year, f=year), 'w') as outfile:
                json.dump(data, outfile)

        return data
    
    def get_draft_results(
            self,
            draft_year, 
            base_url = hf.espn_urls['draft_results'],
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

        driver = hf.open_browser()

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
            hf.export_database(
                dataframe = drafts, 
                database_table = database_table, 
                connection_string = self.pymysql_conn_str
            )

        return drafts

    def get_weekly_data(
            self,
            season,
            week,
            base_player_url = hf.espn_urls['base_player_url'],
            base_league_url = hf.espn_urls['base_league_url'],
            base_boxscore_url = hf.espn_urls['base_boxscore_url'],
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
        response = requests.request("GET", url_player, headers=self.league_headers, cookies=self.league_cookies)
        print('weekly player data hit:', response)
        player_json = json.loads(response.content)         

        url_league = base_league_url.format(season,week)
        response = requests.request("GET", url_league, headers=self.league_headers, cookies=self.league_cookies)
        print('weekly league data hit:', response)
        league_json = json.loads(response.content)

        url_boxscore = base_boxscore_url.format(season, week)
        response = requests.request("GET", url_boxscore, headers=self.league_headers, cookies=self.league_cookies)
        print('weekly boxscore data hit:', response)
        boxscore_json = json.loads(response.content)
        


        if save_json:
            with open(hf.DATA_DIR + '\\Season\\{}week{}_player.txt'.format(season, week), 'w') as outfile:
                json.dump(player_json, outfile)

            with open(hf.DATA_DIR + '\\Season\\{}week{}_league.txt'.format(season, week), 'w') as outfile:
                json.dump(league_json, outfile)

            with open(hf.DATA_DIR + '\\Season\\{}week{}_boxscore.txt'.format(season, week), 'w') as outfile:
                json.dump(boxscore_json, outfile)

        return [player_json, league_json, boxscore_json]

        