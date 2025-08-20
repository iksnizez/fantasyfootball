import json, requests, re, time, math
import pandas as pd 
#import numpy as np

#from selenium import webdriver
#from selenium.webdriver.chrome.service import Service

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
# get_player_info() after championship #> only saving raw json

class ktb():

    def __init__(            
            self, 
            season,
            week,
            league_id=None,
            league_cookies=None,
            league_headers=None,
            db_connection_str = None,
            database_export = False, 
            store_locally=True
        ):
        
        self.season = season
        self.week = week
        self.league_id = league_id or hf.league_id
        self.league_cookies = league_cookies or hf.league_cookies
        self.league_headers = league_headers or hf.league_headers
        self.pymysql_conn_str = db_connection_str

        self.database_export = database_export
        self.store_locally = store_locally

        self.map_team_ids_to_name = hf.map_team_ids_to_name
        self.lineupSlotID = hf.lineupSlotID   

        self.league_history_json = None 
        self.player_data_json = None
        self.weekly_player_json = None
        self.weekly_league_json = None
        self.weekly_boxscore_json = None

        self.df_draft_results = None
        self.df_teams = None
        self.df_games = None
        self.df_players = None
    
    # ===========================
    # data collection and storage
    # ===========================
    ### ONCE A YEAR AFTER SEASON OR DRAFT
    def get_league_history(
            self, 
            endpoints = ['mTeam','mBoxscore', 'mRoster','mSettings','kona_player_info','player_wl','mSchedule'],
            base_url = None,
            json_output_path = None,
            filepath_history_json = None
        ):
        """
        this aims to grab as much data as possible from the league history

        it appears that the only keys in the returned json with non-meta data are ['schedule', 'teams']
        """
        base_url = base_url or str(hf.espn_urls['league_history'])
        json_output_path = json_output_path or str(hf.folderpath_data) + '\\League History\\{s}-{f}_league_history.txt'

        year = str(self.season)
        prev_year = str(int(self.season) - 1)
        
        if filepath_history_json == None:
            url = base_url.format(year, self.league_id)
            with requests.get(url, headers=self.league_headers, cookies=self.league_cookies) as response:
                response.raise_for_status()   # raises HTTPError if not 200
                data = json.loads(response.content)
        else:
            with open(filepath_history_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
                

        if self.store_locally:
            with open(json_output_path.format(s=year, f=year), 'w') as outfile:
                json.dump(data, outfile)

        self.league_history_json = data.copy()
        return 

    def get_player_info(
            self, 
            base_url = None,
            json_output_path = None
        ):
        """ 
        this returns a json dump of all the data they ahve on the players - scoring, news, teams, etc..
        """
        base_url = base_url or str(hf.espn_urls['player_info'])
        json_output_path = json_output_path or str(hf.folderpath_data) + '\\League History\\{s}-{f}_player_history.txt'

        year = str(self.season)

        url = base_url.format(year, self.league_id)
        with requests.get(url, headers=self.league_headers, cookies=self.league_cookies) as response:
            response.raise_for_status()
            data = json.loads(response.content)

        if self.store_locally:
            with open(json_output_path.format(s=year, f=year), 'w') as outfile:
                json.dump(data, outfile)

        self.player_data_json = data.copy()
        return 
    
    def get_draft_results(
            self,
            base_url = None,
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
        base_url = base_url or str(hf.espn_urls['draft_results'])

        year = str(self.season)
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

        self.draft_results = drafts.copy()
        return 

    ### WEEKLY INSEASON
    def get_weekly_data(
            self,
            base_player_url = None,
            base_league_url = None,
            base_boxscore_url = None
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
        
        base_player_url = base_player_url or str(hf.espn_urls['base_player_url'])
        base_league_url = base_league_url or str(hf.espn_urls['base_league_url'])
        base_boxscore_url = base_boxscore_url or str(hf.espn_urls['base_boxscore_url'])
        
        season = str(self.season)
        week = str(self.week)

        url_player = base_player_url.format(self.season, self.week)
        with requests.request("GET", url_player, headers=self.league_headers, cookies=self.league_cookies) as response:
            response.raise_for_status()
            print('weekly player data hit:', response)
            player_json = json.loads(response.content)         

        url_league = base_league_url.format(self.season,self.week)
        with requests.request("GET", url_league, headers=self.league_headers, cookies=self.league_cookies) as response:
            response.raise_for_status()        
            print('weekly league data hit:', response)
            league_json = json.loads(response.content)

        url_boxscore = base_boxscore_url.format(season, week)
        with requests.request("GET", url_boxscore, headers=self.league_headers, cookies=self.league_cookies) as response:
            response.raise_for_status() 
        print('weekly boxscore data hit:', response)
        boxscore_json = json.loads(response.content)
        
        if self.store_locally:
            with open(hf.DATA_DIR / '\\Season\\player\\{}week{}_player.txt'.format(self.season, self.week), 'w') as outfile:
                json.dump(player_json, outfile)

            with open(hf.DATA_DIR / '\\Season\\league\\{}week{}_league.txt'.format(self.season, self.week), 'w') as outfile:
                json.dump(league_json, outfile)

            with open(hf.DATA_DIR / '\\Season\\boxscore\\{}week{}_boxscore.txt'.format(self.season, self.week), 'w') as outfile:
                json.dump(boxscore_json, outfile)

        self.weekly_player_json = player_json
        self.weekly_league_json = league_json
        self.weekly_boxscore_json = boxscore_json
        return 

    # ===========================
    #       data process
    # ===========================
    ### ONCE A YEAR AFTER SEASON OR DRAFT
    def process_league_history_team(
        self,
        filepath_history_json = None,
        database_table = 'ktbteams'
    ):
        year = str(self.season)
        prev_year = str(int(self.season) - 1)
    
        # THIS PROCESSES the mTeam endpoint which feeds the db ktbTeams
        
        ##### TEAM END PT AND TABLE
        allTeamData = []

        # teams are dictionaries in a single list
        if filepath_history_json == None:
            teams = self.league_history_json['teams']
        else:
            with open(filepath_history_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
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
            
            
            team = [int(self.season), teamId, teamName, abbr, wins, losses, ties, pf, pa, pfAllSeason,
                    rankRegSeason, rankFinalCalc,  streak, streakType, 
                    budgetSpent, acqs, drops, draftDayProjRank]
            allTeamData.append(team)
                
        cols = ['season', 'teamId', 'teamName', 'abbr', 'wins', 'losses', 'ties', 'pf', 'pa', 'pfAllSeason',
                'rankRegSeason', 'rankFinalCalc', 'streak', 'streakType', 
                'budgetSpent', 'acqs', 'drops', 'draftDayProjRank']
        teams = pd.DataFrame(allTeamData, columns = cols)

        # retrieve draft order for the season
        query_draft_order = 'SELECT teamId, pick FROM ktbdrafts WHERE season = %s and round = 1'
        draft_order = hf.query_database(query_draft_order, connection_string=None, params=(self.season,))
        map_draft_order = dict(zip(draft_order['teamId'], draft_order['pick']))
        
        # map the draft order to the teams df
        teams['pick'] = teams['teamId'].map(map_draft_order)
        del draft_order, query_draft_order, map_draft_order

        # TODO manually update in db
        teams['rankFinalKtb'] = None

        if self.database_export:
            hf.export_database(
                dataframe = teams, 
                database_table = database_table, 
                connection_string = self.pymysql_conn_str
            )

        self.df_teams = teams.copy()
        return 
    
    def process_league_history_boxscores(
        self,
        filepath_history_json = None,
        database_table = 'ktbgames'
    ):
        ## mBoxscore - has weekly matchup scores at the team level when looking at league history
        # it has player scores too if looking in season during a week.
                
        # teams are dictionaries in a single list
        if filepath_history_json == None:
            gameResults = self.league_history_json['schedule']
        else:
            with open(filepath_history_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
                gameResults = data['schedule']

        allGames = []
        playoffStarts = {}

        for i in range(len(gameResults)):
            g = gameResults[i]
            
            # handles playoff bye weeks
            if 'away' not in g:
                home = g['home']
                
                firstPlayoffWeek = g['matchupPeriodId']
                #checks if the season is already in the dict and skips re assigning if it is
                if self.season in playoffStarts:
                    pass
                else:
                    playoffStarts[int(self.season)] = firstPlayoffWeek
                
                week = g['matchupPeriodId']
                gameId = g['id']
                bye = 1
                teamOneId = None
                teamOneTiebreak = None
                teamOnePf = None
                
                teamTwoId = home['teamId']
                teamTwoTiebreak = home['tiebreak']
                teamTwoPf = home['totalPoints']
                
                winner = 0
                loser = 0
                tieTeamOne = 0  
                tieTeamTwo = 0
                
            # flatten game results for non-bye matchups
            else:
                week = g['matchupPeriodId']
                gameId = g['id']
                bye = 0
                
                away = g['away']
                home = g['home']
            
                teamOneId = away['teamId']
                teamOneTiebreak = away['tiebreak']
                teamOnePf = away['totalPoints']

                teamTwoId = home['teamId']
                teamTwoTiebreak = home['tiebreak']
                teamTwoPf = home['totalPoints']
            
                # label winner and loser
                if (teamOnePf + teamOneTiebreak) > (teamTwoPf + teamTwoTiebreak):
                    winner = teamOneId
                    loser = teamTwoId
                    tieTeamOne = 0  
                    tieTeamTwo = 0
                elif (teamOnePf + teamOneTiebreak) < (teamTwoPf + teamTwoTiebreak):
                    winner = teamTwoId
                    loser = teamOneId
                    tieTeamOne = 0
                    tieTeamTwo = 0
                else:
                    winner = 0
                    loser = 0
                    tieTeamOne = teamOneId
                    tieTeamTwo = teamTwoId
            
            gameResult = [int(self.season), week, gameId, teamOneId, teamOnePf, teamOneTiebreak, teamTwoId,
                        teamTwoPf, teamTwoTiebreak, winner, loser, tieTeamOne, tieTeamTwo,
                        bye#, playoff
                        ]
            allGames.append(gameResult)
                
        cols = ['season', 'week', 'gameId', 'teamOneId', 'teamOnePf', 'teamOneTiebreak',
                'teamTwoId','teamTwoPf', 'teamTwoTiebreak', 'winner', 'loser', 'tieTeamOne', 
                'tieTeamTwo',  'bye'
            ]

        games = pd.DataFrame(allGames, columns = cols)

        #will add values to this in the loop below
        games['playoffs'] = 0

        # adding playoff flag
        for k, v in playoffStarts.items():
            
            # regular season games
            mask = ((games['season'] == k) & (games['week'] < v))
            games.loc[mask, 'playoffs'] = 0
            # play off games
            mask = ((games['season'] == k) & (games['week'] >= v))
            games.loc[mask, 'playoffs'] = 1 

        if self.database_export:
            hf.export_database(
                dataframe = games, 
                database_table = database_table, 
                connection_string = self.pymysql_conn_str
            )

        self.df_games = games.copy()
        return 

    def process_league_history_players(
        self,
        filepath_history_json = None,
        database_table = 'ktbplayers'
    ):      
        
        if filepath_history_json == None:
            players = self.player_data_json['players']
        else:
            with open(filepath_history_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
                players = data['players'] 

        ### kona_player_info
        # this has player stats, info, scoring for season total and weekly (when searching in season)
        playerCount = {
            0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0, 10:0, 11:0, 12:0
        }

        allPlayers = []

        for p in players:
            
            #playerCount[p['onTeamId']] += 1
            playerId = p['id']
            ktbTeamId = p['onTeamId']
            nflTeamId = p['player']['proTeamId']
            playerName = p['player']['fullName']
            defaultPositionId = p['player']['defaultPositionId']
            ############# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< NEED TO FIGURE OUT HOW TO STORE ALL ELIGLBLE POS ID SLOTS
            #eligibleSlots = p['player']['eligibleSlots']
            try:
                auctionValue = p['player']['ownership']['auctionValueAverage']
                adp =  p['player']['ownership']['averageDraftPosition']

                stats = p['player']['stats']
            except:
                continue

            # they started adding multiple entries to the stats landing, there is final week of the season
            # and 2 others I can't figure out what they are. The season long one has an id of '00<year>'
            if len(stats) > 1:
                #searching for the correct id
                for s in stats:
                    if s['id' ] != '00' + str(self.season):
                        continue
                    else:
                        points = s['appliedTotal']
                        pointsAvg = s['appliedAverage']
            else:
                points = stats[0]['appliedTotal']
                pointsAvg = stats[0]['appliedAverage']
                
            ############# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< NEED TO SEPARTE THE STATS, FIND ESPN ID TO STAT NAME MAP
            #stats = p['player']['stats']['stats']
            try:
                positionRank =  p['ratings']['0']['positionalRanking']
                overallRank =  p['ratings']['0']['totalRanking']
            except:
                positionRank = None
                overallRank = None
                
            playerData = [self.season, playerId, playerName, nflTeamId, ktbTeamId, defaultPositionId, #eligibleSlots, 
                    auctionValue, adp, points, pointsAvg, positionRank, overallRank, #stats
                    ]

                
            allPlayers.append(playerData)

        cols = ['season', 'playerId', 'playerName', 'nflTeamId', 'ktbTeamId', 'defaultPositionId', #'eligibleSlots', 
                'auctionValue', 'adp', 'points', 'pointsAvg', 'positionRank', 'overallRank', #'stats'
                ]    
        players = pd.DataFrame(allPlayers, columns = cols)

        if self.database_export:
            hf.export_database(
                dataframe = players, 
                database_table = database_table, 
                connection_string = self.pymysql_conn_str
            )

        self.df_players = players.copy()
        return   

    
    ### WEEKLY INSEASON
    def process_weekly_data_league(
        self,
        filepath_history_json = None,
        database_table = 'ktbWeeklyTeams'
    ):

        teams = []
    
        if filepath_history_json == None:
            league = self.player_data_json
        else:
            with open(filepath_history_json, 'r', encoding='utf-8') as f:
                league = json.load(f)

        season = league['seasonId']
        week = league['scoringPeriodId']
                 
        # code mapping for roster positions
        lineupSlotID = hf.lineupSlotID
        teamIds = hf.map_team_ids_to_name
        
        #extract data
        for t in league['teams']:
            teamcols = [
                'season', 'week', 'id', 'teamName', 'currentProjectedRank',
                'draftDayProjectedRank', 'playoffSeed',
                'rankCalculatedFinal', 'rankFinal', 'waiverRank',
                'gb', 'wins', 'losses', 'ties', 'wp', 'pf', 'pa', 'streakLength',
                'streakType', 'budgetSpent', 'acquisitions', 'drops'
            ]
            
            crank = t['currentProjectedRank']
            drank = t['draftDayProjectedRank']
            id = t['id']
            try:
                team = t['name']
            except:
                team = t['location'] + ' ' + t['nickname']
            seed = t['playoffSeed']
            calrank = t['rankCalculatedFinal']
            finrank = t['rankFinal']
            waiverrank = t['waiverRank']
            gb = t['record']['overall']['gamesBack']
            wins = t['record']['overall']['wins']
            losses = t['record']['overall']['losses']
            ties = t['record']['overall']['ties']
            wp = t['record']['overall']['percentage']
            pf = t['record']['overall']['pointsFor']
            pa = t['record']['overall']['pointsAgainst']
            streak = t['record']['overall']['streakLength']
            streaktype = t['record']['overall']['streakType']
            spent = t['transactionCounter']['acquisitionBudgetSpent']
            acqcount = t['transactionCounter']['acquisitions']
            drops = t['transactionCounter']['drops']

            temp = [
                season, week, id, team, crank, drank, seed, calrank, finrank,
                waiverrank, gb, wins, losses, ties, wp, pf, pa, streak, streaktype, spent,
                acqcount, drops
            ]

            teams.append(temp)        

        teams = pd.DataFrame(teams, columns=teamcols).drop_duplicates(['season', 'week', 'id'])

        if self.database_export:
            hf.export_database(
                dataframe = teams,
                database_table=database_table,
                connection_string=None,
                if_exists='append'
            )

        return

    
    # these are not doing anything 
    def process_weekly_data_boxscore(
        self,
        filepath_history_json = None,
        database_table = 'ktbWeeklyScores'
    ):

        if filepath_history_json == None:
            boxscore_json = self.weekly_boxscore_json
            season = season
        else:
            
            filename = filepath_history_json.split('/')[-1]
            season = int(filename[:4])
            with open(filepath_history_json, 'r', encoding='utf-8') as f:
                boxscore_json = json.load(f)

        week = boxscore_json['scoringPeriodId']
        # espn increments matchups ids over the whole season
        #  week 1 = 0-5, week 2 = 6-11
        matches = [-6+(6*week),-5+(6*week),-4+(6*week),-3+(6*week),-2+(6*week),-1+(6*week)]

        cols_boxscores = [
            'season', 'week', 'teamId', 'oppId', 'pf', 'pa', 'w', 'l', 't'
        ]
        
        data = []
        # loops through the matchups for the week. Matchups are labeled 0 - 83
        for i in matches:

            #grab data starting point for easier reading later
            game = boxscore_json['schedule'][i]
            #grab team ids for the match up
            away_team = game['away']['teamId']
            away_score = float(game['away']['totalPoints'])
            print(away_score)
            
            home_team = game['home']['teamId']
            home_score = float(game['home']['totalPoints'])


            if home_score > away_score:
                home_win, home_loss, home_tie = 1, 0, 0
                away_win, away_loss, away_tie = 0, 1, 0
            elif home_score < away_score:
                home_win, home_loss, home_tie = 0, 1, 0
                away_win, away_loss, away_tie = 1, 0, 0
            else:
                home_win, home_loss, home_tie = 0, 0, 1
                away_win, away_loss, away_tie = 0, 0, 1

            

            data.append([
                season, week, home_team, away_team, home_score, away_score, home_win, home_loss, home_tie
            ])
            data.append([
                season, week, away_team, home_team, away_score, home_score
            ])


            # assign the dictionary with scoring by position dictionary
            #away_scoring = ['schedule'][i]['away']['cumulativeScore']['scoreByStat']
            #away_scoring = ['schedule'][i]['home']['cumulativeScore']['scoreByStat']

        games = pd.DataFrame(
            data, columns = cols_boxscores
        )

        if self.database_export:
            hf.export_database(
                dataframe = games,
                database_table=database_table,
                connection_string=None,
                if_exists='append'
            )

        return

    
    def process_weekly_data_player(
        self,
        filepath_history_json = None,
        database_table = None
    ):
        players = []

        if filepath_history_json == None:
            player = self.weekly_player_json
        else:
            with open(filepath_history_json, 'r', encoding='utf-8') as f:
                player = json.load(f)

        
        return