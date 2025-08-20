import requests, time, re, os, json
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
pd.set_option('display.max_columns', 500)
import numpy as np
from bs4 import BeautifulSoup as bs
from datetime import date
from io import StringIO

#from selenium import webdriver
#from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
#from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC

import modules.helperModule as hf
league = 'nfl'


class scrapers():

    def __init__(
        self,
        season,
        week,
        today = date.today()
    ):
        self.season = season
        self.week = week
        self.today = today

        if self.week < 10:
            self.strWeek = "0" + str(self.week)
        else:
            self.strWeek = self.week

        self.scraping_urls = {
            'cbs':{
                'offseason':{
                    'projections':"https://www.cbssports.com/fantasy/football/stats/{pos}/{year}/restofseason/projections/ppr/",
                    'rankings':"https://www.cbssports.com/fantasy/football/rankings/ppr/{pos}/",
                    'adp':"https://www.cbssports.com/fantasy/football/draft/averages/"
                },
                'inseason':{
                    'projections':"https://www.cbssports.com/fantasy/football/stats/{pos}/{season}/{week}/projections/ppr/",
                    'rankings':"https://www.cbssports.com/fantasy/football/rankings/ppr/{pos}/"
                }
            },
            'ffp':{
                'offseason':{
                    'projections':None,
                    'rankings':"https://www.fantasypros.com/nfl/rankings/half-point-ppr-cheatsheets.php",
                    'adp':"https://www.fantasypros.com/nfl/adp/half-point-ppr-overall.php"
                },
                'inseason':{
                    'projections':None,
                    'rankings':[
                        r"https://www.fantasypros.com/nfl/rankings/half-point-ppr-{}",
                        r"https://www.fantasypros.com/nfl/rankings/{}"
                    ]
                }
            },
            
            'espn':{
                'offseason':{
                    'projections':"https://fantasy.espn.com/football/players/projections",
                    'rankings':{
                        "QB":"https://www.espn.com/fantasy/football/story/_/page/FFPreseasonRank25QBPPR/nfl-fantasy-football-draft-rankings-2025-qb-quarterback",
                        "RB":"https://www.espn.com/fantasy/football/story/_/page/FFPreseasonRank25RBPPR/nfl-fantasy-football-draft-rankings-2025-rb-running-back-ppr",
                        "WR":"https://www.espn.com/fantasy/football/story/_/page/FFPreseasonRank25WRPPR/nfl-fantasy-football-draft-rankings-2025-wr-wide-receiver-ppr",
                        "TE":"https://www.espn.com/fantasy/football/story/_/page/FFPreseasonRank25TEPPR/nfl-fantasy-football-draft-rankings-2025-te-tight-end-ppr",
                        "K":"https://www.espn.com/fantasy/football/story/_/page/FFPreseasonRank25KPPR/nfl-fantasy-football-draft-rankings-2025-kicker-k",
                        "DST":"https://www.espn.com/fantasy/football/story/_/page/FFPreseasonRank25DSTPPR/nfl-fantasy-football-draft-rankings-2025-dst-defense",
                        "IDP":"https://www.espn.com/fantasy/football/story/_/page/FFPreseasonRank25IDP/2025-fantasy-football-rankings-idp-defense-defensive-line-linebacker-defensive-back",
                    },
                    'adp':"https://fantasy.espn.com/football/livedraftresults"
                },
                'inseason':{
                    'projections':"https://fantasy.espn.com/football/players/projections?leagueFormatId=3",
                    'rankings':{
                        "QB":"",
                        "RB":"",
                        "WR":"",
                        "TE":"",
                        "K":"",
                        "DST":"",
                        "IDP":""
                    }
                }
            },
            'nfl':{
                'offseason':{
                    'projections':[
                        "https://fantasy.nfl.com/research/projections?position=O&sort=projectedPts&statCategory=projectedStats&statSeason={season}&offset={offset}&statType=seasonProjectedStats&ajax=1",
                        "https://fantasy.nfl.com/research/projections?position=7&sort=projectedPts&statCategory=projectedStats&statSeason={season}&offset={offset}&statType=seasonProjectedStats&ajax=1",
                        "https://fantasy.nfl.com/research/projections?position=8&sort=projectedPts&statCategory=projectedStats&statSeason={season}&offset={offset}&statType=seasonProjectedStats&ajax=1"
                        ],
                    'rankings':{
                        "QB":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=QB&sort=1&statSeason={season}&statType=seasonStats",
                        "RB":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=RB&sort=1&statSeason={season}&statType=seasonStats",
                        "WR":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=WR&sort=1&statSeason={season}&statType=seasonStats",
                        "TE":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=TE&sort=1&statSeason={season}&statType=seasonStats",
                        "K":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=K&sort=1&statSeason={season}&statType=seasonStats",
                        "DEF":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=DEF&sort=1&statSeason={season}&statType=seasonStats",
                        "DL":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=DL&sort=1&statSeason={season}&statType=seasonStats",
                        "LB":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=LB&sort=1&statSeason={season}&statType=seasonStats",
                        "DB":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=DB&sort=1&statSeason={season}&statType=seasonStats"
                    },
                    'adp':'https://fantasy.nfl.com/research/rankings?offset={offset}&sort=average&statType=draftStats&ajax=1'
                },
                'inseason':{
                    'projections':[
                        "https://fantasy.nfl.com/research/projections?offset={offset}&position=0&statCategory=projectedStats&statSeason={season}&statType=weekProjectedStats&statWeek={week}",
                        "https://fantasy.nfl.com/research/projections?offset={offset}&position=7&statCategory=projectedStats&statSeason={season}&statType=weekProjectedStats&statWeek={week}",
                        "https://fantasy.nfl.com/research/projections?offset={offset}&position=8&statCategory=projectedStats&statSeason={season}&statType=weekProjectedStats&statWeek={week}",
                        ],
                    'rankings':{
                        "QB":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=QB&sort=1&statType=weekStats&week={week}",     
                        "RB":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=RB&sort=1&statType=weekStats&week={week}",
                        "WR":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=WR&sort=1&statType=weekStats&week={week}",
                        "TE":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=TE&sort=1&statType=weekStats&week={week}",
                        "K":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=K&sort=1&statType=weekStats&week={week}",
                        "DST":"https://fantasy.nfl.com/research/rankings?leagueId=0&position=DEF&sort=1&statType=weekStats&week={week}"
                    }
                }
            },
        }

        self.scraped_dfs = {
            'projections':{
                'cbs':None,
                'ffp':None,
                'espn':None,
                'nfl':None
            },
            'rankings':{
                'cbs':None,
                'ffp':None,
                'ffp_ecr':None,
                'espn':None,
                'nfl':None
            },
            'adps':{
                'cbs':None,
                'ffp':None,
                'espn':None,
                'nfl':None
            },
            'lines':{
                'bp':None
            },
            'game_scores':{
                'scores':None,
                'records':None
            }
        }

        self.processed_dfs = {
            'projections':None,
            'rankings':None,
            'adps':None,
            'lines':None
        }
 
    # ====================
    #       cbs
    # ====================
    def cbs_projections(
        self,
        inseason=False,
        export = True,
        positions = ["QB", "RB", "WR", "TE", "K", "DST"],
        tableClass = "TableBase-table",
        tableHeader = "TableBase-headTr",
        headerClass = "Tablebase-tooltipInner",
        tableRow = "TableBase-bodyTr",
        tableD = "TableBase-bodyTd"
    ):
        
        df_cbs_proj = pd.DataFrame(columns=hf.projection_columns)

        if inseason:
            url_projections = self.scraping_urls['cbs']['inseason']['projections']

            # loop through each position to retrieve HTML and convert to df
            for p in range(len(positions)):
                
                time.sleep(3)
                #updating URL for each position
                url_formatted = url_projections.format(pos=positions[p], season=self.season, week=self.week)

                # retreiving HTML and converting it to soup
                r = requests.get(url_formatted)
                soup = bs(r.text, features='lxml')

                # accessing table with the data
                table = soup.find("table", class_= tableClass)

                
                # accounting for the difference in DEF headers
                if positions[p] == "DST":
                    cols = ["pos", "team","name"]
                else:
                    cols = ["playerId", "name", "shortName", "pos", "team"]
                
                ### grabbing column names from the thead for the position. These will be used to create the temp. pos dataframe
                #retrieving column names from the HTML
                for i in table.find_all("div", class_=headerClass):
                    cols.append(''.join(filter(str.isalnum, i.text)))
                    
                # accessing the data in the body
                body = table.find("tbody")
                # looping through rows
                data = []
                for tr in body.find_all("tr", class_=tableRow):
                    # accounting for DST and populating pos as DST since it is not provided
                    if positions[p] == "DST":
                        player_data = ["DST"]
                    else:
                        player_data = []
                    
                    for td in tr.find_all("td", class_=tableD):
                        
                        if positions[p] == "DST":
                        # pulling team name 
                            span = td.find_all("span",class_="CellLogoNameLockup")
                            if span:   
                                for s in span:
                                    player_data.append(s.find("a")["href"].split("/")[3])
                                    player_data.append(str.strip(td.text))
                            
                            # non-span <Td>, 
                            else:
                                player_data.append(str.strip(td.text))
                                
                        # processing table body for all pos except DST
                        else:
                            #the player name, id, pos, and team are all in spans. 
                            #the spans are not present in the stat <td>'s
                            span_short = td.find_all("span",class_="CellPlayerName--short")
                            span_long = td.find_all("span",class_="CellPlayerName--long")

                            # if the <td> has a span, the player info will be extracted
                            if span_long:

                                for s in span_long:
                                    # player Id from the href url
                                    player_data.append(s.find("a")["href"].split("/")[3])
                                    # player full name
                                    player_data.append(str.strip(s.find("a").text).replace(".", ""))

                                for s in span_short:
                                    # player short name
                                    player_data.append(s.find("a").text.replace(".", ""))
                                    #player position
                                    player_data.append(str.strip(s.find("span", class_="CellPlayerName-position").text))
                                    #player nfl team
                                    player_data.append(str.strip(s.find("span", class_="CellPlayerName-team").text).replace("JAC", "JAX").replace("WAS", "WSH"))
                        
                            # non-span <Td>
                            else:
                                player_data.append(str.strip(td.text))
                    
                    # creates the list of players, each player is a list with stats
                    data.append(player_data)
                
                # converts list of list to data frame with the applicable columns pulled earlier
                pos_df = pd.DataFrame(data, columns=cols)
                
                # concats all of the position data to the master df
                df_cbs_proj = pd.concat([df_cbs_proj, pos_df], axis=0, ignore_index=True)

            df_cbs_proj.loc[:,'outlet'] = "cbs"
            df_cbs_proj.loc[:,'date'] = self.today
            df_cbs_proj.loc[:,'season'] = self.season
            df_cbs_proj.loc[:,'week'] = self.week
            df_cbs_proj.loc[:,'LongestFieldGoal'] = np.nan

        # offseason
        else:
            url_projections = self.scraping_urls['cbs']['offseason']['projections']

            # loop through each position to retrieve HTML and convert to df
            for p in range(len(positions)):
                
                time.sleep(3)
                #updating URL for each position
                url_formatted = url_projections.format(pos=positions[p], year=self.season)

                # retreiving HTML and converting it to soup
                r = requests.get(url_formatted)
                soup = bs(r.text, features='lxml')

                # accessing table with the data
                table = soup.find("table", class_= tableClass)

                
                # accounting for the difference in DEF headers
                if positions[p] == "DST":
                    cols = ["pos", "team","name"]
                else:
                    cols = ["playerId", "name", "shortName", "pos", "team"]
                
                ### grabbing column names from the thead for the position. These will be used to create the temp. pos dataframe
                #retrieving column names from the HTML
                for i in table.find_all("div", class_=headerClass):
                    cols.append(''.join(filter(str.isalnum, i.text)))
                    
                # accessing the data in the body
                body = table.find("tbody")
                # looping through rows
                data = []
                for tr in body.find_all("tr", class_=tableRow):
                    # accounting for DST and populating pos as DST since it is not provided
                    if positions[p] == "DST":
                        player_data = ["DST"]
                    else:
                        player_data = []
                    
                    for td in tr.find_all("td", class_=tableD):
                        
                        if positions[p] == "DST":
                            
                            span = td.find_all("span",class_="CellLogoNameLockup")
                            
                            if span:
                                
                                for s in span:
                                    player_data.append(s.find("a")["href"].split("/")[3])
                                    player_data.append(str.strip(td.text))
                            
                            # non-span <Td>
                            else:
                                player_data.append(str.strip(td.text))
                                
                        # processing table body for all pos except DST
                        else:
                            #the player name, id, pos, and team are all in spans. the spans are not present in the stat <td>'s
                            span_short = td.find_all("span",class_="CellPlayerName--short")
                            span_long = td.find_all("span",class_="CellPlayerName--long")

                            # if the <td> has a span, the player info will be extracted
                            if span_long:

                                for s in span_long:
                                    # player Id from the href url
                                    player_data.append(s.find("a")["href"].split("/")[3])
                                    # player full name
                                    player_data.append(str.strip(s.find("a").text).replace(".", ""))

                                for s in span_short:
                                    # player short name
                                    player_data.append(s.find("a").text.replace(".", ""))
                                    #player position
                                    player_data.append(str.strip(s.find("span", class_="CellPlayerName-position").text))
                                    #player nfl team
                                    player_data.append(str.strip(s.find("span", class_="CellPlayerName-team").text))
                        
                            # non-span <Td>
                            else:
                                player_data.append(str.strip(td.text))
                    
                    # creates the list of players, each player is a list with stats
                    data.append(player_data)
                
                # converts list of list to data frame with the applicable columns pulled earlier
                pos_df = pd.DataFrame(data, columns=cols)
                
                # concats all of the position data to the master df
                df_cbs_proj = pd.concat([df_cbs_proj, pos_df], axis=0, ignore_index=True)

            df_cbs_proj.loc[:,'outlet'] = "cbs"
            df_cbs_proj.loc[:,'date'] = self.today
            df_cbs_proj.loc[:,'season'] = self.season
            df_cbs_proj.loc[:,'week'] = self.week


        if export:
            filepath = str(hf.DATA_DIR) + "/projection/cbs_proj_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_cbs_proj.to_csv(filepath, index=False)

        self.scraped_dfs['projections']['cbs'] = df_cbs_proj.copy()
        return df_cbs_proj.shape
    
    def cbs_rankings(
        self,
        inseason=False,
        export = True,
        positions = ["QB", "RB", "WR", "TE", "K", "DST", "FLEX"],
        # key class names that will be targeted
        parentDivClass = "rankings-table multi-authors hide-player-stats", # contains all expert rankings (3 tables)
        individualRankingDivClass = "experts-column triple",  # 3 of these for their 3 experts  
        authorNameAClass = "author-name",
        playersDivClass = "player-wrapper"  # the divs of interest are in here but it also includes data that is not needed 
    ):

        df_cbs_ranking = pd.DataFrame(columns=hf.ranking_columns)

        if inseason:

            url_rankings = self.scraping_urls['cbs']['inseason']['rankings']

            for pos in positions:
                time.sleep(3)    
                # retreiving HTML and converting it to soup
                url_formatted = url_rankings.format(pos=pos)
                r = requests.get(url_formatted)
                soup = bs(r.text)

                # finding the tables with rankings
                rankingTables = soup.find_all("div", class_=individualRankingDivClass)

                # looping through the 3 expert ranks that are in their own tables
                player_ranking_data = []
                if pos == "FLEX":
                    for rt in rankingTables:
                        #extracting expert name
                        expert = rt.find("a", class_=authorNameAClass).span.text

                        #looping through the divs that contain all the player level ranking data
                        ranks = rt.find("div", class_=playersDivClass)
                        for p in ranks:
                            temp = ["cbs", self.today, self.season, self.week, pos, expert]
                            try:
                                temp.append(str.strip(p.find("div", class_="rank").text))  #expert rank, number  .text
                                temp.append(str.strip(p.find("span", class_="player-name").text).replace(".", ""))  #cbs shortName  .text
                                temp.append(str.strip(p.find("a")["href"].split("/")[4])) # cbs playerId is in the url
                                temp.append(str.strip(p.find("span", class_="team position").text.split()[0])) # contains the player nfl team 
                                temp.append(str.strip(p.find("span", class_="team position").text.split()[1])) # contains the player nfl position
                                temp.append(np.nan)
                                temp.append(np.nan)
                                player_ranking_data.append(temp)
                            except:
                                continue
                    
                elif pos == "DST":
                    for rt in rankingTables:
                        #extracting expert name
                        expert = rt.find("a", class_=authorNameAClass).span.text

                        #looping through the divs that contain all the player level ranking data
                        ranks = rt.find("div", class_=playersDivClass)
                        for p in ranks:
                            temp = ["cbs", self.today, self.season, self.week, pos, expert]
                            try:
                                team = str.strip(p.find("span", class_="player-name").text)
                                temp.append(str.strip(p.find("div", class_="rank").text))  #expert rank, number  .text
                                temp.append(team)  #cbs shortName  .text
                                temp.append(str.strip(p.find("a")["href"].split("/")[4])) # cbs playerId is in the url
                                temp.append(team) # contains the player nfl team 
                                temp.append(pos) # contains the player nfl position
                                temp.append(np.nan)
                                temp.append(np.nan)
                                player_ranking_data.append(temp)
                            except:
                                continue
                
                else:
                    for rt in rankingTables:
                        #extracting expert name
                        expert = rt.find("a", class_=authorNameAClass).span.text
                        #looping through the divs that contain all the player level ranking data
                        ranks = rt.find("div", class_=playersDivClass)
                        for p in ranks:
                            temp = ["cbs", self.today, self.season, self.week, pos, expert]
                            try:
                                temp.append(str.strip(p.find("div", class_="rank").text))  #expert rank, number  .text
                                temp.append(str.strip(p.find("span", class_="player-name").text).replace(".", ""))  #cbs shortName  .text
                                temp.append(str.strip(p.find("a")["href"].split("/")[3])) # cbs playerId is in the url
                                temp.append(str.strip(p.find("span", class_="team position").text.split()[0])) # contains the player nfl team 
                                temp.append(pos) # contains the player nfl team
                                temp.append(np.nan)
                                temp.append(np.nan)
                                player_ranking_data.append(temp)
                            except:
                                continue
                    
                # creating temp dataframe that includes all 3 expert rankings for a grouping to add to the master df 
                temp_df = pd.DataFrame(player_ranking_data, columns=hf.ranking_columns)        
                df_cbs_ranking = pd.concat([df_cbs_ranking, temp_df], axis = 0, ignore_index=True)
            
            #offseason run
            else:
                url_rankings = self.scraping_urls['cbs']['offseason']['rankings']

                for pos in positions:
                    time.sleep(3)    
                    # retreiving HTML and converting it to soup
                    url_formatted = url_rankings.format(pos=pos)
                    r = requests.get(url_formatted)
                    soup = bs(r.text)

                    # finding the tables with rankings
                    rankingTables = soup.find_all("div", class_=individualRankingDivClass)
                    
                    # looping through the 3 expert ranks that are in their own tables
                    player_ranking_data = []
                    if pos == "DST":
                        for rt in rankingTables:
                            #extracting expert name
                            expert = rt.find("a", class_=authorNameAClass).span.text

                            #looping through the divs that contain all the player level ranking data
                            ranks = rt.find("div", class_=playersDivClass)
                            for p in ranks:
                                temp = ["cbs", self.today, pos, expert]
                                try:
                                    team = str.strip(p.find("span", class_="player-name").text)
                                    temp.append(str.strip(p.find("div", class_="rank").text))  #expert rank, number  .text
                                    temp.append(team)  #cbs shortName  .text
                                    temp.append(str.strip(p.find("a")["href"].split("/")[4])) # cbs playerId is in the url
                                    temp.append(team) # contains the player nfl team 
                                    temp.append(pos) # contains the player nfl position 
                                    player_ranking_data.append(temp)
                                except:
                                    continue
                    
                    else:
                        for rt in rankingTables:
                            #extracting expert name
                            expert = rt.find("a", class_=authorNameAClass).span.text

                            #looping through the divs that contain all the player level ranking data
                            ranks = rt.find("div", class_=playersDivClass)
                            for p in ranks:
                                temp = ["cbs", self.today, pos, expert]
                                try:
                                    temp.append(str.strip(p.find("div", class_="rank").text))  #expert rank, number  .text
                                    temp.append(str.strip(p.find("span", class_="player-name").text).replace(".", ""))  #cbs shortName  .text
                                    temp.append(str.strip(p.find("a")["href"].split("/")[4])) # cbs playerId is in the url
                                    temp.append(str.strip(p.find("span", class_="team position").text.split()[0])) # contains the player nfl team 
                                    temp.append(pos) # contains the player nfl team 
                                    player_ranking_data.append(temp)
                                except:
                                    continue
                        
                    # creating temp dataframe that includes all 3 expert rankings for a grouping to add to the master df 
                    temp_df = pd.DataFrame(player_ranking_data, columns=hf.ranking_columns)        
                    df_cbs_ranking = pd.concat([df_cbs_ranking, temp_df], axis = 0, ignore_index=True)


        if export:
            filepath = str(hf.DATA_DIR) + "/ranking/weekly/cbs_rank_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_cbs_ranking.to_csv(filepath, index=False)

        self.scraped_dfs['rankings']['cbs'] = df_cbs_ranking.copy()
        return df_cbs_ranking.shape

    def cbs_game_scores(
        self,
        export = True,
        url_scores = r'https://www.cbssports.com/nfl/scoreboard/all/{season}/regular/{week}/'
    ):
        
        prev_week = self.week - 1

        # creating lookup dictionaries that will be used across multiple database inserts
        try:
            # getting outlet db ids to convert the scraped names/ids
            query_outlets = "SELECT outletId, outletName  FROM outlet;"
            outletLookup = hf.query_database(query_outlets, connection_string=None)
            outletLookup = pd.Series(outletLookup.outletId.values, index=outletLookup.outletName).to_dict()
            
            # getting team db ids to convert datasource names to the ids
            query_teams = "SELECT teamId, name  FROM team;"
            teamLookup = hf.query_database(query_teams, connection_string=None)
            teamLookup = pd.Series(teamLookup.teamId.values, index=teamLookup.name).to_dict()
            
            query_all_teams = "SELECT * FROM team;"
            teams =  hf.query_database(query_all_teams, connection_string=None)
            
        except Exception as ex:
            print(ex)
            return
    
        # retreiving HTML and converting it to soup
        r = requests.get(url_scores.format(season=self.season, week=str(prev_week)))
        soup = bs(r.text, features="lxml")

        # accessing tables with the data
        tables = soup.find_all("div", class_="live-update")

        records = pd.DataFrame(columns = ["season", "week", "teamId", "wins", "losses", "ties"])
        games = pd.DataFrame(columns=[
            "season", "week", "homeTeamId", "homeQ1Pts", "homeQ2Pts", "homeQ3Pts", "homeQ4Pts", 
            "homeTotalPts", "awayTeamId", "awayQ1Pts", "awayQ2Pts", "awayQ3Pts", "awayQ4Pts", 
            "awayTotalPts", "homeWinner", "awayWinner", "tie", "winningTeamId", "gameId"
        ])

        for t in tables:
            game = t.find("tbody").find_all("tr")

            # away team 
            away = game[0]
            away_team = away.find("a")['href'].split("/")[3]
            away_record = away.find("span", class_="record").text.split("-")
            away_win = away_record[0]
            away_loss = away_record[1]
            if len(away_record) == 3:
                away_tie = away_record[2]
            else: 
                away_tie = 0
            away_scores = away.find_all("td")
            away_q1Pts = int(away_scores[1].text)
            away_q2Pts = int(away_scores[2].text)
            away_q3Pts = int(away_scores[3].text)
            away_q4Pts = int(away_scores[4].text)
            away_totalPts = int(away_scores[5].text)

            # home team 
            home = game[1]
            home_team = home.find("a")['href'].split("/")[3]
            home_record = home.find("span", class_="record").text.split("-")
            home_win = home_record[0]
            home_loss = home_record[1]
            if len(home_record) == 3:
                home_tie = home_record[2]
            else: 
                home_tie = 0
            home_scores = home.find_all("td")
            home_q1Pts = int(home_scores[1].text)
            home_q2Pts = int(home_scores[2].text)
            home_q3Pts = int(home_scores[3].text)
            home_q4Pts = int(home_scores[4].text)
            home_totalPts = int(home_scores[5].text)

            # designating winner or tie
            home_winner = 0
            away_winner = 0
            tie = 0
            winning_team = np.nan

            if home_totalPts > away_totalPts:
                home_winner = 1
                winning_team = home_team
            elif home_totalPts < away_totalPts:
                away_winner = 1
                winning_team = away_team
            else:
                tie = 1
            
            # gathering data into list for df creation and creating temp dfs to concat with main df
            home_records = [self.season, prev_week, home_team, home_win, home_loss, home_tie]
            away_records = [self.season, prev_week, away_team, away_win, away_loss, away_tie]
            game_data = [
                self.season, prev_week, home_team, home_q1Pts, home_q2Pts, home_q3Pts, home_q4Pts, 
                home_totalPts, away_team, away_q1Pts, away_q2Pts, away_q3Pts, away_q4Pts, away_totalPts,
                home_winner, away_winner, tie, winning_team
            ]

            
            temp_games = pd.DataFrame([game_data], columns=[
                "season", "week", "homeTeamId", "homeQ1Pts", "homeQ2Pts", "homeQ3Pts", "homeQ4Pts", "homeTotalPts",
                "awayTeamId", "awayQ1Pts", "awayQ2Pts", "awayQ3Pts", "awayQ4Pts", "awayTotalPts",
                "homeWinner", "awayWinner", "tie", "winningTeamId"
            ])
            
            cols = ['season', 'week', 'homeTeamId', 'awayTeamId']
            temp_games['gameId'] = temp_games[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
            temp_games = temp_games.replace("WAS", "WSH").replace("JAC", "JAX")
            
            games = pd.concat([games, temp_games])
            
            temp_records_h = pd.DataFrame([home_records], columns = ["season", "week", "teamId", "wins", "losses", "ties"])
            temp_records_a = pd.DataFrame([away_records], columns = ["season", "week", "teamId", "wins", "losses", "ties"])
            temp_records_h = pd.concat([temp_records_h, temp_records_a])
            temp_records_h = temp_records_h.replace("WAS", "WSH").replace("JAC", "JAX")
            
            records = pd.concat([records, temp_records_h])
            #records.rename(columns={'team':'teamId'})

        # LOAD TO DB
        try: 
            games['homeTeamId'] = games['homeTeamId'].map(teamLookup)
            games['awayTeamId'] = games['awayTeamId'].map(teamLookup)
            games['winningTeamId'] = games['winningTeamId'].map(teamLookup)
            records['teamId'] = records['teamId'].map(teamLookup)

            hf.export_database(records, 'weeklyrecord', connection_string=None)
            message = str(self.season) + ' ' + str(prev_week) + " weeklyrecord loaded to db\n"
            print(message)
            self.scraped_dfs['game_scores']['records'] = records

            hf.export_database(games, 'weeklyscore', connection_string=None)
            message = str(self.season) + ' ' + str(prev_week) + " weeklyscore loaded to db\n"
            print(message) 
            self.scraped_dfs['game_scores']['scores'] = games

        except Exception as ex:
            print(ex)
            return
        
        return 

    def cbs_adp(
        self,
        export = True
    ):
        cbs_adp_url = self.scraping_urls['cbs']['offseason']['adp']
        r = requests.get(cbs_adp_url)
        soup = bs(r.text, features='lxml')

        table = soup.find("table", class_="TableBase-table")
        body = table.find("tbody")

        adps = []
        for tr in body.find_all("tr"):
            temp = []
            
            data = tr.find_all("td")
            
            playerId = data[1].find("a")["href"].split("/")[3]
            shortName =  data[1].find("span", class_="CellPlayerName--short").text.split("\n")[0].replace(".", "")
            fullName =  data[1].find("span", class_="CellPlayerName--long").text.split("\n")[0].replace(".", "")
            pos = data[1].find("span", class_="CellPlayerName-position").text.strip()
            team =  data[1].find("span", class_="CellPlayerName-team").text.strip()
            
            adp = data[3].text.strip()
            
            highLow = data[4].text.split("/")
            high = highLow[0].strip()
            low = highLow[1]
            
            temp = ["cbs", self.today, playerId, fullName, shortName, pos, team, adp, high, low]
            adps.append(temp)
            
        df_cbs_adp = pd.DataFrame(adps, columns = hf.adp_columns)
        if export:
            filepath = str(hf.DATA_DIR) + "/adp/cbs_adp_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_cbs_adp.to_csv(filepath, index=False)

        self.scraped_dfs['adps']['cbs'] = df_cbs_adp.copy()
        return
    # ====================
    #       ffp
    # ====================
    def ffp_ecr_rankings(
        self,
        inseason=False,
        export = True,
        waitTime = 15,
    ):
    
        driver = hf.open_browser()
        
        df_ecr_ranks = pd.DataFrame(columns=hf.ranking_columns)

        if inseason:
            urls = self.scraping_urls['ffp']['inseason']['rankings']

            # try to close driver if there are any errors
            try:
                for i in range(len(urls)):
                    if i == 0:
                        pos = ['SUPERFLEX', 'FLEX', 'TE', 'WR', 'RB']
                        for j in pos:
                            player_ranks = []
                            driver.get(urls[i].format(j.lower()))

                            # Accepting cookies if there is a popup
                            try:
                                WebDriverWait(driver, timeout=waitTime).until(lambda d: d.find_element("xpath", "//button[contains(text(), 'Accept Cookies')]"))
                                cookies = driver.find_element("xpath", '//*[@id="onetrust-accept-btn-handler"]')
                                cookies.click()

                            except: pass

                            #driver.execute_script('videos = document.querySelectorAll("video"); for(video of videos) {video.pause()}')

                            # select drop down that defaults to Overview and selecting Ranks
                            WebDriverWait(driver, timeout=waitTime).until(lambda d: d.find_element("xpath", "//button[span[contains(text(), 'Overview')]]"))     
                            drop = driver.find_element("xpath", "//button[span[contains(text(), 'Overview')]]")
                            drop.click()

                            WebDriverWait(driver, timeout=waitTime).until(lambda d: d.find_element("xpath", "//button[div[contains(text(), 'Ranks')]]"))     
                            drop = driver.find_element("xpath", "//button[div[contains(text(), 'Ranks')]]")
                            drop.click()

                            # grab all html
                            html = driver.page_source
                            soup = bs(html, 'lxml')  #parse the html

                            table = soup.find("table", id='ranking-table').find("tbody")
                            ranks = table.find_all("tr")

                            for tr in ranks:
                                tds = tr.find_all("td")
                                #for td in tds:
                                    #print(td.text)

                                # some of the ecr defensive groups have teams in the rankings this will skip them
                                name = tds[2].text.split("(")[0].strip().replace(".", "")
                                if name in list(hf.team_map.keys()):
                                    continue

                                if j in ['SUPERFLEX', 'FLEX']:
                                    rank = tds[0].text
                                    team = tds[2].text.split("(")[1].strip().replace(")", "")
                                    player = tds[2].find("div", class_='player-cell player-cell__td')['data-player']
                                    high = tds[4].text
                                    low = tds[5].text
                                else:
                                    rank = tds[0].text
                                    team = tds[2].text.split("(")[1].strip().replace(")", "")
                                    player = tds[2].find("div", class_='player-cell player-cell__td')['data-player']
                                    high = tds[4].text
                                    low = tds[5].text


                                player_ranks.append(['fantasyPros', self.today, self.season, self.week, j, 'ecr', rank, name, player, team, np.nan,  high, low])

                            temp = pd.DataFrame(player_ranks,  columns=hf.ranking_columns)
                            df_ecr_ranks = pd.concat([df_ecr_ranks, temp])

                    if i == 1:
                        pos = ['QB', 'K', 'DST', 'IDP', 'DL' ,'LB', 'DB']
                        for j in pos:
                            player_ranks = []
                            driver.get(urls[i].format(j.lower()))

                            # Accepting cookies if there is a popup
                            try:
                                WebDriverWait(driver, timeout=waitTime).until(lambda d: d.find_element("xpath", "//button[contains(text(), 'Accept')]"))
                                cookies = driver.find_element("xpath", "//button[contains(text(), 'Accept')]")
                                cookies.click()

                            except: pass

                            # select drop down that defaults to Overview and selecting Ranks
                                                                        # actual buttom xpath //*[@id="onetrust-accept-btn-handler"]
                            WebDriverWait(driver, timeout=waitTime).until(lambda d: d.find_element("xpath", "//button[span[contains(text(), 'Overview')]]"))
                            
                            drop = driver.find_element("xpath", "//button[span[contains(text(), 'Overview')]]")

                            drop.click()

                            WebDriverWait(driver, timeout=waitTime).until(lambda d: d.find_element("xpath", "//button[div[contains(text(), 'Ranks')]]"))     
                            drop = driver.find_element("xpath", "//button[div[contains(text(), 'Ranks')]]")
                            drop.click()

                            # grab all html
                            html = driver.page_source
                            soup = bs(html, 'lxml')  #parse the html

                            table = soup.find("table", id='ranking-table').find("tbody")
                            ranks = table.find_all("tr")

                            for tr in ranks:
                                tds = tr.find_all("td")

                                # some of the ecr defensive groups have teams in the rankings this will skip them
                                name = tds[2].text.split("(")[0].strip().replace(".", "")
                                if name in list(hf.team_map.keys()):
                                    continue

                                if j == 'IDP':
                                    rank = tds[0].text
                                    team = tds[2].text.split("(")[1].strip().replace(")", "")
                                    player = tds[2].find("div", class_='player-cell player-cell__td')['data-player']
                                    high = tds[4].text
                                    low = tds[5].text

                                else:
                                    rank = tds[0].text
                                    team = tds[2].text.split("(")[1].strip().replace(")", "")
                                    player = tds[2].find("div", class_='player-cell player-cell__td')['data-player']
                                    high = tds[3].text
                                    low = tds[4].text

                                player_ranks.append(['fantasyPros', self.today, self.season, self.week, j, 'ecr', rank, name, player, team, np.nan,  high, low])

                            temp = pd.DataFrame(player_ranks, columns=hf.ranking_columns)
                            df_ecr_ranks = pd.concat([df_ecr_ranks, temp])
            except Exception as ex:
                print(ex)
                driver.close()
                
            driver.close()

        # OFFSEASON
        else:
            urls = self.scraping_urls['ffp']['offseason']['rankings']
            data = []
            
            # grab all html
            driver.get(urls)
            time.sleep(3)
             # Accepting cookies if there is a popup
            try:
                WebDriverWait(driver, timeout=waitTime).until(lambda d: d.find_element("xpath", "//button[contains(text(), 'Accept Cookies')]"))
                cookies = driver.find_element("xpath", '//*[@id="onetrust-accept-btn-handler"]')
                cookies.click()

            except: pass
            html = driver.page_source
            soup = bs(html, 'lxml')  #parse the html
            driver.close()

            table = soup.find("table", id='ranking-table')
            tbody = table.find("tbody")
            ranks = tbody.find_all("tr")
            for tr in ranks:
                tds = tr.find_all("td")
                # skip the rows that are sub-headers with no data
                if len(tds) < 7:
                    continue
                else:
                    rank = int(tds[0].text)
                    player_data = tds[2]
                    player_a_tag = player_data.find('a')
                    player_span_tag = player_data.find('span', class_='player-cell-team')
                    pid = player_a_tag['fp-player-id']
                    name = player_a_tag['fp-player-name']
                    team = player_span_tag.text.strip('()') 
                    pos = re.sub(r'\d+', '', tds[3].text)

                    #[
                    # 'outlet','date','season','week', 'group','expert','rank',
                    # 'name','playerId','team','pos','high','low'
                    # ]
                    # outlet and expert Are my id for FFP in my db
                    temp = [
                        'fantasyPros', self.today, self.season, self.week, pos, 'ecr', rank, name,
                        pid, team, pos, np.nan, np.nan
                    ]

                    data.append(temp)

            temp = pd.DataFrame(data, columns=hf.ranking_columns)
            df_ecr_ranks = pd.concat([df_ecr_ranks, temp]) 

        if export:
            filepath = str(hf.DATA_DIR) + "/ranking/fpEcr_rank_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_ecr_ranks.to_csv(filepath, index=False)

        self.scraped_dfs['rankings']['ffp_ecr'] = df_ecr_ranks.copy()
        return df_ecr_ranks.shape
    
    def ffp_adp(
        self,
        export = True
    ):

        fp_adp_url = self.scraping_urls['ffp']['offseason']['adp']
        r = requests.get(fp_adp_url)
        soup = bs(r.text, features='lxml')

        table = soup.find_all("table")[0].find("tbody")

        adps = []
        for tr in table.find_all("tr"):
            
            temp = []
            data = tr.find_all("td")
            
            fullName = data[1].find("a", class_="player-name").text.replace(".", "")
            playerId = data[1].find("a", class_="fp-player-link")
            
            for c in data[1].find_all(class_=True):
                classes = c['class']
                if len(classes) > 1:
                    for i in classes:
                        if "id" in i:
                            playerId = i.split("id-")[1]
                            
            
                
            pos = re.search(pattern = r"\D*", string=data[2].text)[0]
            
            if pos == 'DST':
                team = fullName
            else:
                try:
                    team = data[1].find("small").text
                except:
                    team = "FA"
            
            
            ###
            # adding an entry for each sites adp. they are their own records
            yahoo = data[3].text
            temp = ["yahoo", self.today, playerId, fullName, np.nan, pos, team, yahoo, np.nan, np.nan]
            adps.append(temp)
            
            fantrax = data[4].text
            temp = ["fantrax", self.today, playerId, fullName, np.nan, pos, team, fantrax, np.nan, np.nan]
            adps.append(temp)
            
            ffc = data[5].text
            temp = ["ffc", self.today, playerId, fullName, np.nan, pos, team, ffc, np.nan, np.nan]
            adps.append(temp)
            
            sleeper = data[6].text
            temp = ["sleeper", self.today, playerId, fullName, np.nan, pos, team, sleeper, np.nan, np.nan]
            adps.append(temp)
            
            #avg = data[7].text
            
            
        df_fp_adp = pd.DataFrame(adps, columns=hf.adp_columns)
        
        
        if export:
            filepath = str(hf.DATA_DIR) + "//adp//fp_adp_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_fp_adp.to_csv(filepath, index=False)

        self.scraped_dfs['adps']['ffp'] = df_fp_adp.copy()
        return df_fp_adp.shape
    # ====================
    #       espn
    # ====================
    def espn_projections(
        self,
        inseason=False,
        export = True
    ):
        # have to loop through pages to grab all players, all the zero projections players arent really needed
        stop_at_page = 13

        df_espn_proj = pd.DataFrame(columns = hf.projection_columns)
        
        driver = hf.open_browser()
        if inseason:

            url_espn_proj = self.scraping_urls['espn']['inseason']['projections']
            driver.get(url_espn_proj) 
            # sleep to let the html load
            time.sleep(10)


            try:
                # changing to the desired projection view
                button = driver.find_element(By.XPATH, "//button[@class='Button Button--filter player--filters__projections-button']")
                button.click()
                time.sleep(5)
                
                # grabs the entire pages html
                html = driver.execute_script("return document.body.innerHTML")
                soup = bs(html, features='lxml')
                
                # grabbing the number of pages there are in the projections
                pagenation_list = soup.find("div", class_="Pagination__wrap overflow-x-auto")
                pages = pagenation_list.find_all("li")
                last_page = pages[-1].text
                
            except Exception as ex:
                print(ex)
                driver.close()
                
            espn_player_proj_player = []
            page_count1 = 0
            page_count2 = 0

            for page in range(1, stop_at_page):  #int(last_page)+1):
                try:
                    html = driver.execute_script("return document.body.innerHTML")
                    soup = bs(html, features='lxml')

                    # grabbing the projection tables
                    tables = soup.find_all("table")
                    
                    # the player info table
                    for tr in tables[0].find_all("tr"):
                        for td in tr:
                            if td.find("a", class_="AnchorLink link clr-link pointer"):
                                
                                #grabs the ESPN player id from the image url
                                playerId = td.find("img")['src'].split("/")[-1].split(".")[0]
                                #dst has player ID as the team abbreviation. This catches it
                                try:
                                    int(playerId)
                                    name = td.find("a", class_="AnchorLink link clr-link pointer").text.replace(".", "")
                                except:
                                    playerId = ""
                                    name = td.find("a", class_="AnchorLink link clr-link pointer").text.replace(" D/ST", "")
                                    
                                position = td.find("span", class_="playerinfo__playerpos ttu").text.replace("/","").split(",")[0]
                                team = td.find("span", class_="playerinfo__playerteam").text.upper()

                                espn_player_proj_player.append(["espn", self.today, self.season, self.week, playerId, name, np.nan, position, team, np.nan])


                    # the stat projection table
                    for tr in tables[1].find_all("tr",class_="Table__TR Table__TR--lg Table__odd"):
                        comp_att = tr.find("div", {"title":"Each Pass Completed & Each Pass Attempted"}).text.split("/")
                        pass_comps = comp_att[0]
                        pass_atts = comp_att[1]
                        pass_yds = tr.find("div", {"title":"Passing Yards"}).text
                        pass_tds = tr.find("div", {"title":"TD Pass"}).text
                        ints = tr.find("div", {"title":"Interceptions Thrown"}).text
                        rush_atts = tr.find("div", {"title":"Rushing Attempts"}).text
                        rush_yds = tr.find("div", {"title":"Rushing Yards"}).text
                        rush_tds = tr.find("div", {"title":"TD Rush"}).text
                        rec = tr.find("div", {"title":"Each reception"}).text
                        rec_yds = tr.find("div", {"title":"Receiving Yards"}).text
                        rec_tds = tr.find("div", {"title":"TD Reception"}).text
                        rec_trgts = tr.find("div", {"title":"Receiving Target"}).text
                        
                        espn_player_proj_player[page_count1].extend([pass_atts, pass_comps,pass_yds, 0, pass_tds,
                                                                    ints, 0, rush_atts,rush_yds,0, rush_tds,rec_trgts,rec,rec_yds,0,0,rec_tds,
                                                                    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
                        page_count1 += 1
                
                    # the fantasy points table
                                                    
                    for tr in tables[2].find_all("tr",class_="Table__TR Table__TR--lg Table__odd"):
                        for div in tr.find_all("div"):
                            # some of the free agents/retired players don't have div["title"] need to catch them with try
                            try:
                                if 'point' in div['title']:
                                    total_ff_pts = div.find("span").text
                                    avg_ff_pts = 0
                            except:
                                total_ff_pts = 0
                                avg_ff_pts = 0

                        espn_player_proj_player[page_count2].extend([total_ff_pts, avg_ff_pts])
                        page_count2 += 1
                    
                    #checks for last page
                    
                    if page < int(last_page):
                        # jumping to the next page
                        nextButton = driver.find_element(By.XPATH, "//button[@class='Button Button--default Button--icon-noLabel Pagination__Button Pagination__Button--next']")
                        nextButton.click()
                        time.sleep(10)
                        
                except Exception as ex:
                    print(ex)
                    driver.close()

            try:
                driver.close()
            except:
                pass

            # creating df from gathered data to merge into final df that matches the cbs structure
            df_espn_proj = pd.DataFrame(espn_player_proj_player, columns = hf.projection_columns).replace("--", 0)

            #final espn projections data
            #df_espn_proj = pd.concat([df_espn_proj, temp_proj]).replace("--", 0)
        
        # OFFSEASON
        else:
            url_espn_proj = self.scraping_urls['espn']['offseason']['projections']
            driver.get(url_espn_proj) 
            # sleep to let the html load
            time.sleep(10)

            try:
                # changing to the desired projection view
                button = driver.find_element(By.XPATH, "//button[@class='Button Button--filter player--filters__projections-button']")
                button.click()
                time.sleep(5)
                
                # grabs the entire pages html
                html = driver.execute_script("return document.body.innerHTML")
                soup = bs(html, features='lxml')
                
                # grabbing the number of pages there are in the projections
                pagenation_list = soup.find("div", class_="Pagination__wrap overflow-x-auto")
                pages = pagenation_list.find_all("li")
                last_page = pages[-1].text
                
            except Exception as ex:
                print(ex)
                driver.close()
                

            espn_player_proj_player = []
            page_count1 = 0
            page_count2 = 0

            for page in range(1, stop_at_page):
                try:
                    html = driver.execute_script("return document.body.innerHTML")
                    soup = bs(html, features='lxml')

                    # grabbing the projection tables
                    tables = soup.find_all("table")
                    
                    # the player info table
                    for tr in tables[0].find_all("tr"):
                        for td in tr:
                            if td.find("a", class_="AnchorLink link clr-link pointer"):
                                #grabs the ESPN player id from the image url
                                playerId = td.find("img")['src'].split("/")[-1].split(".")[0]
                                
                                try:
                                    int(playerId)
                                    name = td.find("a", class_="AnchorLink link clr-link pointer").text.replace({".":"", " D/ST":""})
                                except:
                                    playerId = ""
                                    name = td.find("a", class_="AnchorLink link clr-link pointer").text.replace(" D/ST", "")
                                 
                                position = td.find("span", class_="playerinfo__playerpos ttu").text
                                team = td.find("span", class_="playerinfo__playerteam").text

                                espn_player_proj_player.append(["espn", self.today, self.season, self.week, playerId, name, np.nan, position, team, np.nan])


                    # the stat projection table
                    for tr in tables[1].find_all("tr",class_="Table__TR Table__TR--lg Table__odd"):
                        comp_att = tr.find("div", {"title":"Each Pass Completed & Each Pass Attempted"}).text.split("/")
                        pass_comps = comp_att[0]
                        pass_atts = comp_att[1]
                        pass_yds = tr.find("div", {"title":"Passing Yards"}).text
                        pass_tds = tr.find("div", {"title":"TD Pass"}).text
                        ints = tr.find("div", {"title":"Interceptions Thrown"}).text
                        rush_atts = tr.find("div", {"title":"Rushing Attempts"}).text
                        rush_yds = tr.find("div", {"title":"Rushing Yards"}).text
                        rush_tds = tr.find("div", {"title":"TD Rush"}).text
                        rec = tr.find("div", {"title":"Each reception"}).text
                        rec_yds = tr.find("div", {"title":"Receiving Yards"}).text
                        rec_tds = tr.find("div", {"title":"TD Reception"}).text
                        rec_trgts = tr.find("div", {"title":"Receiving Target"}).text
                        
                        espn_player_proj_player[page_count1].extend([pass_atts, pass_comps,pass_yds, 0, pass_tds,
                                                                    ints, 0, rush_atts,rush_yds,0, rush_tds,rec_trgts,rec,rec_yds,0,0,rec_tds,
                                                                    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
                        page_count1 += 1
                
                    # the fantasy points table
                    for tr in tables[2].find_all("tr",class_="Table__TR Table__TR--lg Table__odd"):
                        for div in tr.find_all("div"):
                            # some of the free agents/retired players don't have div["title"] need to catch them with try
                            try:
                                if 'point' in div['title']:
                                    total_ff_pts = div.find("span").text
                                else:
                                    avg_ff_pts = div.find("span").text
                            except:
                                total_ff_pts = 0
                                avg_ff_pts = 0

                        espn_player_proj_player[page_count2].extend([total_ff_pts, avg_ff_pts])
                        page_count2 += 1
                    
                    #checks for last page
                    
                    if page < int(last_page):
                        # jumping to the next page
                        nextButton = driver.find_element(By.XPATH, "//button[@class='Button Button--default Button--icon-noLabel Pagination__Button Pagination__Button--next']")
                        nextButton.click()
                        time.sleep(10)
                        
                except Exception as ex:
                    print(ex)
                    driver.close()

            try:
                driver.close()
            except:
                pass

            # creating df from gathered data to merge into final df that matches the cbs structure
            df_espn_proj = pd.DataFrame(espn_player_proj_player, columns = hf.projection_columns).replace("--", 0)
                                    
            #final espn projections data
            #df_espn_proj = pd.concat([df_espn_proj, temp_proj])

        if export:
            filepath = str(hf.DATA_DIR) + "/projection/espn_proj_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_espn_proj.to_csv(filepath, index=False)

        self.scraped_dfs['projections']['espn'] = df_espn_proj.copy()
        return df_espn_proj.shape      

    def espn_rankings(
        self,
        inseason=False,
        export = True
    ):
        # final dataframe structure to hosue all the rankings
        df_espn_ranking = pd.DataFrame(columns=hf.ranking_columns)

        driver = hf.open_browser()

        if inseason:
            espn_ranking_urls = self.scraping_urls['espn']['inseason']['rankings']
            #try to closed driver if there are any errors
            try:
                # looping through the urls to aggregate the rankings
                for group, url in espn_ranking_urls.items():
                    # opening the webpage and allowing the scripts to load for the HTML to be accessed

                    url_espn_formatted = url.format(season=self.season)
                    driver.get(url_espn_formatted)
                    time.sleep(10)

                    # grabs the entire pages html
                    html = driver.execute_script("return document.body.innerHTML")
                    soup = bs(html, features='lxml')

                    # this will hold a list of list. One list will be a players rank for a single expert
                    player_ranks = []

                    # IDP page has 3 separate tables for positions instead of a single position on the page and a single table handled in the else below
                    if group == "IDP":
                        ranking_tables = soup.find_all("table", class_="inline-table rankings-table fullWidth sortable")
                        count = 0 # hard coded the positions based on the which table the site holds them in

                        # 3 tables for the 3 IDPs  DL, LB, DB
                        for ranking_table in ranking_tables:

                            # retrieves the expert names and the order they are listed
                            expert_names_html = ranking_table.find("thead").find_all("th")
                            expert_names = []
                            for tr in range(2, len(expert_names_html)-1):
                                expert_names.append(expert_names_html[tr].text)

                            player_ranks_html = ranking_table.find("tbody").find_all("tr")#, class_="")
                            for tr in player_ranks_html:

                                tds = tr.find_all("td")

                                playerId = tds[0].find("a")["data-player-id"]
                                name = tds[0].find("a").text.replace(".", "")

                                if count == 0:
                                    POS = "DL"
                                elif count == 1:
                                    POS = "LB"
                                elif count == 2:
                                    POS = "DB"

                                # try block to handle injury designations that the site puts in the same text as the team name
                                try:
                                    #if there is a injury designation, it retrieves it and then removes it from the team name
                                    injury = tds[0].find_all("div", class_="rank")[0].find("span").text
                                    if len(injury) > 1:  # Accounts for suspended tag "SSPD"
                                        team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-4]
                                    else:
                                        team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-1]

                                except:
                                    team = tds[0].find("div", class_="rank").text.split(",")[1].strip().upper()

                                for i in range(len(expert_names)):

                                    # expert name from the list generated from thead
                                    expert = expert_names[i]
                                    # position of the expert ranking column in tbody
                                    idx = i + 2

                                    # retrieves the expert rank from tbody rows
                                    exRank = pd.to_numeric(tds[idx].text, errors='coerce')

                                    player_ranks.append(["espn", self.today, self.season, self.week, POS, expert, exRank, name, playerId, team,  POS, np.nan, np.nan])

                            count += 1


                    # for position specific rankings
                    else:

                        ranking_table = soup.find("table", class_="inline-table rankings-table fullWidth sortable")
                        #driver.close()

                        # retrieves the expert names and the order they are listed
                        expert_names_html = ranking_table.find("thead").find_all("th")
                        expert_names = []
                        for tr in range(2, len(expert_names_html)-1):
                            expert_names.append(expert_names_html[tr].text)

                        player_ranks_html = ranking_table.find("tbody").find_all("tr")#, class_="")
                        for tr in player_ranks_html:

                            tds = tr.find_all("td")

                            playerId = tds[0].find("a")["data-player-id"]
                            if group == "DST":
                                name = tds[0].find("a").text.split()[0].replace(".", "")
                            else:
                                name = tds[0].find("a").text.replace(".", "")

                            POS = group

                            #team = tds[0].find("div", class_="rank").text.split(",")[1].strip().upper()
                            # try block to handle injury designations that the site puts in the same text as the team name
                            try:
                                #if there is a injury designation, it retrieves it and then removes it from the team name
                                injury = tds[0].find_all("div", class_="rank")[0].find("span").text
                                if len(injury) > 1:  # Accounts for suspended tag "SSPD"
                                    team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-4]
                                else:
                                    team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-1]

                            except:
                                team = tds[0].find("div", class_="rank").text.split(",")[1].strip().upper()

                            for i in range(len(expert_names)-1):

                                # expert name from the list generated from thead
                                expert = expert_names[i]
                                # position of the expert ranking column in tbody
                                idx = i + 2

                                # retrieves the expert rank from tbody rows
                                exRank = pd.to_numeric(tds[idx].text, errors='coerce')

                                player_ranks.append(["espn", self.today, self.season, self.week, group, expert, exRank, name, playerId, team,  POS, np.nan, np.nan])

                    temp_df = pd.DataFrame(player_ranks, columns=hf.ranking_columns)
                    df_espn_ranking = pd.concat([df_espn_ranking, temp_df], axis = 0, ignore_index=True)
                
            except Exception as ex:
                print(ex)
                driver.close()    
                
            driver.close() 
        
        # OFFSEASON
        else:
            espn_ranking_urls = self.scraping_urls['espn']['offseason']['rankings']

            # looping through the urls to aggregate the rankings
            for group, url in espn_ranking_urls.items():
                # opening the webpage and allowing the scripts to load for the HTML to be accessed
                
                url_espn_formatted = url
                driver.get(url_espn_formatted)
                time.sleep(10)

                # grabs the entire pages html
                html = driver.execute_script("return document.body.innerHTML")
                soup = bs(html, features='lxml')
                
                # this will hold a list of list. One list will be a players rank for a single expert
                player_ranks = []
                
                # IDP page has 3 separate tables for positions instead of a single position on the page and a single table handled in the else below
                if group == "IDP":
                    ranking_tables = soup.find_all("table", class_="inline-table rankings-table fullWidth sortable")
                    count = 0 # hard coded the positions based on the which table the site holds them in
                    
                    # 3 tables for the 3 IDPs  DL, LB, DB
                    for ranking_table in ranking_tables:

                        # retrieves the expert names and the order they are listed
                        expert_names_html = ranking_table.find("thead").find_all("th")
                        expert_names = []
                        for tr in range(2, len(expert_names_html)):
                            expert_names.append(expert_names_html[tr].text)

                        player_ranks_html = ranking_table.find("tbody").find_all("tr")#, class_="")
                        for tr in player_ranks_html:

                            tds = tr.find_all("td")

                            playerId = tds[0].find("a")["data-player-id"]
                            name = tds[0].find("a").text.replace(".", "")

                            if count == 0:
                                POS = "DL"
                            elif count == 1:
                                POS = "LB"
                            elif count == 2:
                                POS = "DB"

                            # try block to handle injury designations that the site puts in the same text as the team name
                            try:
                                #if there is a injury designation, it retrieves it and then removes it from the team name
                                injury = tds[0].find_all("div", class_="rank")[0].find("span").text
                                if len(injury) > 1:  # Accounts for suspended tag "SSPD"
                                    team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-4]
                                else:
                                    team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-1]

                            except:
                                team = tds[0].find("div", class_="rank").text.split(",")[1].strip().upper()

                            for i in range(len(expert_names)):

                                # expert name from the list generated from thead
                                expert = expert_names[i]
                                # position of the expert ranking column in tbody
                                idx = i + 2

                                # retrieves the expert rank from tbody rows
                                exRank = pd.to_numeric(tds[idx].text, errors='coerce')

                                player_ranks.append(["espn", self.today, self.season, self.week, POS, expert, exRank, name, playerId, team,  POS, np.nan, np.nan])

                        count += 1
                
                
                # for position specific rankings
                else:
                
                    ranking_table = soup.find("table", class_="inline-table rankings-table fullWidth sortable")
                    #driver.close()
                    
                    # retrieves the expert names and the order they are listed
                    expert_names_html = ranking_table.find("thead").find_all("th")
                    expert_names = []
                    for tr in range(2, len(expert_names_html)):
                        expert_names.append(expert_names_html[tr].text)

                    player_ranks_html = ranking_table.find("tbody").find_all("tr")#, class_="")
                    for tr in player_ranks_html:

                        tds = tr.find_all("td")

                        playerId = tds[0].find("a")["data-player-id"]
                        if group == "DST":
                            name = tds[0].find("a").text.split()[0]
                        else:
                            name = tds[0].find("a").text.replace(".", "")
                            
                        POS = group
                        
                        #team = tds[0].find("div", class_="rank").text.split(",")[1].strip().upper()
                        # try block to handle injury designations that the site puts in the same text as the team name
                        try:
                            #if there is a injury designation, it retrieves it and then removes it from the team name
                            injury = tds[0].find_all("div", class_="rank")[0].find("span").text
                            if len(injury) > 1:  # Accounts for suspended tag "SSPD"
                                team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-4]
                            else:
                                team = tds[0].find_all("div", class_="rank")[0].text.split(",")[1].strip().upper()[:-1]

                        except:
                            team = tds[0].find("div", class_="rank").text.split(",")[1].strip().upper()

                        for i in range(len(expert_names)-1):
                            
                            # expert name from the list generated from thead
                            expert = expert_names[i]
                            # position of the expert ranking column in tbody
                            idx = i + 2

                            # retrieves the expert rank from tbody rows
                            exRank = pd.to_numeric(tds[idx].text, errors='coerce')

                            player_ranks.append(["espn", self.today, self.season, self.week, POS, expert, exRank, name, playerId, team,  POS, np.nan, np.nan])

                
                temp_df = pd.DataFrame(player_ranks, columns=hf.ranking_columns)
                df_espn_ranking = pd.concat([df_espn_ranking, temp_df], axis = 0, ignore_index=True)
            driver.close()
        
        if export:
            filepath = str(hf.DATA_DIR) + "/ranking/espn_rank_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_espn_ranking.to_csv(filepath, index=False)

        self.scraped_dfs['rankings']['espn'] = df_espn_ranking.copy()
        return df_espn_ranking.shape

    def espn_adp(
        self,
        export = True
    ):

        espn_adp_url = self.scraping_urls['espn']['offseason']['adp']
        driver = hf.open_browser()
        driver.get(espn_adp_url) 
        # sleep to let the html load
        time.sleep(10)

        html = driver.execute_script("return document.body.innerHTML")
        soup = bs(html, features='lxml')

        table = soup.find("tbody", class_="Table__TBODY")

        adps = []

        for n in range(10):
            
            for tr in table.find_all("tr"):
                temp = []
                data = tr.find_all("td")

                fullName = data[1].find("a", class_="AnchorLink link clr-link pointer").text.replace(".", "")
                pos = data[1].find("span", class_="playerinfo__playerpos").text.replace("/","")
                try:
                    team = data[1].find("span", class_="playerinfo__playerteam").text
                except:
                    team = "FA"
                adp = data[2].text
                
                if pos == "DST":
                    playerId = ""
                else:
                    playerId  = data[1].find('img', src=True)['src'].split("/")[10].split(".")[0]

                temp = ["espn", self.today, playerId, fullName, np.nan, pos, team, adp, np.nan, np.nan]
                adps.append(temp)
            
            # looping over the pages for ADP
            button = driver.find_element(By.XPATH, "//button[@class='Button Button--default Button--icon-noLabel Pagination__Button Pagination__Button--next']")
            button.click()
            time.sleep(10)
            
            # grabs the entire pages html for the new page and sets it for the next scrap iteration
            html = driver.execute_script("return document.body.innerHTML")
            soup = bs(html, features='lxml')
            
            table = soup.find("tbody", class_="Table__TBODY")

        driver.close()
        df_espn_adp = pd.DataFrame(adps, columns=hf.adp_columns)
        
        if export:
            filepath = str(hf.DATA_DIR) + "//adp//espn_adp_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_espn_adp.to_csv(filepath, index=False)

        self.scraped_dfs['adps']['espn'] = df_espn_adp.copy()
        return df_espn_adp.shape
    
    # ====================
    #       nfl
    # ====================
    def nfl_projections(
        self,
        inseason=False,
        export = True
    ):
        
        # position=  0:QB,RB,WR,TE  7:Kicker, 8:D

        df_nfl_proj = pd.DataFrame(columns=hf.projection_columns)
        player_data = []

        if inseason:
            nfl_proj_url = self.scraping_urls['nfl']['inseason']['projections']

            # count will be updated to the player count after the first page load 
            # this is being used to avoid loading more pages than needed
            count = 3000

            #looping through the 3 URLs, the site has QB,RB,WR,TE combined in a single list and then K and D on their own pages
            for i in range(3):
                if i == 0:  # this will handle the offensive players
                    while count > 25:
                        
                        # this grabs the first page, else will handle all others
                        if count == 3000:
                            time.sleep(1)
                            r = requests.get(nfl_proj_url[0].format(offset=1, season=self.season, week=self.week))
                            soup = bs(r.text, features='lxml')

                            # grabs the number of players with projections on the site. pagenated at 25 a page
                            player_count = int(soup.find("span", class_="paginationTitle").text.split("of")[-1].strip())
                            count = player_count

                            table = soup.find_all("table", class_="tableType-player hasGroups")

                            body_trs = table[0].find("tbody").find_all("tr")
                            
                            for tr in body_trs:
                                data = tr.find_all("td")

                                firstColA = data[0].find('a')
                                playerId = firstColA['href'].split("=")[2]
                                fullName = firstColA.text.strip().replace(".", "")

                                posAndTeam = data[0].find('em').text.split("-")
                                pos = posAndTeam[0].strip()
                                try:
                                    team = posAndTeam[1].strip()
                                except:
                                    team = "FA"

                                
                                PassingYards = data[2].text
                                TouchdownsPasses = data[3].text
                                InterceptionsThrown = data[4].text
                                RushingYards = data[5].text
                                RushingTouchdowns = data[6].text
                                Receptions = data[7].text
                                ReceivingYards = data[8].text
                                ReceivingTouchdowns = data[9].text
                                retTd = data[10].text
                                fumTd = data[11].text
                                twoPt= data[12].text
                                FumblesLost = data[13].text
                                FantasyPoints = data[14].text

                                temp = ["nfl", self.today, self.season, self.week, playerId,fullName,np.nan,pos,team,0,0,0,PassingYards,0,TouchdownsPasses, InterceptionsThrown,
                                        0,0,RushingYards,0,RushingTouchdowns,0,Receptions,ReceivingYards,0,0,ReceivingTouchdowns,
                                        FumblesLost,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,twoPt,FantasyPoints,0]
                                player_data.append(temp)

                        else:
                            for j in range(26, player_count, 25):
                                time.sleep(1)
                                r = requests.get(nfl_proj_url[0].format(offset=j, season=self.season, week=self.week))
                                soup = bs(r.text, features='lxml')
                                table = soup.find_all("table", class_="tableType-player hasGroups")
                                body_trs = table[0].find("tbody").find_all("tr")

                                for tr in body_trs:
                                    data = tr.find_all("td")

                                    firstColA = data[0].find('a')
                                    playerId = firstColA['href'].split("=")[2]
                                    fullName = firstColA.text.strip().replace(".", "")

                                    posAndTeam = data[0].find('em').text.split("-")
                                    pos = posAndTeam[0].strip()
                                    try:
                                        team = posAndTeam[1].strip()
                                    except:
                                        team = "FA"

                                    PassingYards = data[2].text
                                    TouchdownsPasses = data[3].text
                                    InterceptionsThrown = data[4].text
                                    RushingYards = data[5].text
                                    RushingTouchdowns = data[6].text
                                    Receptions = data[7].text
                                    ReceivingYards = data[8].text
                                    ReceivingTouchdowns = data[9].text
                                    retTd = data[10].text
                                    fumTd = data[11].text
                                    twoPt= data[12].text
                                    FumblesLost = data[13].text
                                    FantasyPoints = data[14].text

                                    temp = ["nfl", self.today, self.season, self.week, playerId,fullName,np.nan,pos,team,0,0,0,PassingYards,0,TouchdownsPasses, InterceptionsThrown,
                                            0,0,RushingYards,0,RushingTouchdowns,0,Receptions,ReceivingYards,0,0,ReceivingTouchdowns,
                                            FumblesLost,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,twoPt,FantasyPoints,0]
                                    player_data.append(temp)

                                count -= 25
                                
                else: # this will handle K and D
                    for j in range(2):  
                        
                        time.sleep(1)
                        r = requests.get(nfl_proj_url[i].format(offset=j*25+1, season=self.season, week=self.week))  # k and d only have 2 pages, j *25 + 1 handles the url offset that pagenates
                        soup = bs(r.text, features='lxml')

                        table = soup.find_all("table", class_="tableType-player hasGroups")

                        body_trs = table[0].find("tbody").find_all("tr")

                        for tr in body_trs:
                            data = tr.find_all("td")
                            temp = []
                            
                            firstColA = data[0].find('a')
                            playerId = firstColA['href'].split("=")[2]
                            fullName = firstColA.text.strip().replace(".", "")

                            posAndTeam = data[0].find('em').text.split("-")
                            
                            
                            if i == 1:  # K url
                                
                                pos = posAndTeam[0].strip()
                                try:
                                    team = posAndTeam[1].strip()
                                except:
                                    team = "FA"
                                    
                                xpMade = data[2].text
                                made0_19 = float(data[3].text.replace("-", "0"))
                                made20_29 = float(data[4].text.replace("-", "0"))
                                made30_39 = float(data[5].text.replace("-", "0"))
                                made40_49 = float(data[6].text.replace("-", "0"))
                                made50 = float(data[7].text.replace("-", "0"))
                                fgMade = made0_19 + made20_29 + made30_39 + made40_49 + made50
                                FantasyPoints = data[8].text

                                temp = ["nfl", self.today, self.season, self.week, playerId,fullName,np.nan,pos,team,0,0,0,0,0,0, 0,
                                                0,0,0,0,0,0,0,0,0,0,0,0,
                                                fgMade,0,0,made0_19,0,made20_29,0,made30_39,0,made40_49,0,made50,0,xpMade,0,0,
                                                0,0,0,0,0,0,0,0,0,0,0,0,0,0,FantasyPoints,0]
                                player_data.append(temp)    
                                    
                            else: # D url
                                pos = 'DST'
                                team = fullName
                                sacks = data[2].text
                                interceptions = data[3].text
                                fum = data[4].text
                                safety = data[5].text
                                defTd = data[6].text
                                twoPt = data[7].text
                                retTd = data[8].text
                                ptsAllowed= data[9].text
                                fantasyPts= data[10].text
                                
                                temp = ["nfl", self.today, self.season, self.week, playerId,np.nan,np.nan,pos,team,0,0,0,0,0,0,0,
                                        0,0,0,0,0,0,0,0,0,0,0,
                                        FumblesLost,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,interceptions,safety,sacks,0,fum,0,
                                        defTd, retTd,ptsAllowed,0,0,0,0,0,twoPt,fantasyPts,0]
                                
                                player_data.append(temp)
                    
            df_nfl_proj = pd.DataFrame(player_data, columns=hf.projection_columns).replace("-",0)

        # OFFSEASON
        else:
            nfl_proj_url = self.scraping_urls['nfl']['offseason']['projections']
            
            # count will be updated to the player count after the first page load 
            # this is being used to avoid loading more pages than needed
            count = 3000

            #looping through the 3 URLs, the site has QB,RB,WR,TE combined in a single list and then K and D on their own pages
            for i in range(3):
                if i == 0:  # this will handle the offensive players
                    while count > 25:
                        if count == 3000:
                            time.sleep(1)
                            r = requests.get(nfl_proj_url[0].format(offset=0, season=self.season))
                            soup = bs(r.text, features='lxml')

                            # grabs the number of players with projections on the site. pagenated at 25 a page
                            player_count = int(soup.find("span", class_="paginationTitle").text.split("of")[-1].strip())
                            count = player_count

                            table = soup.find_all("table", class_="tableType-player hasGroups")

                            body_trs = table[0].find("tbody").find_all("tr")

                            for tr in body_trs:
                                data = tr.find_all("td")

                                firstColA = data[0].find('a')
                                playerId = firstColA['href'].split("=")[2]
                                fullName = firstColA.text.strip().replace(".", "")

                                posAndTeam = data[0].find('em').text.split("-")
                                pos = posAndTeam[0].strip()
                                try:
                                    team = posAndTeam[1].strip()
                                except:
                                    team = "FA"

                                gp = data[2].text
                                PassingYards = data[3].text
                                TouchdownsPasses = data[4].text
                                InterceptionsThrown = data[5].text
                                RushingYards = data[6].text
                                RushingTouchdowns = data[7].text
                                Receptions = data[8].text
                                ReceivingYards = data[9].text
                                ReceivingTouchdowns = data[10].text
                                retTd = data[11].text
                                fumTd = data[12].text
                                twoPt= data[13].text
                                FumblesLost = data[14].text
                                FantasyPoints = data[15].text

                                temp = ["nfl", self.today, self.season, self.week, playerId,fullName,np.nan,pos,team,0,0,0,PassingYards,0,TouchdownsPasses, InterceptionsThrown,
                                        0,0,RushingYards,0,RushingTouchdowns,0,Receptions,ReceivingYards,0,0,ReceivingTouchdowns,
                                        FumblesLost,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,twoPt,FantasyPoints,0]
                                player_data.append(temp)

                        else:
                            for j in range(26, player_count, 25):
                                time.sleep(1)
                                r = requests.get(nfl_proj_url[0].format(offset=j, season=self.season))
                                soup = bs(r.text, features='lxml')
                                table = soup.find_all("table", class_="tableType-player hasGroups")
                                body_trs = table[0].find("tbody").find_all("tr")

                                for tr in body_trs:
                                    data = tr.find_all("td")

                                    firstColA = data[0].find('a')
                                    playerId = firstColA['href'].split("=")[2]
                                    fullName = firstColA.text.strip().replace(".", "")

                                    posAndTeam = data[0].find('em').text.split("-")
                                    pos = posAndTeam[0].strip()
                                    try:
                                        team = posAndTeam[1].strip()
                                    except:
                                        team = "FA"

                                    gp = data[2].text
                                    PassingYards = data[3].text
                                    TouchdownsPasses = data[4].text
                                    InterceptionsThrown = data[5].text
                                    RushingYards = data[6].text
                                    RushingTouchdowns = data[7].text
                                    Receptions = data[8].text
                                    ReceivingYards = data[9].text
                                    ReceivingTouchdowns = data[10].text
                                    retTd = data[11].text
                                    fumTd = data[12].text
                                    twoPt= data[13].text
                                    FumblesLost = data[14].text
                                    FantasyPoints = data[15].text

                                    temp = ["nfl", self.today, self.season, self.week, playerId,fullName,np.nan,pos,team,0,0,0,PassingYards,0,TouchdownsPasses, InterceptionsThrown,
                                        0,0,RushingYards,0,RushingTouchdowns,0,Receptions,ReceivingYards,0,0,ReceivingTouchdowns,
                                        FumblesLost,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,twoPt,FantasyPoints,0]
                                    player_data.append(temp)

                                count -= 25
                                
                else: # this will handle K and D
                    for j in range(2):  
                        
                        time.sleep(1)
                        r = requests.get(nfl_proj_url[i].format(offset=j*25+1, season=self.season))  # k and d only have 2 pages, j *25 + 1 handles the url offset that pagenates
                        soup = bs(r.text, features='lxml')

                        table = soup.find_all("table", class_="tableType-player hasGroups")

                        body_trs = table[0].find("tbody").find_all("tr")

                        for tr in body_trs:
                            data = tr.find_all("td")
                            temp = []
                            
                            firstColA = data[0].find('a')
                            playerId = firstColA['href'].split("=")[2]
                            fullName = firstColA.text.strip().replace(".", "")

                            posAndTeam = data[0].find('em').text.split("-")
                            pos = posAndTeam[0].strip()
                            
                            if i == 1:  # K url
                                try:
                                    team = posAndTeam[1].strip()
                                except:
                                    team = "FA"
                                    
                                gp = data[2].text
                                xpMade = data[3].text
                                made0_19 = data[4].text
                                made20_29 = data[5].text
                                made30_39 = data[6].text
                                made40_49 = data[7].text
                                made50 = data[8].text
                                fgMade = made0_19 + made20_29 + made30_39 + made40_49 + made50
                                FantasyPoints = data[9].text

                                temp = ["nfl", self.today, self.season, self.week, playerId,fullName,np.nan,pos,team,0,0,0,0,0,0, 0,
                                                0,0,0,0,0,0,0,0,0,0,0,0,
                                                fgMade,0,0,made0_19,0,made20_29,0,made30_39,0,made40_49,0,made50,0,xpMade,0,0,
                                                0,0,0,0,0,0,0,0,0,0,0,0,0,0,FantasyPoints,0]
                                player_data.append(temp)    
                                    
                            else: # D url
                                
                                team = fullName
                                gp = data[2].text
                                sacks = data[3].text
                                interceptions = data[4].text
                                fum = data[5].text
                                safety = data[6].text
                                defTd = data[7].text
                                twoPt = data[8].text
                                retTd = data[9].text
                                ptsAllowed= data[10].text
                                fantasyPts= data[11].text
                                
                                    
                                
                                temp = ["nfl", self.today, self.season, self.week, playerId,np.nan,np.nan,pos,team,0,0,0,0,0,0,0,
                                        0,0,0,0,0,0,0,0,0,0,0,
                                        FumblesLost,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,interceptions,safety,sacks,0,fum,0,
                                        defTd, retTd,ptsAllowed,0,0,0,0,0,twoPt,fantasyPts,0]
                                
                                player_data.append(temp)
                    
            df_nfl_proj = pd.DataFrame(player_data, columns=hf.projection_columns).replace("-",0)
        
        if export:
            filepath = str(hf.DATA_DIR) + "/projection/nfl_proj_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_nfl_proj.to_csv(filepath, index=False)

        self.scraped_dfs['projections']['nfl'] = df_nfl_proj.copy()
        return df_nfl_proj.shape

    def nfl_rankings(
        self,
        inseason=False,
        export = True
    ):
        df_nfl_ranking = pd.DataFrame(columns=hf.ranking_columns)

        if inseason:
            nfl_rank_url = self.scraping_urls['nfl']['inseason']['rankings']
            for k,v in nfl_rank_url.items():
                
                time.sleep(1)
                r = requests.get(nfl_rank_url[k].format(week=self.week))
                soup = bs(r.text, features='lxml')

                # grabs the number of players with projections on the site. pagenated at 25 a page
                rank_table = soup.find("table", class_="tableType-player noGroups").find("tbody")

                player_ranks = []
                for tr in rank_table.find_all("tr"):
                    player_data = []
                    td = tr.find_all("td")

                    pos_raw = td[0].text
                    if pos_raw == '--':
                        pos_raw = pos_raw.replace('--',0)
                    pos_rank = int(pos_raw)
                    playerId = int(td[1].find("a")['href'].split("=")[-1])
                    full_name = td[1].find("a").text.replace(".", "")
                    
                    pos = td[1].find("em").text.split("-")[0].strip()
                    
                    if k == "DST":
                        team = ""
                        pos = "DST"
                    
                    else:
                        # no team name for FAs
                        try:
                            team = td[1].find("em").text.split("-")[1].strip()
                        except:
                            team = "FA"
                    
                    ovr_rank = td[-1].text
                    if ovr_rank == '--':
                        ovr_rank = ovr_rank.replace('--','0')   
                    ovr_rank = int(ovr_rank)

                    player_data = ["nfl", self.today, self.season, self.week, k, "nfl", pos_rank, full_name, playerId, team, pos, np.nan, np.nan]
                    player_ranks.append(player_data)

                temp_df = pd.DataFrame(player_ranks, columns=hf.ranking_columns)
                df_nfl_ranking = pd.concat([df_nfl_ranking, temp_df], axis = 0, ignore_index=True)
        
        # OFFSEASON
        else:
            nfl_rank_url = self.scraping_urls['nfl']['offseason']['rankings']
            
            for k,v in nfl_rank_url.items():
    
                time.sleep(1)
                r = requests.get(nfl_rank_url[k].format(season=self.season))
                soup = bs(r.text, features='lxml')

                # grabs the number of players with projections on the site. pagenated at 25 a page
                rank_table = soup.find("table", class_="tableType-player noGroups").find("tbody")

                player_ranks = []
                for tr in rank_table.find_all("tr"):
                    player_data = []
                    td = tr.find_all("td")
                    
                    pos_rank = int(td[0].text)
                    playerId = int(td[1].find("a")['href'].split("=")[-1])
                    full_name = td[1].find("a").text.replace(".", "")
                    
                    pos = td[1].find("em").text.split("-")[0].strip()
                    
                    if k == "DEF":
                        team = ""
                        
                    
                    else:
                        # no team name for FAs
                        try:
                            team = td[1].find("em").text.split("-")[1].strip()
                        except:
                            team = "FA"
                        
                    ovr_rank = int(td[-1].text)
                    
                    player_data = ["nfl", self.today, self.season, self.week, k, "nfl", pos_rank, full_name, playerId, team, pos, np.nan, np.nan ]
                    player_ranks.append(player_data)

                temp_df = pd.DataFrame(player_ranks, columns=hf.ranking_columns)
                df_nfl_ranking = pd.concat([df_nfl_ranking, temp_df], axis = 0, ignore_index=True)

            
        if export:
            filepath = str(hf.DATA_DIR) + "/ranking/nfl_rank_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_nfl_ranking.to_csv(filepath, index=False)

        self.scraped_dfs['rankings']['nfl'] = df_nfl_ranking.copy()
        return df_nfl_ranking.shape

    # this isn't adp. its their draft rankings
    def nfl_adp(
        self,
        export = True
    ):
        nfl_adp_url = self.scraping_urls['nfl']['offseason']['adp']
        df_nfl_adp = pd.DataFrame(columns=hf.adp_columns)

        # nfl draft 
        offsets = [1, 101]
        player_ranks = []    
        
        for i in offsets:
            time.sleep(1)
            r = requests.get(nfl_adp_url.format(offset=i))
            soup = bs(r.text, features='lxml')

            # grabs the number of players with projections on the site. pagenated at 25 a page
            rank_table = soup.find("table", class_="tableType-player noGroups").find("tbody")

            
            for tr in rank_table.find_all("tr"):
                player_data = []
                td = tr.find_all("td")
                
                pos_rank = int(td[0].text)
                playerId = int(td[1].find("a")['href'].split("=")[-1])
                full_name = td[1].find("a").text.replace(".", "")
                
                pos = td[1].find("em").text.split("-")[0].strip()
        
                try:
                    team = td[1].find("em").text.split("-")[1].strip()
                except:
                    team = "FA"
                    
                ovr_rank = int(td[-1].text)

                player_data = ["nfl", self.today, playerId, full_name, np.nan, pos,team,pos_rank, np.nan, np.nan ]
                player_ranks.append(player_data)

        df_nfl_adp = pd.DataFrame(player_ranks, columns=hf.adp_columns)

        df_nfl_adp  
        if export:
            filepath = str(hf.DATA_DIR) + "/adp/nfl_adp_{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_nfl_adp.to_csv(filepath, index=False)

        self.scraped_dfs['adps']['nfl'] = df_nfl_adp.copy()
        return df_nfl_adp.shape
        
    # ====================
    #       bp
    # ====================
    def bp_lines(
        self,
        export = True,
        urls = [
            r'https://www.bettingpros.com/nfl/odds/spread',#/?season={season}&week={week}',
            r'https://www.bettingpros.com/nfl/odds/moneyline',#/?season={season}&week={week}",
            r'https://www.bettingpros.com/nfl/odds/total'#/?season={season}&week={week}"
        ]
    ):

        driver = hf.open_browser()

        # empty, to be filled with data from websites
        df_lines = pd.DataFrame(columns=["date", "season", "week", "overUnder", "overUnderCost" 
                                                        "awayTeam", "awaySpread", "awayCost", "awayMoneyline",  
                                                        "homeTeam", "homeSpread", "homeCost", "homeMoneyLine"]) 
        # try to closed driver on error
        try:
            for i in range(len(urls)):    
                driver.get(urls[i].format(season=self.season, week=self.week)) 
                # sleep to let the html load
                time.sleep(7)

                #scrolling through web page to open all games
                y_loc_jump = 500 # initial y loc for scroll
                for z in range(5):
                    driver.execute_script("window.scrollTo(0, {});".format(str(y_loc_jump)))
                    y_loc_jump += 500 # adjust the scroll target down the page
                    time.sleep(2)
                # jump back to top of page after loading 
                driver.execute_script("window.scrollTo(0, {});".format(str(0)))
                time.sleep(1)
                
                html = driver.execute_script("return document.body.innerHTML")
                soup = bs(html, features='lxml')

                data = soup.find_all("div", class_="flex odds-offer")
                
                matchups = []
        ######### spreads
                if i == 0:

                    for div in data:

                        data_team = div.find_all("a", class_="link team-overview__team-name")
                        data_spread = div.find_all("div", class_="flex odds-offer__item")

                        # team 1 data - away
                        away_team = data_team[0]['href'].split("/")[3].replace("-", " ").title()
                        away_spread_line = data_spread[1].find_all("span", class_="odds-cell__line")[0].text.replace("+", "")

                        if (away_spread_line == "NL") or (away_spread_line == "--"):
                            away_spread_line = np.nan

                        away_spread_cost = data_spread[1].find_all("span", class_="odds-cell__cost")[0].text.strip().replace("+", "").replace("(", "").replace(")", "")
                        if (away_spread_cost == "NL") or (away_spread_cost == "--"):
                            away_spread_cost = np.nan

                        #team 2 data - home
                        home_team = data_team[1]['href'].split("/")[3].replace("-", " ").title()
                        home_spread_line = data_spread[1].find_all("span", class_="odds-cell__line")[1].text.replace("+", "")

                        if (home_spread_line == "NL") or (home_spread_line == "--"):
                            home_spread_line = np.nan

                        home_spread_cost = data_spread[1].find_all("span", class_="odds-cell__cost")[1].text.strip().replace("+", "").replace("(", "").replace(")", "")
                        if (home_spread_cost == "NL") or (home_spread_cost == "--"):
                            home_spread_cost = np.nan

                        matchup = [self.today, self.season, self.week,
                                np.nan, np.nan,  # placeholder for overUnders
                                away_team, float(away_spread_line), away_spread_cost, np.nan,
                                home_team, float(home_spread_line), home_spread_cost, np.nan
                                ]
                        
                        matchups.append(matchup)

                    df_lines = pd.DataFrame(matchups, columns=["date", "season", "week", "overUnder", "overUnderCost",
                                                            "awayTeam", "awaySpread", "awayCost", "awayMoneyline",  
                                                            "homeTeam", "homeSpread", "homeCost", "homeMoneyLine"])

        #########moneylines
                elif i == 1:
                    for div in data:

                        data_team = div.find_all("a", class_="team-overview__team-name")
                        data_moneylines = div.find_all("div", class_="flex odds-offer__item")

                        # team 1 data - away
                        away_team = data_team[0]['href'].split("/")[3].replace("-", " ").title()
                        away_moneyline = data_moneylines[1].find_all("span", class_="odds-cell__line")[0].text.strip().replace("+", "")
                        if (away_moneyline == "NL") or (away_moneyline == "--"):
                            away_moneyline = np.nan
                        elif away_moneyline == "EVEN":
                            away_moneyline = 0
                        else:
                            away_moneyline = float(away_moneyline)

                        #team 2 data - home
                        home_moneyline = data_moneylines[1].find_all("span", class_="odds-cell__line")[1].text.strip().replace("+", "")
                        if (home_moneyline == "NL") or (home_moneyline == "--"):
                            home_moneyline = np.nan
                        elif home_moneyline == "EVEN":
                            home_moneyline = 0
                        else:
                            home_moneyline = float(home_moneyline)
                        #print("ML: ", away_moneyline, home_moneyline)
                        df_lines.loc[df_lines[df_lines['awayTeam'] == away_team].index[0],'awayMoneyline'] = away_moneyline
                        df_lines.loc[df_lines[df_lines['awayTeam'] == away_team].index[0],'homeMoneyLine'] = home_moneyline

        ######## totals
                elif i == 2:

                    for div in data:

                        data_team = div.find_all("a", class_="team-overview__team-name")
                        data_spread = div.find_all("div", class_="flex odds-offer__item")

                        # team 1 data - away
                        away_team = data_team[0]['href'].split("/")[3].replace("-", " ").title()
                        overUnder_line = data_spread[0].find_all("span", class_="odds-cell__line")[0].text.strip()
                        
                        if (overUnder_line == "NL") or (overUnder_line == "--"):
                            overUnder_line = np.nan
                        elif (overUnder_line == 'OFF'):
                            overUnder_line = data_spread[1].find_all("span", class_="odds-cell__line")[0].text.strip()
                            overUnder_line = float(overUnder_line.replace("+", "").split(" ")[1])
                        else:
                            overUnder_line = float(overUnder_line.replace("+", "").split(" ")[1])

                        df_lines.loc[df_lines[df_lines['awayTeam'] == away_team].index[0],'overUnder'] = overUnder_line
                        df_lines.loc[df_lines[df_lines['awayTeam'] == away_team].index[0],'overUnderCost'] = -110
                        #print("OU: ", overUnder_line)
            df_lines = df_lines.replace(np.nan, None).replace("EVEN", 100)


        except Exception as ex:
            print(i)
            print(ex)
            driver.close()

        driver.close()

        if export:
            filepath = str(hf.DATA_DIR) + "/betting/lines{season}-{week}_{date}.csv".format(season=self.season, week=self.strWeek, date=self.today)
            df_lines.to_csv(filepath, index=False)

        self.scraped_dfs['lines']['bp'] = df_lines.copy()
        return df_lines.shape

    # ========================
    #    processing scrapes
    # ========================
    def generate_id_maps(
        self
    ):
        
        # getting outlet db ids to convert the scraped names/ids
        outletLookup = hf.query_database(
            query="SELECT outletId, outletName  FROM outlet;"
        )
        # pandas might import some ids as floats, convert back to  ints
        outletLookup['outletId'] = outletLookup['outletId'].astype(float).astype('Int64')
        self.outletLookup = pd.Series(outletLookup.outletId.values, index=outletLookup.outletName).to_dict()
        
        # getting team db ids to convert datasource names to the ids
        teams = hf.query_database(
            query="SELECT * FROM team;"
        )
        # pandas might import some ids as floats, convert back to  ints
        teams['teamId'] = teams['teamId'].astype(float).astype('Int64')
        self.teamLookup = pd.Series(teams.teamId.values, index=teams.nflfastrName).to_dict()
        self.teamLookupBp = pd.Series(teams.teamId.values, index=teams.bpName).to_dict()
        
        # getting expert db ids to convert the scraped names/ids
        expertLookup = hf.query_database(
            query="SELECT analystId, analystName FROM analyst;"
        )
        # pandas might import some ids as floats, convert back to  ints
        expertLookup['analystId'] = expertLookup['analystId'].astype(float).astype('Int64')
        self.expertLookup = pd.Series(expertLookup.analystId.values, index=expertLookup.analystName).to_dict()

        # getting pos db ids for espn
        posLookup = hf.query_database(
            query="SELECT posId, pos FROM pos;"
        )
        # pandas might import some ids as floats, convert back to  ints
        posLookup['posId'] = posLookup['posId'].astype(float).astype('Int64')
        self.posLookup = pd.Series(posLookup.posId.values, index=posLookup.pos).to_dict()

        # create name table back up
        dbPlayers = hf.query_database(
            query="SELECT * FROM player"
        )
        # pandas might import some ids as floats, convert back to  ints
        id_cols = ['playerId', 'cbsId', 'espnId', 'fpId', 'nflId']
        dbPlayers[id_cols] = dbPlayers[id_cols].astype(float).astype('Int64')

        s = pd.Series(dbPlayers.playerId.values, index=dbPlayers.cbsId)
        self.playerLookupCbs = s[s.index.notna()].to_dict()

        s = pd.Series(dbPlayers.cbsId.values, index=dbPlayers.name)
        self.playerLookupCbsName = s[s.index.notna()].to_dict()

        s = pd.Series(dbPlayers.playerId.values, index=dbPlayers.espnId)
        self.playerLookupEspn = s[s.index.notna()].to_dict()

        s = pd.Series(dbPlayers.espnId.values, index=dbPlayers.espnName)
        self.playerLookupEspnName = s[s.index.notna()].to_dict()

        s = pd.Series(dbPlayers.playerId.values, index=dbPlayers.fpId)
        self.playerLookupFfp = s[s.index.notna()].to_dict()

        s = pd.Series(dbPlayers.playerId.values, index=dbPlayers.nflId)
        self.playerLookupNfl = s[s.index.notna()].to_dict()

        #self.playerLookupNflName = pd.Series(dbPlayers.nflId.values, index=dbPlayers.name).dropna().to_dict() 
        nflTeam = hf.query_database(
            query="SELECT nflName, name FROM team WHERE nflName IS NOT NULL;"
        )
        self.playerLookupNflName= pd.Series(nflTeam.name.values, index=nflTeam.nflName).to_dict()
        
        dbPlayers.to_csv(str(hf.DATA_DIR) + "/names_backup.csv", index=False)

        return

    def process_game_lines(self):

        # check there was a scrape, if not hit the data folder and load all files in there
        if self.scraped_dfs['lines']['bp'] == None:
            df_lines = pd.DataFrame(columns=hf.bettingCols)

            directory =  str(hf.DATA_DIR) + "/betting/"
            #looping through every betting line file to aggregate into single df
            for filename in os.listdir(directory):
                f = os.path.join(directory,filename)
                # checking if it is a file
                if os.path.isfile(f):
                    temp = pd.read_csv(f, parse_dates=['date'],  names=hf.bettingCols, skiprows=1)
                    df_lines = pd.concat([df_lines, temp], ignore_index=True)

        else:
            df_lines = self.scraped_dfs['lines']['bp']
            df_lines.columns=hf.bettingCols

        try:
            # convert bp team ids to database teamids
            df_lines['awayTeamId'] = df_lines['awayTeamId'].map(self.teamLookupBp)
            df_lines['homeTeamId'] = df_lines['homeTeamId'].map(self.teamLookupBp)

            print("success")
        except Exception as ex:
            print(ex)

        self.processed_dfs['lines'] = df_lines
        return
    
    #TODO add pulling from the class object if the df is populated
    def process_rankings(self):
        try:
            #main df to hold all data
            df_load_rank = pd.DataFrame(columns = hf.rankingCols)  

            # df to hold players that are not in the database for a source yet
            df_missing_players_rank = pd.DataFrame(columns=['date', 'outlet', 'group', 'playerId', 'sourceId', 'name'])

            
            # combining all outlets rankings to a single dataframe and converting names to the database Ids
            directory = str(hf.DATA_DIR) + '/ranking/'
            #looping through every rank file to aggregate into single df
            for filename in os.listdir(directory):

                f = os.path.join(directory,filename)
                # checking if it is a file
                if os.path.isfile(f) and f.endswith('.csv'):
                    temp = pd.read_csv(f)
                else:
                    temp = pd.read_excel(f)

                temp['date'] = pd.to_datetime(temp['date'])
                # playerId will be regenerated below to the db pid. keeping source id for missing player info
                temp = temp.rename(columns={'playerId':'sourceId'})
                # drop rows where ANY of those columns contain alphabetic characters
                temp['sourceId'] = (
                    temp['sourceId']
                    .apply(pd.to_numeric, errors='coerce')  # non-numeric -> NaN
                    .astype('Int64')                        # nullable int dtype, keeps NaN
                )
                

                # updtaing outlet specific playerIds to database IDs
                if 'cbs_' in f:
                    lookup = self.playerLookupCbs
                    
                elif 'espn_' in f:
                    lookup = self.playerLookupEspn
                    
                elif ('fp_' in f) or ('fpEcr_' in f):
                    lookup = self.playerLookupFfp
                    
                elif 'nfl_' in f:
                    lookup = self.playerLookupNfl
                    lookup = {k: pd.to_numeric(v, errors='coerce') for k, v in lookup.items()}
                    

                # using the lookup to make the change from outletId to dbId
                temp['playerId'] = temp['sourceId'].map(lookup)
                temp['playerId'] = temp['playerId'].astype('Int64')

                ####################################
                # creating a df to hold that date for players who are not in the player table for the source
                if temp[pd.isnull(temp['playerId'])].shape[0] > 0:
                    df_missing_players_rank = pd.concat([
                        df_missing_players_rank,
                        temp.loc[
                            pd.isnull(temp['playerId']),['date', 'outlet', 'group','playerId', 'sourceId', 'name']
                        ]
                    ])
                    
                ####################################

                # updating outlet name to db outlet id 
                temp['outletId'] = temp['outlet'].replace(self.outletLookup)

                #updating expert name to db expert id
                temp['analystId'] = temp['expert'].replace(self.expertLookup)

                temp = temp.rename(columns={
                    'group':'rankGroup',
                    'rank':'ranking'
                })

                temp = temp[hf.rankingCols]
                # adding outlet dataframe to the upload dataframe
                df_load_rank = pd.concat([df_load_rank, temp])

                        
            df_load_rank = df_load_rank.replace(np.nan, None)
            # removing unranked players and rankings that have been loaded already
            df_load_rank = df_load_rank.loc[pd.notnull(df_load_rank['ranking'])]
            df_load_rank['date'] = pd.to_datetime(df_load_rank['date'])
            #df_load_rank = df_load_rank.loc[df_load_rank['date'] >= pd.to_datetime(self.today)]
            df_load_rank = df_load_rank[df_load_rank['analystId']!='AVG']
            df_load_rank['analystId'] = df_load_rank['analystId'].astype(int)

            # drop duplicate rows of the PKs
            subset_cols = [
                'outletId',
                'date',
                'season',
                'week',
                'rankGroup',
                'analystId',
                'playerId'
            ]

            df_load_rank = df_load_rank.drop_duplicates(subset=subset_cols, keep='first')

        except Exception as ex:
            print(ex)
            return 'failed'

        if df_missing_players_rank.shape[0] > 0:
            df_missing_players_rank = df_missing_players_rank.drop_duplicates(subset=['outlet', 'sourceId', 'name'], keep='first')
            df_missing_players_rank = df_missing_players_rank[df_missing_players_rank['name'].notna()]
            df_missing_players_rank.to_csv(str(hf.DATA_DIR) + '/missing players/missingPlayersRank.csv')
            print(df_missing_players_rank.shape[0], 'missing players..')
            hf.add_new_players_to_db(df_missing_players_rank)
        else:
            print('no missing players')
        
        self.processed_dfs['rankings'] = df_load_rank.copy()
        return 
    
    #TODO add pulling from the class object if the df is populated
    def process_projections(self):
        
        try:

            #main df to hold all data
            df_load_proj = pd.DataFrame(columns = hf.projectionCols)
            
            # df to hold players that are not in the database for a source yet
            df_missing_players_proj = pd.DataFrame(
                columns=['date', 'outlet', 'playerId', 'sourceId', 'name'
            ])

            directory = str(hf.DATA_DIR) + '/projection/'
            #looping through every projection file to aggregate into single df
            for filename in os.listdir(directory):

                f = os.path.join(directory,filename)
                # checking if it is a file
                if os.path.isfile(f) and f.endswith('.csv'):
                    temp = pd.read_csv(f)
                else:
                    temp = pd.read_excel(
                        f 
                    )
                temp['date'] = pd.to_datetime(temp['date'])
                # playerId will be regenerated below to the db pid. keeping source id for missing player info
                temp = temp.rename(columns={
                    'playerId':'sourceId'
                })
            
                # drop rows where ANY of those columns contain alphabetic characters
                temp['sourceId'] = (
                    temp['sourceId']
                    .apply(pd.to_numeric, errors='coerce')  # non-numeric -> NaN
                    .astype('Int64')                        # nullable int dtype, keeps NaN
                )

                # creating dicts to convert outlet.team, player) name/id to db id
                if 'cbs' in f:

                    # dict name:pid
                    lookup = self.playerLookupCbsName
                    # updating the cbs source data full team name to the abbreviated db name
                    temp['team'] = temp['team'].replace({"JAC":"JAX","WAS":"WSH" })
                    temp.loc[temp['sourceId'].isna(), 'sourceId'] = temp.loc[temp['sourceId'].isna(), 'team'].map(lookup)

                    # cbs source data does not have playerId for the defenses
                    # dict cbsId:pid
                    lookup = self.playerLookupCbs

                elif 'espn' in f:
                    # espn source data does not have playerId for the defenses
                    # converting full team name to db table 'TEAM'.name
                    
                    # dict name:pid
                    lookup = self.playerLookupEspnName
                    # updating the nfl source data full team name to the abbreviated db name
                    temp.loc[temp['sourceId'].isnull(),'name'] = temp.loc[temp['sourceId'].isnull(),'name'].str.extract(r'^(\S+)')
                    temp.loc[temp['sourceId'].isnull(),'name'] = temp.loc[temp['sourceId'].isnull(),'name'].str.strip()
                    temp.loc[temp['sourceId'].isnull(), 'sourceId'] = temp.loc[temp['sourceId'].isnull(), 'name'].map(lookup)
                    
                    # dict espnId:pid
                    lookup = self.playerLookupEspn

                elif 'nfl' in f:
                    # converting full team name to db table 'TEAM'.name
                    # dict nflName:dbname
                    lookup = self.playerLookupNflName

                    # updating the nfl source data full team name to the abbreviated db name
                    temp.loc[temp['name'].isna(), 'team'] = temp.loc[temp['name'].isna(), 'team'].map(lookup)

                    # dict nflId:pid
                    lookup = self.playerLookupNfl
                    
                # using the lookup to make the change from outletId to dbId
                temp['playerId'] = temp['sourceId'].map(lookup)
                temp['playerId'] = temp['playerId'].astype('Int64')

                # creating a df to hold that date for players who are not in the player table for the source
                if temp[pd.isnull(temp['playerId'])].shape[0] > 0:
                    df_missing_players_proj = pd.concat(
                        [
                            df_missing_players_proj,
                            temp.loc[
                                pd.isnull(temp['playerId']), ['date', 'outlet', 'playerId', 'sourceId', 'name']
                            ]
                        ])

                # updating outlet name to db outlet id 
                temp['outlet'] = temp['outlet'].replace(self.outletLookup)

                temp = temp[hf.projection_filter_cols]
                temp = temp.rename(columns=hf.map_projInput_to_projOut)
                df_load_proj = pd.concat([df_load_proj, temp])
            
        except Exception as ex:
            print(ex)
            return 'failed'
            
        if df_missing_players_proj.shape[0] > 0:
            df_missing_players_proj = df_missing_players_proj.drop_duplicates(subset=['outlet', 'sourceId', 'name'], keep='first')
            df_missing_players_proj = df_missing_players_proj[df_missing_players_proj['name'].notna()]
            df_missing_players_proj.to_csv(str(hf.DATA_DIR) + '/missing players/missingPlayersProj.csv')
            print(df_missing_players_proj.shape[0], 'missing players..')
            hf.add_new_players_to_db(df_missing_players_proj)
        else:
            print('no missing players')

        # helps catch random text sites use instead of null or zero. encoding issues are causing replace to not work
        to_deci = [
            'gp','att', 'comp', 'passYd', 'passYdPg', 'passTd', 'pInt', 'passRtg', 'rush', 'rushYd', 'ydPerRush', 'rushTd', 'target', 'rec', 'recYd', 'recYdPg', 'ydPerRec', 'recTd', 'fmb', 'fgM', 'fgA', 'fgLong', 'fgM0119', 'fgA0119', 'fgM2029', 'fgA2029', 'fgM3039', 'fgA3039', 'fgM4049', 'fgA4049', 'fgM5099', 'fgA5099', 'xpM', 'xpA', 'defInt', 'sfty', 'sack', 'tckl', 'defFmbRec', 'defFmbFor', 'defTd', 'retTd', 'ptsAllowed', 'ptsAllowedPg', 'pYdAllowedPg', 'rYdAllowedPg', 'totalYdAllowed', 'totalYdAllowedPg', 'twoPt', 'fantasyPoints', 'fantasyPointsPg'
        ]
        df_load_proj[to_deci] = df_load_proj[to_deci].apply(
            lambda col: pd.to_numeric(col, errors='coerce')
        )

        #drop duplicates of PKs
        subset_cols = [
            'playerId', 'date', 'season', 'week', 'outletId'
        ]
        df_load_proj = df_load_proj.drop_duplicates(subset=subset_cols, keep='first')

        self.processed_dfs['projections'] = df_load_proj.copy()
        return 
    
    def process_adps(self):

        try:
            #main df to hold all data
            df_load_adp = pd.DataFrame(columns = [
                    'outletId','date','playerId','adp','high','low'
                ])  

            # df to hold players that are not in the database for a source yet
            df_missing_players_adp = pd.DataFrame(columns=['date', 'outlet', 'playerId', 'sourceId', 'name'])

            
            # combining all outlets rankings to a single dataframe and converting names to the database Ids
            directory = str(hf.DATA_DIR) + '/adp/'
            #looping through every rank file to aggregate into single df
            for filename in os.listdir(directory):
                print(filename)
                f = os.path.join(directory,filename)
                # checking if it is a file
                if os.path.isfile(f) and f.endswith('.csv'):
                    temp = pd.read_csv(f)
                elif f.endswith('.xlsx'):
                    temp = pd.read_excel(f)
                else:
                    continue

                temp['date'] = pd.to_datetime(temp['date'])
                # playerId will be regenerated below to the db pid. keeping source id for missing player info
                temp = temp.rename(columns={'playerId':'sourceId'})
                # drop rows where ANY of those columns contain alphabetic characters
                temp['sourceId'] = (
                    temp['sourceId']
                    .apply(pd.to_numeric, errors='coerce')  # non-numeric -> NaN
                    .astype('Int64')                        # nullable int dtype, keeps NaN
                )
                

                # updtaing outlet specific playerIds to database IDs
                if 'cbs_' in f:
                    # dict name:pid
                    lookup = self.playerLookupCbsName
                    # updating the cbs source data full team name to the abbreviated db name
                    temp['team'] = temp['team'].replace({"JAC":"JAX","WAS":"WSH" })
                    temp.loc[temp['sourceId'].isna(), 'sourceId'] = temp.loc[temp['sourceId'].isna(), 'team'].map(lookup)

                    # cbs source data does not have playerId for the defenses
                    # dict cbsId:pid
                    lookup = self.playerLookupCbs
                    
                elif 'espn_' in f:
                    # dict name:pid
                    lookup = self.playerLookupEspnName
                    # updating the nfl source data full team name to the abbreviated db name
                    temp.loc[temp['sourceId'].isnull(),'name'] = temp.loc[temp['sourceId'].isnull(),'name'].str.extract(r'^(\S+)')
                    temp.loc[temp['sourceId'].isnull(),'name'] = temp.loc[temp['sourceId'].isnull(),'name'].str.strip()
                    temp.loc[temp['sourceId'].isnull(), 'sourceId'] = temp.loc[temp['sourceId'].isnull(), 'name'].map(lookup)
                    
                    # dict espnId:pid
                    lookup = self.playerLookupEspn
                    
                elif ('fp_' in f) or ('fpEcr_' in f):
                    lookup = self.playerLookupFfp
                    
                elif 'nfl_' in f:
                    # converting full team name to db table 'TEAM'.name
                    # dict nflName:dbname
                    lookup = self.playerLookupNflName

                    # updating the nfl source data full team name to the abbreviated db name
                    temp.loc[temp['name'].isna(), 'team'] = temp.loc[temp['name'].isna(), 'team'].map(lookup)

                    # dict nflId:pid
                    lookup = self.playerLookupNfl
                    

                # using the lookup to make the change from outletId to dbId
                temp['playerId'] = temp['sourceId'].map(lookup)
                temp['playerId'] = temp['playerId'].astype('Int64')
                
                temp = temp[temp['playerId'].notna()]

                # creating a df to hold that date for players who are not in the player table for the source
                if temp[pd.isnull(temp['playerId'])].shape[0] > 0:
                    df_missing_players_adp = pd.concat(
                        [
                            df_missing_players_adp,
                            temp.loc[
                                pd.isnull(temp['playerId']), ['date', 'outlet', 'playerId', 'sourceId', 'name']
                            ]
                        ])
                    

                # finish processing and build data set
                # updating outlet name to db outlet id 
                temp['outletId'] = temp['outlet'].replace(self.outletLookup)
                
                temp = temp[[
                    'outletId','date','playerId','adp','high','low'
                ]]
                
                df_load_adp = pd.concat([df_load_adp, temp])
            
                
                
        except Exception as ex:
            print(ex)
            return 'failed'

        if df_missing_players_adp.shape[0] > 0:
            df_missing_players_adp = df_missing_players_adp.drop_duplicates(subset=['outlet', 'sourceId', 'name'], keep='first')
            df_missing_players_adp = df_missing_players_adp[df_missing_players_adp['name'].notna()]
            df_missing_players_adp.to_csv(str(hf.DATA_DIR) + '/missing players/missingPlayersAdp.csv')
            print(df_missing_players_adp.shape[0], 'missing players..')
            
        else:
            print('no missing players')

        df_load_adp[["adp","high","low"]] = df_load_adp[["adp","high","low"]].apply(
            pd.to_numeric, errors="coerce"
        )
        self.processed_dfs['adps'] = df_load_adp.copy()
        return 

        
