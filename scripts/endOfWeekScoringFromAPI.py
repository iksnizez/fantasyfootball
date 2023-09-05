########################
## weekly score retrieval
########################
# when this is ran after the last game of the weekly schedule
# it will pull all of the player stats and scores for the week
# once ESPN updates the current scoring period the full load of data
# is lost so it has to be run after the final game and before the
# week change in their system.

if __name__ == "__main__":

    # retrieving league credentials from file - league id and espn cookies
    path = r"..\..\Data\archive\leagueCreds.txt"
    with open(path, 'r', encoding='utf-8') as f:
        creds = json.loads(f.read())

    lid = creds["league_id"]    #espn league id
    cookies = creds["cookies"]  #espn league cookies
    year = "2023"
    week = 1
    #week = str(input("Which week is it?"))

    # retrieves the entire player population
    headers = {
        'X-Fantasy-Filter': '{"players": {"limit": 1500, "sortPercOwned":{"sortAsc":false,"sortPriority":1}}}'
    }

    # code mapping for roster positions and teamIDs
    lineupSlotID = {17: 'K', 0: 'QB', 20: 'bench', 15: 'DP', 6: 'TE', 23: 'FLEX', 4: 'WR', 2: 'RB', 21: 'IR'}
    teamIds = {
        1: 'John', 2: 'Gomer', 3: 'Pope', 4: "Jamie", 5: "Geik", 6: "Bryan",
        7: "Chaunce", 8: "Sam", 9: "Chris", 10: "Murphy", 11: "Colin", 12: 'Ethan'
    }

    #saving the weeks data for later use
    url1 = "https://fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=kona_player_info".format(year)
    response = requests.request("GET", url1, headers=headers, cookies=cookies)
    player_json = json.loads(response.content)
    with open('{}week{}_player.txt'.format(year, week), 'w') as outfile:
        json.dump(player_json, outfile)

    url2 = "https://fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?view=mBoxscore".format(year)
    response = requests.request("GET", url2, headers=headers, cookies=cookies)
    boxscore_json = json.loads(response.content)
    with open('{}week{}_boxscore.txt'.format(year, week), 'w') as outfile:
        json.dump(boxscore_json, outfile)

    url3 = """https://fantasy.espn.com/apis/v3/games/ffl/seasons/{}/segments/0/leagues/245118?
                view=mDraftDetail&view=mLiveScoring&view=mMatchupScore&view=mPendingTransactions
                &view=mPositionalRatings&view=mSettings&view=mTeam&view=modular&view=mNav&view=mMatchupScore""".format(year)
    response = requests.request("GET", url3, headers=headers, cookies=cookies)
    league_json = json.loads(response.content)
    with open('{}week{}_league.txt'.format(year, week), 'w') as outfile:
        json.dump(league_json, outfile)

    # assigning the data for each team to the owner
    data = {1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {}, 10: {}, 11: {}, 12: {}}
    for i in range(1, 13):
        for j in range(week, 18):  # start range set to week so that the previous weeks don't get erased on accident
            data[i][j] = {'roster': {0: {}, 2: {}, 4: {}, 6: {}, 23: {}, 15: {}, 17: {}, 20: {}, 21: {}},
                          'pf': 0, 'pa': 0,
                          'ppf': 0, 'ppa': 0,  # proj points for and against
                          'pfTotal': 0, 'paTotal': 0,
                          'ppfTotal': 0, 'ppaTotal': 0,
                          'wl': [0, 0, 0],  # wins, losses, ties
                          'prfScore': 0,  # the score for the perfect line up
                          'prfLineup': 0,  # perfect line up, 1 = yes, 0 = no
                          'pwrESPN': 0,  # espn rank
                          'pwrCommish': 0,  # commish rank
                          'wst': 0,  # total points left on the bench,
                          'scoreRnk': 0,  # absolute rank of this weeks score 1-12
                          'oppScoreRnk': 0,  # absolute rank of this weeks score 1-12
                          'projPlusMinus': 0,  # total pts over or under total projection
                          'oppProjPlusMinus': 0,  # opp. total pts over or under total projection
                          'oppId': 0  # opp id
                          }

    # calculating the matchupIDs for the week - 12 teams = 6 matchups
    matches = [-6 + (6 * week), -5 + (6 * week), -4 + (6 * week), -3 + (6 * week), -2 + (6 * week), -1 + (6 * week)]

    # loops through the matchups for the week. Matchups are labeled 0 - 83
    #  week 1 = 0-5, week 2 = 6-11, etc..
    week_scores = []
    for i in matches:
        # grab data starting point for easier reading later
        game = boxscore_json['schedule'][i]

        # grab team ids for the match up
        away_team = game['away']['teamId']
        home_team = game['home']['teamId']
        data[away_team][week]['oppId'] = home_team
        data[home_team][week]['oppId'] = away_team

        # grab base scoring data for easier reading later in code
        away_stats = game['away']['rosterForCurrentScoringPeriod']
        home_stats = game['home']['rosterForCurrentScoringPeriod']

        # update points for and points against for each team
        awayPts = away_stats['appliedStatTotal']
        homePts = home_stats['appliedStatTotal']
        data[away_team][week]['pf'] = awayPts
        data[away_team][week]['pa'] = homePts

        # building list of scores to create league weekly ranks
        week_scores.append(awayPts)
        week_scores.append(homePts)

        data[home_team][week]['pa'] = awayPts
        data[home_team][week]['pf'] = homePts

        # update record
        if awayPts > homePts:
            data[away_team][week]['wl'][0] += 1
            data[home_team][week]['wl'][1] += 1
        elif awayPts < homePts:
            data[away_team][week]['wl'][1] += 1
            data[home_team][week]['wl'][0] += 1
        else:
            data[away_team][week]['wl'][2] += 1
            data[home_team][week]['wl'][2] += 1

        ###################################################
        # looping through home and away data to populate weekly team data
        ##################################################
        ##### AWAY TEAM
        # populating the player act and proj scores
        for p in away_stats['entries']:
            # set vars for easier reading
            slotId = p['lineupSlotId']
            pId = p['playerId']
            player = p['playerPoolEntry']['player']['fullName']
            defaultSlot = p['playerPoolEntry']['player']['defaultPositionId']

            # set base stat data for easier reading
            stats = p['playerPoolEntry']['player']['stats']
            data[away_team][week]['roster'][slotId][pId] = {}
            data[away_team][week]['roster'][slotId][pId]['name'] = player

            # updating all defensive players to have default slot == 15
            if (defaultSlot >= 10) and (defaultSlot <= 16):
                data[away_team][week]['roster'][slotId][pId]['defaultSlot'] = 15
            else:
                data[away_team][week]['roster'][slotId][pId]['defaultSlot'] = defaultSlot

            # building the roster performance dictionary - scores, projects, starts, bench
            for s in stats:
                # [statSourceId] == 0 is actual points scored
                if s['statSourceId'] == 0:
                    act = s['appliedTotal']
                    data[away_team][week]['roster'][slotId][pId]['act'] = act
                else:
                    proj = s['appliedTotal']
                    data[away_team][week]['roster'][slotId][pId]['proj'] = proj
                    if (slotId == 20) or (slotId == 21):
                        pass
                    else:
                        data[away_team][week]['ppf'] += proj

        ## HOME TEAM -same as the loop above but for the home team
        for p in home_stats['entries']:
            # set vars for easier reading
            slotId = p['lineupSlotId']
            pId = p['playerId']
            player = p['playerPoolEntry']['player']['fullName']
            defaultSlot = p['playerPoolEntry']['player']['defaultPositionId']
            # set base stat data for easier reading
            stats = p['playerPoolEntry']['player']['stats']
            data[home_team][week]['roster'][slotId][pId] = {}
            data[home_team][week]['roster'][slotId][pId]['name'] = player
            if (defaultSlot >= 10) and (defaultSlot <= 16):
                data[home_team][week]['roster'][slotId][pId]['defaultSlot'] = 15
            else:
                data[home_team][week]['roster'][slotId][pId]['defaultSlot'] = defaultSlot
            for s in stats:
                # [statSourceId] == 0 #actual
                if s['statSourceId'] == 0:
                    act = s['appliedTotal']
                    data[home_team][week]['roster'][slotId][pId]['act'] = act
                else:
                    proj = s['appliedTotal']
                    data[home_team][week]['roster'][slotId][pId]['proj'] = proj
                    if (slotId == 20) or (slotId == 21):
                        pass
                    else:
                        data[home_team][week]['ppf'] += proj

        ###################################
        # exited the home/away loops but still in the game json loop for that week
        ##################################

        # calculate PF vs Proj
        data[home_team][week]['ppa'] = data[away_team][week]['ppf']
        data[away_team][week]['ppa'] = data[home_team][week]['ppf']
        data[home_team][week]['projPlusMinus'] = data[home_team][week]['pf'] - data[home_team][week]['ppf']
        data[away_team][week]['projPlusMinus'] = data[away_team][week]['pf'] - data[away_team][week]['ppf']
        data[home_team][week]['oppProjPlusMinus'] = data[away_team][week]['projPlusMinus']
        data[away_team][week]['oppProjPlusMinus'] = data[home_team][week]['projPlusMinus']

        #########################
        ### calculating perfect line up scores
        #########################

        # creating a dictionary for the away team points by position id
        away_points = {0: [], 2: [], 4: [], 6: [], 15: [], 17: []}
        for pos in [0, 2, 4, 6, 15, 17, 20]:
            for i, p in data[away_team][week]['roster'][pos].items():
                slot = p['defaultSlot']
                if slot == 1:
                    away_points[0].append(p['act'])
                elif slot == 2:
                    away_points[2].append(p['act'])
                    # bisect.insort(points[2], p['act'])
                elif slot == 3:
                    away_points[4].append(p['act'])
                    # bisect.insort(points[4], p['act'])
                elif slot == 4:
                    away_points[6].append(p['act'])
                # bisect.insort(points[6], p['act'])
                elif slot == 15:
                    away_points[15].append(p['act'])
                    # bisect.insort(points[15], p['act'])
                elif slot == 5:
                    away_points[17].append(p['act'])
                    # bisect.insort(points[17], p['act'])

        # variable to hold the running point total for a perfect lineup
        prfPoints = 0

        # variable to hold the RB, WR, and TE points that did not get into the perfect starting lineup,
        # the max point in this line up will be the starting flex player
        flex = []
        for i, v in away_points.items():
            scores = list(sorted(v, reverse=True))
            if i == 0:
                prfPoints += scores[0]
            elif i == 2:
                prfPoints += sum(scores[0:2])
                flex.append(scores[2:])
            elif i == 4:
                prfPoints += sum(scores[0:2])
                flex.append(scores[2:])
            elif i == 6:
                prfPoints += scores[0]
                flex.append(scores[1:])
            elif i == 15:
                prfPoints += scores[0]
            elif i == 17:
                prfPoints += scores[0]

        # flattens the list of list created from the flex list generation. the max value is added to the perfect lineup
        prfPoints += sorted([item for sublist in flex for item in sublist], reverse=True)[0]
        # calculates the wasted points
        data[away_team][week]['wst'] = prfPoints - data[away_team][week]['pf']
        # marks a perfect lineup if there are no wasted points
        if data[away_team][week]['wst'] == 0:
            data[away_team][week]['prfLineup'] == 1
        # adds perfect lineup score to the team data for the week
        data[away_team][week]['prfScore'] = prfPoints

        # calculates the running totals for act and proj pf/pa
        if week == 1:
            data[away_team][week]['pfTotal'] = data[away_team][week]['pf']
            data[away_team][week]['paTotal'] = data[away_team][week]['pa']
            data[away_team][week]['ppfTotal'] = data[away_team][week]['ppf']
            data[away_team][week]['ppaTotal'] = data[away_team][week]['ppa']
        else:
            data[away_team][week]['pfTotal'] += data[away_team][week - 1]['pf']
            data[away_team][week]['paTotal'] += data[away_team][week - 1]['pa']
            data[away_team][week]['ppfTotal'] += data[away_team][week - 1]['ppf']
            data[away_team][week]['ppaTotal'] += data[away_team][week - 1]['ppa']
        # saves the ESPN power ranking
        data[away_team][week]['pwrESPN'] = league_json['teams'][away_team - 1]['currentProjectedRank']

        ### calculating perfect line up scores
        # creating a dictionary for the HOME team points by position id
        home_points = {0: [], 2: [], 4: [], 6: [], 15: [], 17: []}
        for pos in [0, 2, 4, 6, 15, 17, 20]:
            for i, p in data[home_team][week]['roster'][pos].items():
                slot = p['defaultSlot']
                if slot == 1:
                    home_points[0].append(p['act'])
                elif slot == 2:
                    home_points[2].append(p['act'])
                    # bisect.insort(points[2], p['act'])
                elif slot == 3:
                    home_points[4].append(p['act'])
                    # bisect.insort(points[4], p['act'])
                elif slot == 4:
                    home_points[6].append(p['act'])
                    # bisect.insort(points[6], p['act'])
                elif slot == 15:
                    home_points[15].append(p['act'])
                    # bisect.insort(points[15], p['act'])
                elif slot == 5:
                    home_points[17].append(p['act'])
                    # bisect.insort(points[17], p['act'])

        # variable to hold the running point total for a perfect lineup
        prfPoints = 0
        # variable to hold the RB, WR, and TE points that did not get into the perfect starting lineup,
        # the max point in this line up will be the starting flex player
        flex = []
        for i, v in home_points.items():
            scores = list(sorted(v, reverse=True))
            if i == 0:
                prfPoints += scores[0]
            elif i == 2:
                prfPoints += sum(scores[0:2])
                flex.append(scores[2:])
            elif i == 4:
                prfPoints += sum(scores[0:2])
                flex.append(scores[2:])
            elif i == 6:
                prfPoints += scores[0]
                flex.append(scores[1:])
            elif i == 15:
                prfPoints += scores[0]
            elif i == 17:
                prfPoints += scores[0]

        # flattens the list of list created from the flex list generation. the max value is added to the perfect lineup
        prfPoints += sorted([item for sublist in flex for item in sublist], reverse=True)[0]
        # calculates the wasted points
        data[home_team][week]['wst'] = prfPoints - data[home_team][week]['pf']
        # marks a perfect lineup if there are no wasted points
        if data[home_team][week]['wst'] == 0:
            data[home_team][week]['prfLineup'] == 1
        # adds perfect lineup score to the team data for the week
        data[home_team][week]['prfScore'] = prfPoints

        # calculates the running totals for act and proj pf/pa
        if week == 1:
            data[home_team][week]['pfTotal'] = data[home_team][week]['pf']
            data[home_team][week]['paTotal'] = data[home_team][week]['pa']
            data[home_team][week]['ppfTotal'] = data[home_team][week]['ppf']
            data[home_team][week]['ppaTotal'] = data[home_team][week]['ppa']
        else:
            data[home_team][week]['pfTotal'] += data[home_team][week - 1]['pf']
            data[home_team][week]['paTotal'] += data[home_team][week - 1]['pa']
            data[home_team][week]['ppfTotal'] += data[home_team][week - 1]['ppf']
            data[home_team][week]['ppaTotal'] += data[home_team][week - 1]['ppa']

        # saves the ESPN power ranking
        data[home_team][week]['pwrESPN'] = league_json['teams'][home_team - 1]['currentProjectedRank']

    week_scores = sorted(week_scores)
    for team in data:
        data[team][week]['scoreRnk'] = week_scores.index(data[team][week]["pf"]) + 1
        data[team][week]['oppScoreRnk'] = week_scores.index(data[data[team][week]['oppId']][week]["pf"]) + 1





