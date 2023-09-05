########################
## Historical Draft Data
########################
# this script will extract draft pick data from HTML
# saved from the ESPN website for a fantasy league draft history

if __name__ == "__main__":

    import math, os
    import pandas as pd
    from bs4 import BeautifulSoup as bs

    ### INPUTS THAT NEED TO BE UPDATE FOR USER
    # number of teams in the league
    nTeams = 12
    # assign directory and save output path
    # the "..\" is because the script and output/data folders are at the same level
    directory = "..\Data\Draft\espnHTML"
    outputPath = "..\Data\Draft\draftHistory.xlsx"

    #suffixes = ["Jr.", "Sr.", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"]  # name suffixes that will be

    # this will hold the agg for all of the years
    drafts = pd.DataFrame(columns=['year', 'pick', 'round', 'overallPick',
                                   'firstName', 'lastName', 'playerTeam', 'pos',
                                   'team'])

    # iterate over files in  that directory
    for filename in os.scandir(directory):
        if filename.is_file():

            # the file is just the <divs> that contain the draft round data
            # parent div at the time of creation = <div class="ResponsiveTable">
            # details for each pick are <td class=Table__TD>  there are 3 <td>s for each pick
            with open(filename.path, "r", encoding='utf-8') as f:
                data = f.read()

            soup = bs(data, "html.parser")
            picks = soup.find_all("td", class_="Table__TD")

            o = 1  # overall pick count tracker
            year = filename.name.split(".")[0]

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
                team = playerInfo[2].replace(",", "")
                pos = playerInfo[3]

                # round details
                n = int(picks[p].text)  # pick number in the round
                r = math.ceil((o / nTeams))  # round number

                # fantasy team making the pick
                fTeam = picks[p + 2].text

                pick = [year, n, r, o, firstName, lastName, team, pos, fTeam]
                # drafts = pd.concat([drafts, pick], ignore_index=True)
                drafts.loc[len(drafts.index)] = pick
                o += 1


    drafts.to_excel(outputPath, index=False)