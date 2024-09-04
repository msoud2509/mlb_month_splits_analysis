import pandas as pd
from bs4 import BeautifulSoup
import os

'''#comment from pybaseball repo on the necessity of BRefSession

    Bref is needed because Baseball Reference has rules against bots.

    Current policy says no more than 20 requests per minute, but in testing
    anything more than 10 requests per minute gets you blocked for one hour.

    So this global session will prevent a user from getting themselves blocked.'''
    
from pybaseball.datasources.bref import BRefSession
session = BRefSession()

def gather_playoffs(year):
    df = pd.DataFrame(columns=['Tm','Playoff_BA','Playoff_OPS','Playoff_SO','Playoff_AB','Playoff_Series_Count'])
    #only takes 2023 and 2022 soz
    if year == 2022 or year == 2023:
        serieslist = ['WS','ALCS','NLCS','ALDS1','NLDS1','ALDS2','NLDS2','ALWC1','NLWC1','ALWC2','NLWC2']
    #CHANGE HERE!!!!!!
    elif (year in range(2012,2022)) and year != 2020:
        serieslist= ['WS','ALCS','NLCS','ALDS1','NLDS1','ALDS2','NLDS2','ALWC','NLWC']
    else:
        raise Exception('only playoff formats from 2012 to present (excluding 2020) are accounted for at the moment')
    
    playoff_teams_list = []

    teamabbrevdataf = pd.read_csv('mlb_month_splits/team_abbreviations.csv')
    if year < 2022:
        teamabbrevdataf.at[7, 'Team'] = 'Cleveland Indians'
    
    
    for series in serieslist:
        _URL = "https://www.baseball-reference.com/postseason/{}_{}.shtml".format(year,series)

        content = session.get(_URL).content
        soup = BeautifulSoup(content,"lxml")

        #head text contains full name of each team playing in series
        head = soup.find('h1')
        head = head.text.strip()
        count = 0
        teams = []
        for c,x in enumerate(teamabbrevdataf['Team']):
            if x in head:
                if teamabbrevdataf.iloc[c]['Abbreviations'] not in playoff_teams_list:
                    playoff_teams_list.append(teamabbrevdataf.iloc[c]['Abbreviations'])
                count += 1
                teams.append(teamabbrevdataf.iloc[c]['Abbreviations'])
            #only two teams can play in a series
            if count == 2:
                break

        for team in teams:
            #find table of team batting stats for the entire series
            table = soup.find("table",attrs=dict(id = "post_batting_{}".format(team)))
            dataf = pd.read_html(str(table),header=[1])[0]
            
            #remove unecessary columns
            dataf = dataf.drop(['G.1','AB.1','R.1','H.1','HR.1','RBI.1','SB.1','BA.1','OPS.1'],axis=1)
            

            if team not in df['Tm'].tolist():
                row_to_add = [team, 0, 0, 0, 0, 0]
                df.loc[len(df)] = row_to_add
            
            #add up totals
            df.at[df['Tm'].tolist().index(team),'Playoff_BA'] = df.iloc[df['Tm'].tolist().index(team)]['Playoff_BA'] + float(dataf.iloc[-1]['BA'])
            df.at[df['Tm'].tolist().index(team),'Playoff_OPS'] = df.iloc[df['Tm'].tolist().index(team)]['Playoff_OPS'] + float(dataf.iloc[-1]['OPS'])
            df.at[df['Tm'].tolist().index(team),'Playoff_SO'] = df.iloc[df['Tm'].tolist().index(team)]['Playoff_SO'] + float(dataf.iloc[-1]['SO'])
            df.at[df['Tm'].tolist().index(team),'Playoff_AB'] = df.iloc[df['Tm'].tolist().index(team)]['Playoff_AB'] + float(dataf.iloc[-1]['AB'])
            df.at[df['Tm'].tolist().index(team),'Playoff_Series_Count'] = df.iloc[df['Tm'].tolist().index(team)]['Playoff_Series_Count'] + 1

        #for checkpoints for each playoff series when loading data, use this code:
        print('series done')

    #get averages per series
    df['Playoff_BA'] = df['Playoff_BA']/df['Playoff_Series_Count']
    df['Playoff_OPS'] = df['Playoff_OPS']/df['Playoff_Series_Count']
    df['Playoff_SO/BA'] = df['Playoff_SO'] / df['Playoff_AB']
    
    if year == 2022 or year == 2023:
        try:
            assert len(playoff_teams_list) == 12
        except AssertionError:
            raise Exception(playoff_teams_list)
    elif (year in range(2012,2022)) and year != 2020:
        try:
            assert len(playoff_teams_list) == 10
        except AssertionError:
            raise Exception(playoff_teams_list)

    #returns playoff_teams_list to gather regular season data
    return df, playoff_teams_list

#adds regular season data to existing pandas dataframe
def add_regszn_data(df, year, playoff_teams):
    #create all the new necessary columns
    month_splits_list = ['Mar/Apr_BA','Mar/Apr_OPS','Mar/Apr_SO/PA','Mar/Apr_LOB','May_BA','May_OPS','May_SO/PA','May_LOB',
    'Jun_BA','Jun_OPS','Jun_SO/PA','Jun_LOB','Jul_BA','Jul_OPS','Jul_SO/PA','Jul_LOB','Aug_BA','Aug_OPS','Aug_SO/PA','Aug_LOB',
    'Sep/Oct_BA','Sep/Oct_OPS','Sep/Oct_SO/PA','Sep/Oct_LOB']
    
    df[month_splits_list] = 0

    gameloghitlist = os.listdir('mlb_month_splits/hittingteamgamelogs/teamgamelogs{}/hitting'.format(year))
    monthlist = ['Apr','May','Jun','Jul','Aug','Sep']
    
    #sort to make searching in gameloghitlist easier
    playoff_teams.sort()
    
    #gameloghitlistcounter
    hitlogcount = 0

    def avg(mylist):
        return sum(mylist) / len(mylist)

    for team in playoff_teams:
        while(team not in gameloghitlist[hitlogcount]):
            hitlogcount += 1
        row_to_add = []
        teamgamelogdf = pd.read_csv('mlb_month_splits/hittingteamgamelogs/teamgamelogs{}/hitting/{}hit{}.csv'.format(year, year, team))
        
        #adds month stats for one team a month at a time
        for month in monthlist:
            #combine april and march games, as well as october and september games for simplicity
            if month == 'Apr':
                splitdataf = teamgamelogdf.loc[teamgamelogdf['Date'].str.contains(month) | teamgamelogdf['Date'].str.contains('Mar')]
            elif month == 'Sep':
                splitdataf = teamgamelogdf.loc[teamgamelogdf['Date'].str.contains(month) | teamgamelogdf['Date'].str.contains('Oct')]
            else:
                splitdataf = teamgamelogdf.loc[teamgamelogdf['Date'].str.contains(month)]

            row_to_add.append(avg(splitdataf['gameBA']))
            avgOBP = avg(splitdataf['gameOBP'])
            avgSLG = avg(splitdataf['gameSLG'])
            #appends ops
            row_to_add.append(avgOBP + avgSLG)
            
            row_to_add.append(avg(splitdataf['SO'])/avg(splitdataf['PA']))
            row_to_add.append(avg(splitdataf['LOB']))

        #adds all month stats of one team to existing dataframe
        df.loc[df['Tm'].tolist().index(team), month_splits_list] = row_to_add

        #for checkpoints for each playoff series when loading data, use this code:
        print('team done')
    
    return df

            
            
            