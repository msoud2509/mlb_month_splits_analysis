import pandas as pd
import os
import pybaseball


def get_raw_logs(year):
    """gets regular season game logs of every team by year and writes to respective files"""
    #make sure directory exists beforehand
    #correct path: mlb_month_splits/hittingteamgamelogs/teamgamelogs{year}/hitting/
    teamabbrevdataf = pd.read_csv("mlb_month_splits/team_abbreviations.csv")
    for team in teamabbrevdataf["Abbreviations"]:
        #pybaseball.team_game_logs defaults to hitting
        df = pybaseball.team_game_logs(year,team)
        df.to_csv("mlb_month_splits/hittingteamgamelogs/teamgamelogs{}/hitting/{}hit{}.csv".format(year,year,team),index=False)


def add_game_by_game_stats(year):
    """need to get daily stats for gamelog files, because baseball-reference lists accumulation of stats over season per entry"""
    gameloghitlist = os.listdir("mlb_month_splits/hittingteamgamelogs/teamgamelogs{}/hitting".format(year))
    for file in gameloghitlist:
        dataf = pd.read_csv("mlb_month_splits/hittingteamgamelogs/teamgamelogs{}/hitting/{}".format(year,file))
        dataf["gameBA"] = dataf["H"] / dataf["AB"]
        dataf["gameOBP"] = (dataf["H"] + dataf["BB"] + dataf["HBP"])/dataf["PA"]
        dataf["gameSLG"] = ((4*dataf["HR"]) + (3*dataf["3B"]) + (2*dataf["2B"]) + (dataf["H"]-dataf["3B"]-dataf["2B"]-dataf["HR"]))/dataf["AB"]
        dataf["gameOPS"] = (dataf["gameOBP"] + dataf["gameSLG"])
        dataf.to_csv("mlb_month_splits/hittingteamgamelogs/teamgamelogs{}/hitting/{}".format(year,file), index=False)