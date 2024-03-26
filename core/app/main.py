import sys
import os
import sqlite3
import pandas as pd
import numpy as np

# Read the environment variables
base_dir = os.getenv("base_dir")
app_dir = os.getenv("app_dir")
scripts_dir = os.getenv("scripts_dir")
db_path = os.getenv("db_path")

# Add the paths to the sys.path list
sys.path.append(base_dir)

from scripts.get_data import GetHistoricData
from scripts.norm_data import NormData
from scripts.clean_data import CleanData
from scripts.save_data import SaveDataToDB





def main():
    """
    This is the main function that performs data retrieval, normalization, cleaning, and saving to the database.
    For now it retrieves countries and venues information
    """
    get = GetHistoricData("2021", 10)
    norm = NormData("2021")
    clean = CleanData()
    save = SaveDataToDB()

    
    
    """
    # Countries info
    json_countries = get.get_countries_info()
    df_countries = norm.norm_countries_info(json_countries)
    df_countries_clean = clean.clean_countries_info(df_countries)
    save.countries_info_to_db(df_countries_clean)
    """
    
 
    
    """
    # Venues info
    conn = sqlite3.connect(db_path) # Connect to the database
    countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn) # Get the list of countries
    conn.close()
    
    for country_id in countries['country_name']:
        if get._check_api_limit():  # Controlla se il limite delle chiamate API è stato raggiunto
            break
        json_venues = get.get_venues_info(country_id)
        
        if json_venues["results"] == 0:
            continue    
        df_venues = norm.norm_venues_info(json_venues)
        df_venues_clean = clean.clean_venues_info(df_venues)
        save.venues_info_to_db(df_venues_clean)
    """
    
    
    
    """
    # Leagues info
    conn = sqlite3.connect(db_path) # Connect to the database
    countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn) # Get the list of countries
    conn.close()
    
    for country_id in countries['country_name']:
        if get._check_api_limit():  # Controlla se il limite delle chiamate API è stato raggiunto
            break
        json_leagues = get.get_leagues_info(country_id)
        
        if json_leagues["results"] == 0:
            continue
        get.save_json(json_leagues, "leagues_info.json")
        df_leagues = norm.norm_leagues_info(json_leagues)
        df_leagues_clean = clean.clean_leagues_info(df_leagues)
        save.leagues_info_to_db(df_leagues_clean)
    """
    
    
    """
    # Teams info
    conn = sqlite3.connect(db_path) # Connect to the database
    countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn) # Get the list of leagues   
    conn.close()
    
    for country_id in countries['country_name']:
        if get._check_api_limit():  # Controlla se il limite delle chiamate API è stato raggiunto
            break
        json_teams = get.get_teams_info(country_id)
        
        if json_teams["results"] == 0:
            continue 
        df_teams = norm.norm_teams_info(json_teams)
        df_teams_clean = clean.clean_teams_info(df_teams)
        save.teams_info_to_db(df_teams_clean)
    """
    
    
    """
    # Players info
    conn = sqlite3.connect(db_path) # Connect to the database
    teams = pd.read_sql_query("SELECT DISTINCT team_id FROM teams_info", conn) # Get the list of teams
    conn.close()
    
    for team_id in teams['team_id']:
        if get._check_api_limit():
            break
        
        json_players = get.get_players_info(team_id)
        print(json_players)
        if json_players["results"] == 0:
            continue
        
        get.save_json(json_players, "players_info.json")
        
        df_players = norm.norm_players_info(json_players)
        print(df_players)
        
        df_players_clean = clean.clean_players_info(df_players)
        print(df_players_clean)
        
        save.players_info_to_db(df_players_clean)
    """
    
    
    # Matches info  
    dates = get.date_range()
      
    for date in dates['date']:
        if get._check_api_limit():
            break
        
        json_matches = get.get_matches_info(date)
        print(json_matches)
        if json_matches["results"] == 0:
            continue
        
        get.save_json(json_matches, "matches_info.json")
        
        df_matches = norm.norm_matches_info(json_matches)
        print(df_matches)
        
        df_matches_clean = clean.clean_matches_info(df_matches)
        print(df_matches_clean)
        
        save.matches_info_to_db(df_matches_clean)
    
        
        
    
        
   
    
    
   



if __name__ == "__main__":
    main()
