import sys
import os
import sqlite3
import pandas as pd
import numpy as np


from ..scripts.get_json_data import GetHistoricData
from ..scripts.norm_data import NormData
from ..scripts.clean_data import CleanData
from ..scripts.save_data import SaveDataToDB

get_json=GetHistoricData(2020,10)
norm=NormData()
clean=CleanData()
save=SaveDataToDB()

class GetData ():
    def __init__(self, season, api_limit):
        self.season = season
        self.api_limit = api_limit
        self.db_path = os.getenv("DB_PATH")
    
    
    def country_data(self):
        # 1 chiamata API
        json_data = get_json.countries_info()
        normalized_data = norm.countries_info(json_data)
        cleaned_data = clean.countries_info(normalized_data)
        save.countries(cleaned_data)
        print("COUNTRIES INFO SAVED TO DATABASE")
        return True
        
        
     
    def venues_data(self):
        # 1 chiamata API
        # Prende i dati dei paesi dal database perché vanno dati all'endpoint per la chiamata API
        conn = sqlite3.connect(self.db_path)
        countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn)
        conn.close()

        # Per ogni paese prende i dati degli stadi
        for country_id in countries['country_name']:
            json_data = get_json.venues_info(country_id)
            # save_json(json_data, "venues_info.json")
            get_json.save_json(json_data, "venues_info.json")
            
            normalized_data = norm.venues_info(json_data)
            cleaned_data = clean.venues_info(normalized_data)
            save.venues(cleaned_data)
            print(f"VENUES INFO SAVED TO DATABASE FOR {country_id}")

        # Restituisce True solo dopo aver processato tutti i paesi
        return True
            
    
    def leagues_data(self):
        # ... chiamate API
        # Prende i dati dei paesi dal database perchè vanno dati all' endpoint per la chiamata API
        conn = sqlite3.connect(self.db_path)
        countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn)
        conn.close()
        # Per ogni paese prende i dati deglle leghe
        for country_id in countries['country_name']:
            json_data = get_json.leagues_info(country_id)
            print(json_data)
            normalized_data = norm.leagues_info(json_data)
            print(normalized_data)
            cleaned_data = clean.leagues_info(normalized_data)
            print(cleaned_data)
            save.leagues(cleaned_data)
            print("LEAGUES INFO SAVED TO DATABASE")
        return True
        
        
   
    
    
    
    
    
    
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
        if json_players["results"] == 0:
            continue
        
        get.save_json(json_players, "players_info.json")
        
        df_players = norm.norm_players_info(json_players)
        
        df_players_clean = clean.clean_players_info(df_players)
        
        save.players_info_to_db(df_players_clean)
    """
    
    """
    # Matches info  
    dates = get.date_range()
      
    for date in dates['date']:
        if get._check_api_limit():
            break
        
        json_matches = get.get_matches_info(date)
        if json_matches["results"] == 0:
            continue
        
        df_matches = norm.norm_matches_info(json_matches)
        print(df_matches)
        
        df_matches_clean = clean.clean_matches_info(df_matches)
        print(df_matches_clean)
        
        save.matches_info_to_db(df_matches_clean)
        
        
        
    # Lineups info
        fixtures = pd.read_sql_query("SELECT DISTINCT fixture_id FROM matches_info", sqlite3.connect(db_path))   
        
    """    