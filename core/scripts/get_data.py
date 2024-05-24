import sys
import os
import sqlite3
import pandas as pd
import numpy as np


from ..scripts.get_json_data import GetHistoricData
from ..scripts.norm_data import NormData
from ..scripts.clean_data import CleanData
from ..scripts.save_data import SaveDataToDB




class GetData ():
    def __init__(self, season, api_limit):
        self.season = season
        self.api_limit = api_limit
        self.api_call_count = 0
        self.db_path = os.getenv("DB_PATH")
        self.get_json = GetHistoricData(season=self.season)
        self.norm = NormData()
        self.clean = CleanData()
        self.save = SaveDataToDB()
    
    
    def _check_api_limit(self):
        if self.api_call_count >= self.api_limit:
            print("Reached maximum API call limit")
            return True
        else:
            self.api_call_count += 1
            print(f"API call count: {self.api_call_count}")
            return False
    
    
    
    def country_data(self): # 1 api call
        json_data = self.get_json.countries_info()
        normalized_data = self.norm.countries_info(json_data)
        cleaned_data = self.clean.countries_info(normalized_data)
        self.save.countries(cleaned_data)
        print("COUNTRIES INFO SAVED TO DATABASE")
        print("\n") 
        return True
        
        
     
    def venues_data(self): # 60 api call
        conn = sqlite3.connect(self.db_path)
        countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn)
        conn.close()

        for country_id in countries['country_name']:
            if self._check_api_limit():
                print("API call limit reached, stopping further data processing.")
                break # Interrompe il ciclo se il limite di API è stato raggiunto
            
            json_data = self.get_json.venues_info(country_id)
            if json_data is None:
                print(f"Skipping data processing for {country_id} due to no data.")
                continue  # Salta al prossimo paese se non ci sono dati
            
            self.get_json.save_json(json_data, "venues_info.json")
            normalized_data = self.norm.venues_info(json_data)
            cleaned_data = self.clean.venues_info(normalized_data)
            self.save.venues(cleaned_data)
            print(f"VENUES INFO SAVED TO DATABASE FOR {country_id}")
            print("\n") 

        return True

            
    
    def leagues_data(self): # 171 api calls
        conn = sqlite3.connect(self.db_path)
        countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn)
        conn.close()

        for country_id in countries['country_name']:
            if self._check_api_limit():
                print("API call limit reached, stopping further data processing.")
                break
            
            json_data = self.get_json.leagues_info(country_id)
            if json_data is None:
                print(f"Skipping data processing for {country_id} due to no data.")
                continue
            
            self.get_json.save_json(json_data, "leagues_info.json")
            normalized_data = self.norm.leagues_info(json_data)
            cleaned_data = self.clean.leagues_info(normalized_data)
            self.save.leagues(cleaned_data)
            print(f"LEAGUES INFO SAVED TO DATABASE FOR {country_id}")
            print("\n")
            
        return True
        
        
   
    def teams_data(self): # 171 api calls
        conn = sqlite3.connect(self.db_path)
        countries = pd.read_sql_query("SELECT DISTINCT country_name FROM countries_info", conn)
        conn.close()

        for country_id in countries['country_name']:
            if self._check_api_limit():
                print("API call limit reached, stopping further data processing.")
                break
                
            json_data = self.get_json.teams_info(country_id)
            if json_data is None:
                print(f"Skipping data processing for {country_id} due to no data.")
                continue
                
            self.get_json.save_json(json_data, "teams_info.json")
            normalized_data = self.norm.teams_info(json_data)
            cleaned_data = self.clean.teams_info(normalized_data)
            self.save.teams(cleaned_data)
            print(f"TEAMS INFO SAVED TO DATABASE FOR {country_id}")
            print("\n")
                
        return True
    
    
    def players_data(self):
        conn = sqlite3.connect(self.db_path)
        teams = pd.read_sql_query("SELECT DISTINCT team_id FROM teams_info", conn)
        conn.close()

        for team_id in teams['team_id']:
            if self._check_api_limit():
                print("API call limit reached, stopping further data processing.")
                break
                
            json_data = self.get_json.players_info(team_id)
            if json_data is None:
                print(f"Skipping data processing for {team_id} due to no data.")
                continue
                
            self.get_json.save_json(json_data, "players_info.json")
            normalized_data = self.norm.players_info(json_data)
            cleaned_data = self.clean.players_info(normalized_data)
            self.save.players(cleaned_data)
            print(f"PLAYERS INFO SAVED TO DATABASE FOR {team_id}")
            print("\n")
                
        return True
    
    
    
    
    
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