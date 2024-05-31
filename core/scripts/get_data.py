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
        teams = self.get_json.check_db_data(table_name="players_info", reference_table="teams_info", reference_column="team_id")

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
    
    
    
    def matches_data(self): #365 api calls
        dates = self.get_json.date_range()
        
        for date in dates['date']:
            if self._check_api_limit():
                print("API call limit reached, stopping further data processing.")
                break
                
            json_matches = self.get_json.matches_info(date)
            if json_matches["results"] == 0:
                continue
            
            self.get_json.save_json(json_matches, "matches_info.json")
            normalized_data = self.norm.matches_info(json_matches)
            cleaned_data = self.clean.matches_info(normalized_data)    
            self.save.matches(cleaned_data)
            print(f"MATCHES INFO SAVED TO DATABASE FOR {date}")
            print("\n")
            
        return True
    
    
    
    
 
    def lineups_data(self):
        matches_home = self.get_json.check_db_data(table_name="home_lineups_info", reference_table="matches_info", reference_column="fixture_id")
        matches_away = self.get_json.check_db_data(table_name="away_lineups_info", reference_table="matches_info", reference_column="fixture_id")
        
        def process_lineups(matches: pd.DataFrame, lineup_type: str, lineup_info_func: callable, save_func: callable,) -> None:
            for match_id in matches['fixture_id']:
                if self._check_api_limit():
                    print("API call limit reached, stopping further data processing.")
                    break
                
                json_data = self.get_json.lineups_info(match_id)
                if json_data is None:
                    print(f"Skipping data processing for {match_id} due to no data.")
                    continue
                
                filename = f"{lineup_type}_lineups_info.json"
                self.get_json.save_json(json_data, filename)
                
                normalized_data = lineup_info_func(json_data)
                save_func(normalized_data)
                
                print(f"{lineup_type.upper()}LINEUPS INFO SAVED TO DATABASE FOR {match_id}\n")

        process_lineups(matches_home, "home", self.norm.home_lineups_info, self.save.home_lineups,)
        process_lineups(matches_away, "away", self.norm.away_lineups_info, self.save.away_lineups)
        
        return True
    
    
    
    def match_stats_home(self):
        matches = self.get_json.check_db_data(table_name="match_statistics_home", reference_table="matches_info", reference_column="fixture_id")
        
        for match_id in matches['fixture_id']:
            if self._check_api_limit():
                print("API call limit reached, stopping further data processing.")
                break
                
            json_data = self.get_json.match_statistics(match_id)
            if json_data is None:
                print(f"Skipping data processing for {match_id} due to no home data.")
                continue
                
            self.get_json.save_json(json_data, "match_statistics.json")
            normalized_data = self.norm.match_statistics_home(json_data)
            print(normalized_data)
            cleaned_data = self.clean.match_statistics_home(normalized_data)
            self.save.home_stats(cleaned_data)
            print("\n")
            
        return True
    
    
    
    def match_stats_away(self):
        matches = self.get_json.check_db_data(table_name="match_statistics_away", reference_table="matches_info", reference_column="fixture_id")
        
        for match_id in matches['fixture_id']:
            if self._check_api_limit():
                print("API call limit reached, stopping further data processing.")
                break
                
            json_data = self.get_json.match_statistics(match_id)
            if json_data is None:
                print(f"Skipping data processing for {match_id} due to no away data.")
                continue
                
            self.get_json.save_json(json_data, "match_statistics.json")
            normalized_data = self.norm.match_statistics_away(json_data)
            cleaned_data = self.clean.match_statistics_away(normalized_data)
            self.save.away_stats(cleaned_data)
            print("\n")
            
        return True
    
    
