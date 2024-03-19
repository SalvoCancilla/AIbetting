from dotenv import load_dotenv
import os
import pandas as pd
from pandas import json_normalize
import sqlite3
import requests
import json

load_dotenv()  # Load env. variables from .env file
api_key = os.getenv("RAPIDAPI_KEY")
api_host = os.getenv("RAPIDAPI_HOST")





class GetHistoricData:
    def __init__(self, season, max_api_calls=100):
        self.db_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/Ai_Betting.db"
        self.json_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/json_files"
        self.season = season
        self.max_api_calls = max_api_calls
        self.api_call_count = 0



   
    def _make_api_call(self, url):
        headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print(f"API call done for {url}")
            return response.json()
        else:
            print(f"ERROR: API call to {url} failed with status code {response.status_code}")
            return None



    def _save_json_data(self, data, filename):
        filepath = os.path.join(self.json_path, self.season, filename)
        existing_data = self._load_existing_json_data(filename)
        
        # If YES, append the new data to the existing data
        if existing_data:
            existing_data.update(data) 
            with open(filepath, "w") as outfile:
                json.dump(existing_data, outfile)
                print(f"JSON FILE UPDATED FOR: {filename}")
        
        # If NO, create a new directory forr the season and save the data 
        else:
            os.makedirs(os.path.join(self.json_path, self.season), exist_ok=True) 
            with open(filepath, "w") as outfile:
                json.dump(data, outfile)
                print(f"JSON FILE CREATED FOR: {filename}") 



    def _load_existing_json_data(self, filename):
        filepath = os.path.join(self.json_path, self.season, filename)
        with open(filepath, "r") as infile:
            return json.load(infile)
        



    def save_to_db(self, df, table_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'") # Check if the table allredy exists
        result = cursor.fetchone()

        if result is None:
            df.to_sql(table_name, conn, index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()
        
        

    """ _______________________ FUNZIONI PER NAZIONE _______________________ """
    
    
    def get_countries_info(self): 
        existing_data = self._load_existing_json_data("countries.json")
        url = "https://api-football-v1.p.rapidapi.com/v3/countries"
        json_data_countries = self._make_api_call(url)
        if json_data_countries:
            self._save_json_data(json_data_countries, "countries.json")
            self.api_call_count += 1
        if existing_data:
            existing_data.update(json_data_countries)
            return existing_data
        else:
            return json_data_countries



    def normalize_countries_data(self):
        # Load JSON file from data/json_files/season
        with open(os.path.join(self.json_path, "countries.json")) as json_file:
            json_countries_data = json.load(json_file)
            print("JSON file loaded for: countries info")

        # Normalizza il file JSON e restituisce il DataFrame
        df_countries = json_normalize(json_countries_data["response"])
        print("JSON file normalized for: countries info")

        return df_countries


    def clean_countries_data(self, df_countries):
        df_countries.columns = ['country_' + col for col in df_countries.columns] # Aggiungi il prefisso 'country_' a ciascuna colonna
        print("Columns cleaned for: countries info")
        table_name = "countries_info"
        return df_countries , table_name
    

    





    """ _______________________ FUNZIONI PER STADI _______________________"""

    def get_venues_info(self): 
        existing_data = self._load_existing_json_data("venues.json")
        if not existing_data:
            existing_data = {}
        countries = pd.read_sql_query("SELECT * FROM countries_info", sqlite3.connect(self.db_path)) 
        all_venues_data = {}
        for country in countries["country_name"]:
            url = f"https://api-football-v1.p.rapidapi.com/v3/venues?country={country}"
            json_data_venues = self._make_api_call(url)
            if json_data_venues:
                all_venues_data.extend(json_data_venues)  # Estendi la lista existing_data
                self.api_call_count += 1
        existing_data.update(all_venues_data)  # Estendi la lista existing_data
        self._save_json_data(existing_data, "venues.json")
        return existing_data  # Restituisci existing_data



    def normalize_venues_data(self):
        # Load JSON file from data/json_files/season
        with open(os.path.join(self.json_path, "venues.json")) as json_file:
            json_countries_data = json.load(json_file)
            print("JSON file loaded for: venues info")

        # Normalizza il file JSON e restituisce il DataFrame
        df_countries = json_normalize(json_countries_data["response"])
        print("JSON file normalized for: venues info")

        return df_countries


    
    def clean_venues_data(self, df_venues):
        df_venues.columns = ['venue_' + col for col in df_venues.columns] # Aggiungi il prefisso 'venue_' a ciascuna colonna
        print("Columns cleaned for: venues info")
        table_name = "venues_info"
        return df_venues, table_name
    
    
    
    

    def get_leagues_info(self): 
        existing_data = self._load_existing_json_data("leagues.json")
        countries = pd.read_sql_query("SELECT * FROM countries_info", sqlite3.connect(self.db_path))
        all_leagues_data = []
        for country in countries["country_name"]:
            url = f"https://api-football-v1.p.rapidapi.com/v3/leagues/?season={self.season}"
            json_data_leagues = self._make_api_call(url)
            if json_data_leagues:
                all_leagues_data.append(json_data_leagues)
                self.api_call_count += 1
        existing_data += all_leagues_data
        self._save_json_data(existing_data, "leagues.json")
        return existing_data + json_data_leagues



    def get_teams_info(self): 
        existing_data = self._load_existing_json_data("teams.json")
        leagues = pd.read_sql_query("SELECT * FROM leagues_info", sqlite3.connect(self.db_path))
        all_teams_data = []
        for league in leagues["league_id"]:
            url = f"https://api-football-v1.p.rapidapi.com/v3/teams?season={self.season}"
            json_data_teams = self._make_api_call(url)
            if json_data_teams:
                all_teams_data.append(json_data_teams)
                self.api_call_count += 1
        existing_data += all_teams_data
        self._save_json_data(existing_data, "teams.json")
        return existing_data



    def get_players_info(self): 
        existing_data = self._load_existing_json_data("players.json")
        teams = pd.read_sql_query("SELECT * FROM teams_info", sqlite3.connect(self.db_path))
        all_players_data = []
        for team in teams["team_id"]:
            url = f"https://api-football-v1.p.rapidapi.com/v3/players/squads?team={team}"
            json_data_players = self._make_api_call(url)
            if json_data_players:
                all_players_data.append(json_data_players)
                self.api_call_count += 1
        existing_data += all_players_data
        self._save_json_data(existing_data, "players.json")
        return existing_data



    def get_matches_info(self):
        existing_data = self._load_existing_json_data("matches.json")
        dates = pd.date_range(start=f"{self.season}-01-01", end=f"{self.season}-12-31", freq="D").to_frame(index=False, name="date")
        dates['date'] = pd.to_datetime(dates['date']).dt.date
        all_matches_data = []
        for date in dates['date']:
            url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures?date={date}"
            json_data_matches = self._make_api_call(url)
            if json_data_matches:
                all_matches_data.append(json_data_matches['response'])
                self.api_call_count += 1
        existing_data += all_matches_data
        self._save_json_data(existing_data, "matches.json")
        return existing_data



    



   

if __name__ == "__main__":

    season = "2020"  # Imposta la stagione desiderata
    max_api_calls = 1000  # Imposta il numero massimo di chiamate API consentite
    getData = GetHistoricData(season, max_api_calls)
    
    getData.get_countries_info()
    getData.get_venues_info()
    getData.get_leagues_info()
    getData.get_teams_info()
    getData.get_players_info()
    getData.get_matches_info()
    #getData.get_lineups()
    #getData.get_players_stats()
    print("API CALLS DONE: ", getData.api_call_count)
