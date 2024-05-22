import os
import json
import requests
import pandas as pd
import sqlite3
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("RAPIDAPI_KEY")
api_host = os.getenv("RAPIDAPI_HOST")



class GetHistoricData:
    def __init__(self, season, max_api_calls):
        self.base_dir = os.getenv("BASE_DIR")
        self.db_path = os.getenv("DB_PATH")
        self.json_path = os.getenv("JSON_PATH")        
        self.season = season
        self.max_api_calls = max_api_calls
        self.api_call_count = 0
        self.api_key = os.getenv("RAPIDAPI_KEY")
        self.api_host = os.getenv("RAPIDAPI_HOST")


    def date_range(self):
        dates = pd.date_range(start=f"{self.season}-01-01", end=f"{self.season}-12-31", freq="D").to_frame(index=False, name="date")
        dates['date'] = pd.to_datetime(dates['date']).dt.date
        return dates
    
    
    def _check_api_limit(self):
        if self.api_call_count >= self.max_api_calls:
            print("Reached maximum API call limit")
            return True
        return False
    
    
    def fixtrures_no_update(self, table_name=str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name};")
        data_info_exists = cursor.fetchone() is not None
        
        if data_info_exists:
            query_teams = f"""
                CREATE VIEW IF NOT EXISTS view_fixtures_no_{table_name} AS 
                SELECT fixture_id
                FROM matches_info
                WHERE fixture_id NOT IN (SELECT DISTINCT fixture_id FROM {table_name}_info);
                """
            print(f"View created from {table_name}_info table")
        else:
            query_teams = f"""
                CREATE VIEW IF NOT EXISTS view_fixtures_no_{table_name} AS 
                SELECT fixture_id
                FROM matches_info;
                """
            print(f"View created from matches_info table.")
        
        cursor.execute(query_teams)
        conn.commit()

        # Get fixtures without data
        query_view = f"SELECT fixture_id FROM view_fixtures_no_{table_name}"
        data = pd.read_sql_query(query_view, conn)
        conn.close()
        return data
    

    def save_json(self, json_data, file_name):
        file_path = os.path.join(self.json_path, file_name)
        file_path = os.path.normpath(file_path)  # Normalizza il percorso
        with open(file_path, 'w') as file:
            json.dump(json_data, file, indent=4)
        print(f"JSON data saved to file: {file_path}")  # Mostra il percorso normalizzato

    
    
    def make_api_call(self, url):
        if not self._check_api_limit():
            headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                if not response.content:  # Check if response content is empty
                    print(f"Empty response for {url}")
                    return None
                print(f"API call done for {url}")
                self.api_call_count += 1
                print(f"Number of API calls done: {self.api_call_count}")
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"ERROR: API call to {url} failed with exception: {e}")
                return None
        else:
            print("API call limit reached")
            return None
    
    
    def countries_info(self):
        url = "https://api-football-v1.p.rapidapi.com/v3/countries"
        return self.make_api_call(url)


    def venues_info(self, country_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/venues?country={country_id}"
        return self.make_api_call(url)

    
    def leagues_info(self, country_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/leagues/?season={self.season}&country={country_id}"
        return self.make_api_call(url)

    
    def teams_info(self, country_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/teams?country={country_id}"
        return self.make_api_call(url)

    
    def players_info(self, team_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/players?season={self.season}&team={team_id}"
        return self.make_api_call(url)


    def matches_info(self, date): 
        url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures?date={date}"
        return self.make_api_call(url)

    
    def lineups_info(self, matches):
        url = f"https://api-football-v1.p.rapidapi.com/v3/lineups?fixture={matches}"
        return self.make_api_call(url)
