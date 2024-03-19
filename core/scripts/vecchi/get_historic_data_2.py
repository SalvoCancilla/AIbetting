from dotenv import load_dotenv
import os
import pandas as pd
import sqlite3
import requests
import json
from pandas import json_normalize


load_dotenv()  # Load env. variables from .env file
api_key = os.getenv("RAPIDAPI_KEY")
api_host = os.getenv("RAPIDAPI_HOST")
json_path = os.getenv("json_path")


class GetHistoricData:
    
    def __init__(self, season):
        self.db_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/AiBetting.db"
        self.json_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/json_files"
        self.season = season  
        
        
        
        
        
        
    """
    Questo metodo restituisce le informazioni sui paesi per un dato anno.
    Fa una sola chiamata API
    Salva i dati in un file json nella cartella data/json_files/2020...    
    """     
    def get_countries_info(self): 
        api_call_count = 0 # Counter for API calls      
        url = "https://api-football-v1.p.rapidapi.com/v3/countries"
        headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}
        
        response = requests.get(url, headers=headers)  
        if response.status_code == 200:
            api_call_count += 1
            json_data_countries = response.json()
            print("API CALL DONE FOR: countries info")
            print("TOTAL API CALLS FOR COUNTRIES: " + str(api_call_count))
                
            # crea una cartella in data/json_files per ogni stagione e salva il file json
            if not os.path.exists(self.json_path + "/" + self.season):
                os.makedirs(self.json_path + "/" + self.season)
            with open(self.json_path + "/" + self.season + "/countries.json", "w") as outfile:
                json.dump(json_data_countries, outfile)
                print("JSON FILE SAVED FOR: countries info")
        
        else:
            print("ERROR: API call for countries info failed")
    
    
    
    
    
    
    """
    Questo metodo restituisce le informazioni sugli stadi per ogni paese.
    Fa una chiamata API per ogni paese (168)
    Salva i dati in un file json nella cartella data/json_files/season    
    """     
    def get_venues_info(self): 
        api_call_count = 0 # Counter for API calls
        countries = pd.read_sql_query("SELECT * FROM countries_info", sqlite3.connect(self.db_path)) 
        all_venues_data = []  # Accumulator for venues data
        
        for country in countries["country_name"]:
            url = f"https://api-football-v1.p.rapidapi.com/v3/venues?country={country}"
            headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}
        
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                print("API CALL DONE FOR: venues info")
                api_call_count += 1
                json_data_venues = response.json()
                            
                # Append each response to the accumulator
                all_venues_data.append(json_data_venues)
                
            else:
                print("ERROR: API call for venues info failed")
                
        # Stampa il totale delle chiamate API dopo il completamento del ciclo
        print("TOTAL API CALLS FOR VENUES: " + str(api_call_count)) 
        
        # crea una cartella in data/json_files per ogni stagione e salva il file json
        if not os.path.exists(self.json_path + "/" + self.season):
            os.makedirs(self.json_path + "/" + self.season)
                
        with open(self.json_path + "/" + self.season + "/venues.json", "w") as outfile:
            json.dump(all_venues_data, outfile)  
            print("JSON FILE SAVED FOR: venues info")
            
            
            
            
            
            
            
    """
    Questo metodo restituisce le informazioni sulle leghe per una determinata stagione, per ogni paese.
    Fa una chiamata API per ogni paese (168)
    Salva i dati in un file json nella cartella data/json_files/season    
    """        
    def get_leagues_info(self): 
        api_call_count = 0 # Counter for API calls
        max_api_calls = 7000 # API limit
        countries = pd.read_sql_query("SELECT * FROM countries_info", sqlite3.connect(self.db_path))
        all_leagues_data = []  # Accumulator for leagues data
        
        for country in countries["country_name"]:
            if api_call_count >= max_api_calls:
                print("Raggiunto il limite massimo di chiamate API.")
                break
        
            url = f"https://api-football-v1.p.rapidapi.com/v3/leagues/?season={self.season}&country={country}"
            headers = {'x-rapidapi-key': api_key,
                       'x-rapidapi-host': api_host
                       }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                print("API CALL DONE FOR: leagues info")
                api_call_count += 1
                json_data_leagues = response.json()
                # Append each response to the accumulator
                all_leagues_data.append(json_data_leagues)
                
            else:
                print("ERROR: API call for leagues info failed")
                
        # Stampa il totale delle chiamate API dopo il completamento del ciclo
        print("TOTAL API CALLS FOR LEAGUES: " + str(api_call_count))

        # crea una cartella in data/json_files per ogni stagione e salva il file json
        if not os.path.exists(self.json_path + "/" + self.season):
            os.makedirs(self.json_path + "/" + self.season)
            
        with open(self.json_path + "/" + self.season + "/leagues.json", "w") as outfile:
            json.dump(all_leagues_data, outfile)  
            print("JSON FILE SAVED FOR: leagues info")
            
            
            
            
            
            
    """
    Questo metodo restituisce le informazioni sulle squadre per una determinata stagione, per ogni lega.
    Fa una chiamata API per ogni lega (758)
    Salva i dati in un file json nella cartella data/json_files/season    
    """     
    def get_teams_info(self): 
        api_call_count = 0 # Counter for API calls
        max_api_calls = 3000 # API limit
        leagues = pd.read_sql_query("SELECT * FROM leagues_info", sqlite3.connect(self.db_path))
        all_teams_data = []  # Accumulator for teams data
    
        for league in leagues["league_id"]:
            if api_call_count >= max_api_calls:
                print("Raggiunto il limite massimo di chiamate API.")
                break
    
            url = f"https://api-football-v1.p.rapidapi.com/v3/teams?season={self.season}&league={league}"
            headers = {'x-rapidapi-key': api_key,'x-rapidapi-host': api_host}
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                print("API CALL DONE FOR: teams info")
                api_call_count += 1
                json_data_teams = response.json()
                # Append each response to the accumulator
                all_teams_data.append(json_data_teams)
            
            else:
                print("ERROR: API call for teams info failed")
            
        # Stampa il totale delle chiamate API dopo il completamento del ciclo
        print("TOTAL API CALLS FOR TEAMS: " + str(api_call_count))

        # crea una cartella in data/json_files per ogni stagione e salva il file json
        if not os.path.exists(self.json_path + "/" + self.season):
            os.makedirs(self.json_path + "/" + self.season)
        
        with open(self.json_path + "/" + self.season + "/teams.json", "w") as outfile:
            json.dump(all_teams_data, outfile)  
            print("JSON FILE SAVED FOR: teams info")
    
    
    
    
    
    # ok
    def get_players_info(self): 
        api_call_count = 0 # Counter for API calls
        max_api_calls = 7000 # API limit
        teams = pd.read_sql_query("SELECT * FROM teams_info", sqlite3.connect(self.db_path))
        all_players_data = []
        
        for team in teams["team_id"]:
            if api_call_count >= max_api_calls:
                print("Raggiunto il limite massimo di chiamate API.")
                break
            
            url = f"https://api-football-v1.p.rapidapi.com/v3/players/squads?team={team}"
            headers = {'x-rapidapi-key': api_key,'x-rapidapi-host': api_host}
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                print("API CALL DONE FOR: players info")
                api_call_count += 1
                json_data_players = response.json()
                # Append each response to the accumulator
                all_players_data.append(json_data_players)
            
            else:
                print("ERROR: API call for players info failed")
            
        # Stampa il totale delle chiamate API dopo il completamento del ciclo
        print("TOTAL API CALLS FOR PLAYERS: " + str(api_call_count))
        
        # crea una cartella in data/json_files per ogni stagione e salva il file json
        if not os.path.exists(self.json_path + "/" + self.season):
            os.makedirs(self.json_path + "/" + self.season)
            
        with open(self.json_path + "/" + self.season + "/players.json", "w") as outfile:
            json.dump(all_players_data, outfile)  
            print("JSON FILE SAVED FOR: players info")
    
    
    
    
    
    
    # ok
    def get_matches_info(self):
        api_call_count = 0 # Counter for API calls
        max_api_calls = 1000 # API limit
        dates = pd.date_range(start=f"{self.season}-01-01", end=f"{self.season}-12-31", freq="D").to_frame(index=False, name="date") #Create a series of dates form start to end of the 2020 year
        dates['date'] = pd.to_datetime(dates['date']).dt.date # Covert the date in datetime format
        all_matches_data =[]
        print(dates)
        
        for date in dates['date']:
            if api_call_count >= max_api_calls:
                print("Raggiunto il limite massimo di chiamate API.")
                break
            
            url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures?date={date}"
            headers = {'x-rapidapi-key': api_key,'x-rapidapi-host': api_host}
            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                print("API CALL DONE FOR: match info " + "(date:" + str(date) + ")")
                api_call_count += 1
                json_data_matches = response.json()
                all_matches_data.append(json_data_matches['response'])
                
            else:
                print(f"Failed to get data for matches: {response.status_code}")
                
        print("TOTAL API CALLS FOR MATCHES: " + str(api_call_count))
        print(f"Last date called: {date}")
        
        # crea una cartella in data/json_files per ogni stagione e salva il file json
        if not os.path.exists(self.json_path + "/" + self.season):
            os.makedirs(self.json_path + "/" + self.season)
            
        with open(self.json_path + "/" + self.season + "/matches.json", "w") as outfile:
            json.dump(all_matches_data, outfile)  
            print("JSON FILE SAVED FOR: matches info")
            
    
    
    
    
    #ok
    def get_lineups(self):
        with sqlite3.connect(self.db_path) as conn:
            # Check if lineups_info table exists
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='lineups_info';")
            lineups_info_exists = cursor.fetchone() is not None

            # IF YES create a temporary view based on lineups_info table to take fixtures_id without lineups data
            if lineups_info_exists:
                query_teams = """
                    CREATE VIEW view_fixtures_no_lineups AS 
                    SELECT fixture_id
                    FROM matches_info
                    WHERE fixture_id NOT IN (SELECT DISTINCT fixture_id FROM lineups_info);
                """
                print("View created from lineups_info table.")
            # IF NO create a temporary view based on matches_info table to take fixtures_id without lineups data
            else:
                query_teams = """
                    CREATE VIEW view_fixtures_no_lineups AS 
                    SELECT fixture_id
                    FROM matches_info;
                """
                print("View created from matches_info table.")
            conn.execute(query_teams)

            # print the view
            query_view = "SELECT * FROM view_fixtures_no_lineups"
            matches = pd.read_sql_query(query_view, conn)
            
            
            # Get fixtures without lineups data
            query_view = "SELECT * FROM view_fixtures_no_lineups"
            matches = pd.read_sql_query(query_view, conn)
            

            # API call to get lineups data for each fixture
            max_api_calls = 300
            api_call_count = 0
            all_lineups_data = []

            for fixture_id in matches['fixture_id']:
                if api_call_count >= max_api_calls:
                    print("Raggiunto il limite massimo di chiamate API.")
                    break

                url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups?fixture={fixture_id}"
                headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}

                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    print("API CALL DONE FOR: match info lineups")
                    api_call_count += 1
                    json_data_lineups = response.json()
                    all_lineups_data.append(json_data_lineups)
                else:
                    print(f"Failed to get data for lineups: {response.status_code}")

            print("TOTAL API CALLS FOR LINEUPS: " + str(api_call_count))

            # Save the new data to a temporary JSON file
            with open(f"{self.json_path}/{self.season}/lineups.json", "w") as outfile:
                json.dump(all_lineups_data, outfile)
                print("JSON FILE SAVED FOR: lineups updated info")

            # Drop existing view
            conn.execute("DROP VIEW IF EXISTS view_fixtures_no_lineups;")
            print("View dropped.")
            
    
    
    
    
    #ok
    def get_players_stats(self):
        with sqlite3.connect(self.db_path) as conn:
            # Check if players_stats table exists
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='players_stats';")
            players_stats_exists = cursor.fetchone() is not None

            # IF YES create a temporary view based on "players_stats table" to take fixtures_id without players statistics data
            if players_stats_exists:
                query_teams = """
                    CREATE VIEW view_fixtures_no_players_stats AS 
                    SELECT fixture_id
                    FROM matches_info
                    WHERE fixture_id NOT IN (SELECT DISTINCT fixture_id FROM players_stats);
                """
            # IF NOT create a temporary view based on "matches_info table" to take fixtures_id without lineups data
            else:
                query_teams = """
                    CREATE VIEW view_fixtures_no_players_stats AS 
                    SELECT fixture_id
                    FROM matches_info;
                """
            conn.execute(query_teams)
            print("View created.")

            # Get fixtures without lineups data
            query_view = "SELECT * FROM view_fixtures_no_players_stats"
            matches = pd.read_sql_query(query_view, conn)

            # API call to get lineups data for each fixture
            max_api_calls = 50
            api_call_count = 0
            all_players_stats = []

            for fixture_id in matches['fixture_id']:
                if api_call_count >= max_api_calls:
                    print("Raggiunto il limite massimo di chiamate API.")
                    break

                url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures/players?fixture={fixture_id}"
                headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}

                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    print("API CALL DONE FOR: players stats")
                    api_call_count += 1
                    json_data_players_stats = response.json()
                    all_players_stats.append(json_data_players_stats)
                else:
                    print(f"Failed to get data for players statistics: {response.status_code}")

            print("TOTAL API CALLS FOR PLAYERS STATISTICS: " + str(api_call_count))

            # Save the new data to a temporary JSON file
            with open(f"{self.json_path}/{self.season}/players_stats.json", "w") as outfile:
                json.dump(all_players_stats, outfile)
                print("JSON FILE SAVED FOR: Players statistics")

            # Drop existing view
            conn.execute("DROP VIEW IF EXISTS view_fixtures_no_players_stats;")
            print("View dropped.")
    
    
    
    
            
            




 
    
                                                     
            
        
        
        
            
        