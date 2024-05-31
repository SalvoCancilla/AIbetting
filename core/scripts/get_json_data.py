import os
import json
import requests
import pandas as pd
import sqlite3
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("RAPIDAPI_KEY")
api_host = os.getenv("RAPIDAPI_HOST")



class GetHistoricData():
    def __init__(self,season):
        self.base_dir = os.getenv("BASE_DIR")
        self.db_path = os.getenv("DB_PATH")
        self.json_path = os.getenv("JSON_PATH")        
        self.api_key = os.getenv("RAPIDAPI_KEY")
        self.api_host = os.getenv("RAPIDAPI_HOST")
        self.season = season
        


    def date_range(self):
        dates = pd.date_range(start=f"{self.season}-01-01", end=f"{self.season}-12-31", freq="D").to_frame(index=False, name="date")
        dates['date'] = pd.to_datetime(dates['date']).dt.date
        return dates
    
    
   


   

    def check_db_data(self, table_name, reference_table, reference_column):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Verifica se la tabella `table_name` esiste
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            data_info_exists = cursor.fetchone() is not None

            # Verifica se la tabella `reference_table` esiste
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{reference_table}';")
            reference_table_exists = cursor.fetchone() is not None

            if not reference_table_exists:
                raise ValueError(f"La tabella di riferimento {reference_table} non esiste nel database.")

            # Creazione della vista basata sull'esistenza della tabella `table_name`
            view_name = f"view_fixtures_no_{table_name}"
            if data_info_exists:
                query_teams = f"""
                    CREATE VIEW IF NOT EXISTS {view_name} AS 
                    SELECT {reference_column}
                    FROM {reference_table}
                    WHERE {reference_column} NOT IN (SELECT DISTINCT {reference_column} FROM {table_name});
                    """
                print(f"View created from {table_name} table")
            else:
                query_teams = f"""
                    CREATE VIEW IF NOT EXISTS {view_name} AS 
                    SELECT {reference_column}
                    FROM {reference_table};
                    """
                print(f"View created from {table_name} table.")

            cursor.execute(query_teams)
            conn.commit()

            # Ottenere i dati dalla vista
            query_view = f"SELECT {reference_column} FROM {view_name}"
            data = pd.read_sql_query(query_view, conn)

        except sqlite3.OperationalError as e:
            print(f"Errore operativo SQLite: {e}")
            raise e  # Rilancia l'errore dopo averlo stampato per ulteriore gestione

        finally:
            # Elimina la vista
            try:
                cursor.execute(f"DROP VIEW IF EXISTS {view_name};")
                conn.commit()
                print(f"View {view_name} dropped")
            except sqlite3.OperationalError as e:
                print(f"Errore durante l'eliminazione della vista {view_name}: {e}")
            finally:
                conn.close()

        return data

    
    
    # Funzione d' appoggio per salvare i dati in un file JSON che posso consultare serapatamente per capire come sono annidati i dati
    def save_json(self, json_data, file_name):
        file_path = os.path.join(self.json_path, file_name)
        file_path = os.path.normpath(file_path)  
        with open(file_path, 'w') as file:
            json.dump(json_data, file, indent=4)
        print(f"JSON data saved to file: {file_path}")  

    
    
    def make_api_call(self, url):
        headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Solleva un'eccezione per risposte HTTP errate (es. 400, 500, etc.)
            print(f"API call done for {url}")
            # Verifica che la risposta contenga dati JSON validi e non vuoti
            data = response.json()
            if data['results'] == 0: 
                print(f"No valid data available for {url}")
                return None  # Nessun dato utile, ritorna None
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"ERROR: API call to {url} failed with exception: {e}")
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
        url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups?fixture={matches}"
        return self.make_api_call(url)


    def match_statistics(self, matches):
        url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics?fixture={matches}"
        return self.make_api_call(url)
    
 