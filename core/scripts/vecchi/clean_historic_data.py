from dotenv import load_dotenv
import os
import pandas as pd
import sqlite3
import requests
import json
from pandas import json_normalize
import pandas as pd

# Load env. variables from .env file
load_dotenv()
api_key = os.getenv("RAPIDAPI_KEY")
api_host = os.getenv("RAPIDAPI_HOST")
db_path = os.getenv("db_path")
json_path = os.getenv("json_path")





class NormalizeJsonData:
    
    def __init__(self, season):
        self.season = season 
        self.json_path = json_path + "/" + self.season
        self.db_path = db_path
    
    
    
    
        
    def clean_countries_info(self): 
        
        # Load JSON file from data/json_files/season
        with open(self.json_path + "/countries.json") as json_file:
            json_countries_data = json.load(json_file)
            print("JSON file loaded for: countries info")
            
        # se la cartella season non è vuota, normalizza il file json e salva i dati in un dataframe
        if os.listdir(self.json_path):
            df_countries = json_normalize(json_countries_data["response"])
            print("JSON file normalized for: countries info")
            
            # Cleaning steps
            df_countries.columns = ['country_' + col for col in df_countries.columns] # Add prefix 'country_' for each column
            
            # Save data to DB
            conn = sqlite3.connect(self.db_path)
            df_countries.to_sql("countries_info", conn, if_exists="replace", index=False)
            print("Data saved to DB for: countries info")
            
        else:
            print("Error: JSON file not found for: countries")
    
    
    
    
    
    def clean_venues_info(self):
                  
        # Load JSON file from data/json_files/season
        with open(self.json_path + "/venues.json") as json_file:
            json_venues_data = json.load(json_file)
            print("JSON file loaded for: venues info")
            
            # Normalize JSON file
            df_venues = pd.json_normalize(json_venues_data, 'response', sep='_')
        
            # Cleaning steps
            df_venues.columns = ['venue_' + col for col in df_venues.columns] # Add prefix 'venue_' for each column
            df_venues = df_venues.drop(columns=['venue_address', 'venue_city']) # Drop columns "venue_address", "venue_city"
            
            # Save data to DB
            conn = sqlite3.connect(self.db_path)
            df_venues.to_sql("venues_info", conn, if_exists="replace", index=False)
            print("Data saved to DB for: venues info")
        
        
        
        
        
    def clean_leagues_info(self):
            
            # Load JSON file from data/json_files/season
            with open(self.json_path + "/leagues.json") as json_file:
                json_leagues_data = json.load(json_file)
                print("JSON file loaded for: leagues info")
                
                # Normalize JSON file
                df_leagues = pd.json_normalize(json_leagues_data, 'response', sep='_')
            
                # Cleaning steps
                df_leagues = df_leagues.drop(columns=['seasons']) # Drop column 'seasons'
                df_leagues = df_leagues.drop(columns=['country_code']) # Drop column country_code
                df_leagues = df_leagues.drop(columns=['country_flag']) # Drop column country_flag
                df_leagues['season'] = self.season # Add column season by self.season
                df_leagues = df_leagues.sort_values(by=['league_id'],ascending=True) # Sort by league_id ascending
                
                
                # Save data to DB
                conn = sqlite3.connect(self.db_path)
                df_leagues.to_sql("leagues_info", conn, if_exists="replace", index=False)
                print("Data saved to DB for: leagues info")
    
    
    
    
    
    def clean_teams_info(self):
            
            # Load JSON file from data/json_files/season
            with open(self.json_path + "/teams.json") as json_file:
                json_teams_data = json.load(json_file)
                print("JSON file loaded for: teams info")
                
                # Normalize JSON file
                df_teams = pd.json_normalize(json_teams_data, 'response', sep='_')
            
                # Cleaning steps
                df_teams = df_teams[df_teams.columns.drop(list(df_teams.filter(regex='venue')))] # Drop columns wich contains "venies" in the name
                df_teams['team_founded'] = df_teams['team_founded'].fillna(0) # Fill NaN values with 0 in column team_founded
                df_teams['team_founded'] = df_teams['team_founded'].astype(int) # Convert column team_founded from float to int
                df_teams = df_teams.drop_duplicates(subset=['team_id']) # Drop duplicate ieam_id
                df_teams = df_teams.sort_values(by=['team_id'],ascending=True) # Sort by team_id ascending
                
                
                # Save data to DB
                conn = sqlite3.connect(self.db_path)
                df_teams.to_sql("teams_info", conn, if_exists="replace", index=False)
                print("Data saved to DB for: teams info")
    
    
    
    
    
    
    def clean_players_info(self):
        
        # Load JSON file from data/json_files/season
        with open(self.json_path + "/players.json") as json_file:
            json_players_data = json.load(json_file)
            print("JSON file loaded for: players info")
            
            # Normalization steps
            df1 = pd.json_normalize(json_players_data, 'response', sep='_') # Utilizza json_normalize per estrarre le informazioni da 'response' e creare il DataFrame

            df_players = pd.json_normalize(df1['players']) # Normalize players column and create a new dataframe

            df_combined = pd.concat([df1, df_players], axis=1) # Merge the two dataframes

            # Drop unnecessary columns
            columns_to_drop = ['players', 'team_name', 'team_logo']
            df_combined = df_combined.drop(columns=columns_to_drop)

            df_players = pd.DataFrame() # Initialize the resulting dataframe

            # Loop through the columns of the combined dataframe
            for col in df_combined.columns[1:]:
                normalized_data = pd.json_normalize(df_combined[col]) # Normalize data in columns with dictionaries
    
                normalized_data['team_id'] = df_combined['team_id'] # Add the "team_id" column to the normalized data
    
                df_players = pd.concat([df_players, normalized_data], ignore_index=True) # Concatenate the result to the main dataframe
            
            # Cleaning steps
            df_players.columns = ['player_' + col for col in df_players.columns] # Rename all columns with prefix "player_"
            df_players = df_players.dropna(subset=['player_id']) # If player_id is null, drop the row
            df_players['player_id'] = df_players['player_id'].astype(int) # Player_id column to int
            df_players = df_players.sort_values(by=['player_id'],ascending=True) # Sort by player_id ascending
            
            #save data to DB
            conn = sqlite3.connect(self.db_path)
            df_players.to_sql("players_info", conn, if_exists="replace", index=False)
            print("Data saved to DB for: players info")
            
       
       
       
       
    #ok        
    def clean_matches_info(self):
        
        # Load JSON file from data/json_files/season
        with open(self.json_path + "/matches.json") as json_file:
            json_matches_data = json.load(json_file)
            print("JSON file loaded for: matches info")
            
            # Normalize json file
            df = pd.json_normalize(json_matches_data, sep = "_")
            # Since each record is a team and each column is a game, unpivot to have a record... a match
            df_unstacked = pd.melt(df, var_name='Index', value_name='Fixture_Info')
            df_unstacked = df_unstacked['Fixture_Info'] # Drop old column 'Index'
            # Normalizing the new unpivoted dataframe
            df_match = json_normalize(df_unstacked)
            
            # Rename colums
            df_match.rename(columns={'goals_home': 'total_goals_home',
                                     'goals_away': 'total_goals_away',
                                     'score_halftime_home': 'first_time_goals_home',
                                     'score_halftime_away': 'first_time_goals_away',
                                     'teams_home_id': 'home_team_id',
                                     'teams_away_id': 'away_team_id',
                                     'teams_home_name': 'home_team_name',
                                     'teams_away_name': 'away_team_name',
                                     'teams_home_winner': 'home_team_winner',
                                     'teams_away_winner': 'away_team_winner',
                                     'fixture_venue_id': 'venue_id',
                                     'fixture_status_long': 'status_long',
                                     'fixture_status_elapsed': 'status_elapsed'}, inplace=True)

            df_match['fixture_date'] = pd.to_datetime(df_match['fixture_date']) # Convert Complete_fixture_date in datetime
            df_match = df_match.sort_values(by=['fixture_id'],ascending=True) # order by fixture_id
            df_match = df_match.dropna(subset=['fixture_id']) # Drop rows where team_id is null
            df_match = df_match.drop_duplicates(subset=['fixture_id']) # Drop duplicates fixture_id

            # NEW COLUMNS
            # Hours
            df_match['hours'] = df_match['fixture_date'].dt.strftime('%H:%M')
    
            # New column Day
            df_match.insert(14, "day", df_match["league_round"].str.split("-").str[-1].str.strip())
                         
            # Second_time_goals_home/away
            df_match['second_time_goals_home'] = df_match['total_goals_home'] - df_match['first_time_goals_home']
            df_match['second_time_goals_away'] = df_match['total_goals_away'] - df_match['first_time_goals_away']
    
            # Draws
            df_match['draws'] = ((df_match['home_team_winner'] == 0) & (df_match['away_team_winner'] == 0)).astype(int)
            
            # Fail to score_home/away
            df_match['home_team_fail_to_score'] = (df_match['total_goals_home'] == 0).astype(int)
            df_match['away_team_fail_to_score'] = (df_match['total_goals_away'] == 0).astype(int)
            
            # Clean sheet_home/away
            df_match['home_team_clean'] = (df_match['total_goals_away'] == 0).astype(int)
            df_match['away_team_clean'] = (df_match['total_goals_home'] == 0).astype(int)
        
            # Drop columns
            df_match = df_match.drop(columns=['fixture_referee', 
                                            'fixture_timezone', 
                                            'fixture_timestamp', 
                                            'fixture_status_short', 
                                            'league_round', 
                                            'fixture_periods_first',
                                            'fixture_periods_second', 
                                            'fixture_venue_name', 
                                            'fixture_venue_city',
                                            'score_extratime_home',
                                            'score_extratime_away',
                                            'score_penalty_home',
                                            'score_penalty_away',
                                            'score_fulltime_home', 
                                            'score_fulltime_away'],axis=1)

            # Fill nan values in home_team_winner and away_team_winner with 0 beacuse it means a draw
            df_match['home_team_winner'].fillna(0, inplace=True)    
            df_match['away_team_winner'].fillna(0, inplace=True)
            
       
            # Correct datatype
            for col in df_match.columns:
                if 'id' in col or 'goals' in col:
                    df_match[col] = df_match[col].astype('Int64')
                    
            # Save data to DB
            conn = sqlite3.connect(self.db_path)
            df_match.to_sql("matches_info", conn, if_exists="replace", index=False)
            print("Data saved to DB for: matches info")
            
            

            
    #ok                        
    def clean_lineups_info(self):
        
        # Load JSON file from data/json_files/season
        with open(self.json_path + "/lineups.json") as json_file:
            json_lineups_data = json.load(json_file)
            print("JSON file loaded for: lineups info")
            
            # Normalize JSON file
            df = pd.json_normalize(json_lineups_data, sep = "_")
            
            # drop rows where results column is 0
            df = df[df['results'] != 0]
            
            # Drop useless columns
            df = df.drop(columns=['get', 'errors', 'results', 'paging_current', 'paging_total'])
            # Rename parameter_fixture in fixture_id and set as index
            df = df.rename(columns={'parameters_fixture': 'fixture_id'})
            
            # Normalize response
            df_teams=pd.json_normalize(df['response'])
            df_teams = pd.concat([df['fixture_id'], df_teams], axis=1) # concat fixture_id to df_teams
            
            # Normalize home team data in response
            df_home = pd.json_normalize(df_teams[0])
            df_home = df_home.add_prefix('home_')
            df_home = df_home.rename(columns={'home_team_id': 'team_id'})
            df_home.columns = df_home.columns.str.replace(".", "_")
            # Normalize away team data in response
            df_away = pd.json_normalize(df_teams[1])
            df_away = df_away.add_prefix('away_')
            df_away = df_away.rename(columns={'away_team_id': 'team_id'})
            df_away.columns = df_away.columns.str.replace(".", "_")
            df_away = pd.concat([df_teams['fixture_id'], df_away], axis=1) # Concat fixture_id to df_away team
            # merge home and away normalized data
            df_lineups = pd.concat([df_home, df_away], axis=1)
            df_lineups = df_lineups.rename(columns={'home_startXI': 'home_titolars', 'away_startXI': 'away_titolars'}) # Replace column name "startXI" with "titolars"
            
            # normalize home_titolars 
            df_home_titolars = pd.json_normalize(df_lineups['home_titolars'])
            df_home_titolars = df_home_titolars.add_prefix('home_titolar_')
            # normalize away_titolars
            df_away_titolars = pd.json_normalize(df_lineups['away_titolars'])
            df_away_titolars = df_away_titolars.add_prefix('away_titolar_')
            # merge home and away titolars
            df_titolars = pd.concat([df_home_titolars, df_away_titolars], axis=1) 
            # merge lineups and titolars
            df_lineups = pd.concat([df_lineups, df_titolars], axis=1)
            # Drop home_titolars and away_titolars old nested columns
            df_lineups = df_lineups.drop(columns=['home_titolars', 'away_titolars'])
            
            # drop subtitutes columns
            df_lineups = df_lineups.drop(columns=['home_substitutes', 'away_substitutes'])
            
            # Normalize all players nested informations
            sd=df_lineups.filter(regex='[0-9]$', axis=1) # Selects all the columns that have a number as their suffix
            
            # With a for loop normalizes the data nested in all these columns
            for col in sd.columns:
                df = pd.json_normalize(sd[col])
                df = df.add_prefix(col + '_')
                df_lineups = pd.concat([df_lineups, df], axis=1)
            df_lineups = df_lineups.drop(columns=sd.columns) # Drop home and away titolars and substitutes nested columns
            print("JSON file normalized for: lineups info")
            
            # Drop useless columns
            df_lineups = df_lineups.drop(columns=['home_team_name', 
                                                  'away_team_name', 
                                                  'home_team_logo', 
                                                  'away_team_logo', 
                                                  'home_team_colors', 
                                                  'away_team_colors']) 
            
            # In columns names replace "." with "_"
            df_lineups.columns = df_lineups.columns.str.replace(".", "_")
            
            # Sort by fixture_id ascending
            df_lineups = df_lineups.sort_values(by=['fixture_id'],ascending=True)      
                         
            # Save data to DB
            conn = sqlite3.connect(self.db_path)
            df_lineups.to_sql("lineups_info", conn, if_exists="append", index=False)
            print("Data saved to DB for: lineups info")
            
            
            

   