import os
import pandas as pd
from pandas import json_normalize

class NormData:
    def __init__(self):
        self.base_dir = os.getenv("BASE_DIR")
        self.json_path = os.path.join(self.base_dir, "data", "json_files")

    def norm_data_info(self, json_data, data_type, record_path=None, sep='_'):
        if record_path:
            normalized_data = json_normalize(json_data, record_path, sep=sep)
        else:
            normalized_data = json_normalize(json_data["response"], sep=sep)
        
        print(f"Data normalized for: {data_type} info")
        return pd.DataFrame(normalized_data)

    def countries_info(self, json_countries):
        return self.norm_data_info(json_countries, data_type="countries")

    def venues_info(self, json_venues):
        return self.norm_data_info(json_venues, data_type="venues")

    def leagues_info(self, json_leagues):
        return self.norm_data_info(json_leagues, data_type="leagues")

    def teams_info(self, json_teams):
        return self.norm_data_info(json_teams, data_type="teams")

    def players_info(self, json_players):
        team_id = json_players['parameters']['team']
        players = self.norm_data_info(json_players, data_type="players", record_path='response')
        players['team_id'] = team_id
        return players

    def matches_info(self, json_matches):
        return self.norm_data_info(json_matches, data_type="matches", record_path='response')
    
    
    
    def home_lineups_info(self, json_lineups):
        fixture_id = json_lineups['parameters']['fixture']
        print(f"Fixture ID: {fixture_id}")
        
        # Extract home lineup information
        try:
            home_lineup = json_lineups['response'][0]
            lineups_home = json_normalize(home_lineup)
            print(lineups_home)
        except (KeyError, IndexError) as e:
            print(f"Error processing home lineup: {e}")
            lineups_home = pd.DataFrame()

        # Extract startXI information
        try:
            start_xi = home_lineup.get('startXI', [])
            start_xi = json_normalize(start_xi, sep='_')
            # Add "start" column to indicate player is in starting XI, 1 for True, 0 for False
            start_xi['start'] = 1                      
            print(start_xi)
        except Exception as e:
            print(f"Error processing start XI: {e}")
            start_xi = pd.DataFrame()
            
        # Extract substitutes information
        try:
            substitutes = home_lineup.get('substitutes', [])
            substitutes = json_normalize(substitutes, sep='_')
            # Add "sobstitue" column to indicate player is substitute
            substitutes['substitute'] = 1
            print(substitutes)
        except Exception as e:
            print(f"Error processing substitutes: {e}")
            substitutes = pd.DataFrame()
            
        lineups_home_norm = pd.concat([start_xi, substitutes], ignore_index=True) # Concatenate start XI and substitutes
        lineups_home_norm['fixture_id'] = fixture_id # Add fixture_id column to identify the fixture
        lineups_home_norm.columns = [f"home_{col}" for col in lineups_home_norm.columns] # Add "home_" prefix to all columns
        lineups_home_norm.fillna(0, inplace=True) # fill NaN values with 0
                 
        return lineups_home_norm
            
        
    def away_lineups_info(self, json_lineups):
        fixture_id = json_lineups['parameters']['fixture']
        print(f"Fixture ID: {fixture_id}")
        
        # Extract away lineup information
        try:
            away_lineup = json_lineups['response'][1]
            lineups_away = json_normalize(away_lineup)
            print(lineups_away)
        except (KeyError, IndexError) as e:
            print(f"Error processing away lineup: {e}")
            lineups_away = pd.DataFrame()

        # Extract startXI information
        try:
            start_xi = away_lineup.get('startXI', [])
            start_xi = json_normalize(start_xi, sep='_')
            # Add "start" column to indicate player is in starting XI, 1 for True, 0 for False
            start_xi['start'] = 1                      
            print(start_xi)
        except Exception as e:
            print(f"Error processing start XI: {e}")
            start_xi = pd.DataFrame()
            
        # Extract substitutes information
        try:
            substitutes = away_lineup.get('substitutes', [])
            substitutes = json_normalize(substitutes, sep='_')
            # Add "sobstitue" column to indicate player is substitute
            substitutes['substitute'] = 1
            print(substitutes)
        except Exception as e:
            print(f"Error processing substitutes: {e}")
            substitutes = pd.DataFrame()
            
        lineups_away_norm = pd.concat([start_xi, substitutes], ignore_index=True)
        lineups_away_norm['fixture_id'] = fixture_id
        lineups_away_norm.columns = [f"away_{col}" for col in lineups_away_norm.columns]
        lineups_away_norm.fillna(0, inplace=True)
        
        return lineups_away_norm
    
    
    def match_statistics_home(self, json_match_statistics):
        fixture_id = json_match_statistics['parameters']['fixture']
        print(f"Fixture ID: {fixture_id}")
        
        # Extract home team statistics
        try:
            home_stats = json_match_statistics['response'][0]['statistics']
            home_stats = json_normalize(home_stats)
            home_stats['fixture_id'] = fixture_id # Add fixture_id column to identify the fixture
            home_stats['team_id'] = json_match_statistics['response'][0]['team']['id'] # Add team_id column to identify the team
            home_stats.columns = [f"home_{col}" for col in home_stats.columns] # Add "home_" prefix to all columns
            home_stats.rename(columns={'home_fixture_id': 'fixture_id'}, inplace=True) # Rename home_fixture_id to fixture_id because is a foren key
            print(home_stats)
        except (KeyError, IndexError) as e:
            print(f"Error processing home team statistics: {e}")
            home_stats = pd.DataFrame()
            
        return home_stats
    
    
    def match_statistics_away(self, json_match_statistics):
        fixture_id = json_match_statistics['parameters']['fixture']
        print(f"Fixture ID: {fixture_id}")
        
        # Extract away team statistics
        try:
            away_stats = json_match_statistics['response'][1]['statistics']
            away_stats = json_normalize(away_stats)
            away_stats['fixture_id'] = fixture_id # Add fixture_id column to identify the fixture
            away_stats['team_id'] = json_match_statistics['response'][1]['team']['id'] # Add team_id column to identify the team
            away_stats.columns = [f"away_{col}" for col in away_stats.columns] # Add "away_" prefix to all columns
            away_stats.rename(columns={'away_fixture_id': 'fixture_id'}, inplace=True) # rename home_fixture_id to fixture_id because is a foren key
            print(away_stats)
        except (KeyError, IndexError) as e:
            print(f"Error processing away team statistics: {e}")
            away_stats = pd.DataFrame()
            
        return away_stats