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
        return self.norm_data_info(json_players, data_type="players", record_path='response')

    def matches_info(self, json_matches):
        return self.norm_data_info(json_matches, data_type="matches", record_path='response')

