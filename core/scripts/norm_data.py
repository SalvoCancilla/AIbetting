import os
import json
import pandas as pd
from pandas import json_normalize








class NormData:
    """
    A class for normalizing data.

    Attributes:
        season (int): The season for which data is being normalized.
        json_path (str): The path to the JSON files directory.

    Methods:
        norm_countries_info: Normalize the countries information from the JSON file.
        norm_venues_info: Normalize the venues information from the JSON file.
    """

    def __init__(self, season):
        self.json_path = f"C:/Users/Lavoro/Desktop/AIBetting/AIbetting/core/data/json_files/{season}"




    def norm_countries_info(self, json_countries):
        """
        Normalize the countries information from the JSON file.
        1- Load the JSON file containing the countries information.
        2- Normalize the JSON nested informations.
        3- Create a pandas DataFrame from the normalized data.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized countries information.
        """
        normalized_data = json_normalize(json_countries["response"])
        print("Data normalized for: countries info")
        df_countries = pd.DataFrame(normalized_data)
        return df_countries




    def norm_venues_info(self, json_venues):
        """
        Normalize the venues information from the JSON file.
        1- Load the JSON file containing the venues information.
        2- Normalize the JSON nested informations.
        3- Create a pandas DataFrame from the normalized data.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized venues information.
        """
        normalized_data = json_normalize(json_venues["response"], sep='_')
        print("Data normalized for: venues info")
        df_venues = pd.DataFrame(normalized_data)
        return df_venues
    
    
    
    
    def norm_leagues_info(self, json_leagues):
        """
        Normalize the seasons information from the JSON file.
        1- Load the JSON file containing the seasons information.
        2- Normalize the JSON nested informations.
        3- Create a pandas DataFrame from the normalized data.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized seasons information.
        """
        normalized_data = json_normalize(json_leagues["response"])
        print("Data normalized for: seasons info")
        df_leagues = pd.DataFrame(normalized_data)
        return df_leagues
    
    
    
    def norm_teams_info(self, json_teams):
        """
        Normalize the teams information from the JSON file.
        1- Load the JSON file containing the teams information.
        2- Normalize the JSON nested informations.
        3- Create a pandas DataFrame from the normalized data.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized teams information.
        """
        normalized_data = json_normalize(json_teams["response"], sep='_')
        print("Data normalized for: teams info")
        df_teams = pd.DataFrame(normalized_data)
        return df_teams
    
    
    
    def norm_players_info(self, json_players):
        """
        Normalize the players information from the JSON file.
        1- Load the JSON file containing the players information.
        2- Normalize the JSON nested informations.
        3- Create a pandas DataFrame from the normalized data.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized players information.
        """
        df_players = pd.json_normalize(json_players, 'response', sep='_') # Extract the response data       
        print("Data normalized for: players info")
        return df_players
    
    
    def norm_matches_info(self, json_matches):
        """
        Normalize the matches information from the JSON file.
        1- Load the JSON file containing the matches information.
        2- Normalize the JSON nested informations.
        3- Create a pandas DataFrame from the normalized data.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized matches information.
        """
        df_matches = pd.json_normalize(json_matches, 'response', sep='_') # Extract the response data
        print("Data normalized for: matches info")
        return df_matches