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
        normalized_data = json_normalize(json_venues["response"])
        print("Data normalized for: venues info")
        df_venues = pd.DataFrame(normalized_data)
        return df_venues