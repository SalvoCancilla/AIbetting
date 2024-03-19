import os
import json
import pandas as pd
from pandas import json_normalize








class NormData:
    """
    A class for normalizing data related to countries and venues.

    Attributes:
        json_path (str): The path to the JSON files directory.

    Methods:
        norm_countries_info: Normalize the countries information from the JSON file.
        norm_venues_info: Normalize the venues information from the JSON file.
    """



    def __init__(self, season):
        self.json_path = f"C:/Users/Lavoro/Desktop/AIBetting/core/data/json_files/{season}"



    def norm_countries_info(self):
        """
        Normalize the countries information from the JSON file.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized countries information.
        """
        with open(os.path.join(self.json_path, "countries.json"), "r") as infile:
            countries_data = json.load(infile)
            normalized_data = json_normalize(countries_data["response"])
            print("Data normalized for: countries info")
            df_countries = pd.DataFrame(normalized_data)
            return df_countries



    def norm_venues_info(self):
        """
        Normalize the venues information from the JSON file.

        Returns:
            DataFrame: A pandas DataFrame containing the normalized venues information.
        """
        with open(os.path.join(self.json_path, "venues.json"), "r") as infile:
            venues_data = json.load(infile)
            normalized_data = json_normalize(venues_data["response"])
            print("Data normalized for: venues info")
            df_venues = pd.DataFrame(normalized_data)
            return df_venues