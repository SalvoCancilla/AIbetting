import os
import json
import sqlite3
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("RAPIDAPI_KEY")
api_host = os.getenv("RAPIDAPI_HOST")


class GetHistoricData:
    """
    A class to retrieve and manage historic data.

    Attributes:
        season (str): The season for which the data is retrieved.
        max_api_calls (int): The maximum number of API calls allowed.
        db_path (str): The path to the database file.
        json_path (str): The path to the JSON files directory.
        api_call_count (int): The number of API calls made.

    Methods:
        _make_api_call(url): Makes an API call to the specified URL.
        _save_json_data(data, filename): Saves the JSON data to a file.
        _update_json_data(data, filename): Updates the existing JSON data in a file.
        _load_existing_json_data(filename): Loads the existing JSON data from a file.
        _check_api_limit(): Checks if the maximum API call limit has been reached.
        _get_api_data(url, filename): Retrieves the API data and saves it to a JSON file.
        get_countries_info(): Retrieves the countries information.
        get_venues_info(): Retrieves the venues information.

    """

    def __init__(self, season, max_api_calls=100):
        self.db_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/Ai_Betting.db"
        self.json_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/json_files"
        self.season = season
        self.max_api_calls = max_api_calls
        self.api_call_count = 0



class GetHistoricData:
    def __init__(self, season, max_api_calls=100):
        """
        Initializes an instance of the GetData class.

        Parameters:
        - season (str): The season for which data is being retrieved.
        - max_api_calls (int, optional): The maximum number of API calls allowed. Default is 1000.

        Attributes:
        - db_path (str): The path to the database file.
        - json_path (str): The path to the JSON files.
        - season (str): The season for which data is being retrieved.
        - max_api_calls (int): The maximum number of API calls allowed.
        - api_call_count (int): The current number of API calls made.
        """
        self.db_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/Ai_Betting.db"
        self.json_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/json_files"
        self.season = season
        self.max_api_calls = max_api_calls
        self.api_call_count = 0




    def _make_api_call(self, url):
        """
        Makes an API call to the specified URL and returns the response in JSON format.

        Args:
            url (str): The URL to make the API call to.

        Returns:
            dict: The response from the API call in JSON format, or None if the API call fails.

        Raises:
            requests.exceptions.RequestException: If the API call fails with an exception.

        """
        headers = {'x-rapidapi-key': api_key, 'x-rapidapi-host': api_host}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raises exception for 4XX and 5XX status codes
            print(f"API call done for {url}")
            self.api_call_count += 1
            print(f"Number of API calls done: {self.api_call_count}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"ERROR: API call to {url} failed with exception: {e}")
            return None




    def _save_json_data(self, data, filename):
        """
        Save the given data as a JSON file.

        Args:
            data (dict): The data to be saved as JSON.
            filename (str): The name of the JSON file.

        Returns:
            None
        """
        filepath = os.path.join(self.json_path, self.season, filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as outfile:
            json.dump(data, outfile)
            print(f"JSON file saved for: {filename}")




    def _update_json_data(self, data, filename):
        """
        Updates the existing JSON data with new data and saves it to a file.

        Args:
            data (dict): The new data to be added to the existing JSON data.
            filename (str): The name of the JSON file.

        Returns:
            None
        """
        filepath = os.path.join(self.json_path, self.season, filename)
        existing_data = self._load_existing_json_data(filename)
        if existing_data:
            existing_data.update(data)
            with open(filepath, "w") as outfile:
                json.dump(existing_data, outfile)
                print(f"JSON file updated for: {filename}")
        else:
            self._save_json_data(data, filename)




    def _load_existing_json_data(self, filename):
        """
        Load existing JSON data from a file.

        Args:
            filename (str): The name of the file to load the JSON data from.

        Returns:
            dict or None: The loaded JSON data as a dictionary, or None if the file is not found.
        """
        filepath = os.path.join(self.json_path, self.season, filename)
        try:
            with open(filepath, "r") as infile:
                return json.load(infile)
        except FileNotFoundError:
            return None




    def _check_api_limit(self):
        """
        Checks if the API call count has reached the maximum limit. 

        Returns:
            bool: True if the API call count has reached the maximum limit, False otherwise.
        """
        if self.api_call_count >= self.max_api_calls:
            print("Reached maximum API call limit.")
            return True
        return False




    def _get_api_data(self, url, filename):
        """
        Retrieves data from an API endpoint and saves it as a JSON file.

        Args:
            url (str): The URL of the API endpoint.
            filename (str): The name of the JSON file to be saved.

        Returns:
            dict: The JSON data retrieved from the API.

        Raises:
            None

        """
        if not self._check_api_limit():
            if not os.path.exists(os.path.join(self.json_path, self.season, filename)):
                # The JSON file doesn't exist, so we save it
                json_data = self._make_api_call(url)
                if json_data:
                    self._save_json_data(json_data, filename)
                    return json_data
            else:
                # The JSON file already exists, so we update it
                json_data = self._make_api_call(url)
                if json_data:
                    self._update_json_data(json_data, filename)
                    return json_data

                
                
                

    def get_countries_info(self):
        """
        Retrieves information about countries from an API.

        Returns:
            None
        """
        url = "https://api-football-v1.p.rapidapi.com/v3/countries"
        self._get_api_data(url, "countries.json")



    def get_venues_info(self):
        """
        Retrieves information about venues from the API for each country in the database.

        Returns:
            list: A list of venue data for each country.
        """
        conn = sqlite3.connect(self.db_path)
        countries = pd.read_sql_query("SELECT country_name FROM countries_info", conn)
        conn.close()
        venues_data = []

        for country in countries['country_name']:
            url = f"https://api-football-v1.p.rapidapi.com/v3/venues?country={country}"
            data = self._get_api_data(url, "venues.json")
            venues_data.append(data)
        return venues_data
        




   
       
