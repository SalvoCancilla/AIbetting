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
    def __init__(self, season, max_api_calls=100):
        """
        A class to retrieve and manage historic data.

        Parameters:
        - season (str): The season for which data is being retrieved.
        - max_api_calls (int, optional): The maximum number of API calls allowed. Default is 100.

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
        1- Set the headers for the API call.
        2- Make the API call using the requests library.
        3- Check for any exceptions during the API call.
        4- Return the response in JSON format.
        5- Increment the API call count.
        6- Print the number of API calls made.
        7- Return the response in JSON format if the API call is successful and print a succesfull message.
        8- Return None if the API call fails with an exception and print an error message.
        
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
            response.raise_for_status() 
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
        1- Create the file path for the JSON file.
        2- Create the directory if it doesn't exist.
        3- Save the data as a JSON file.

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
        1- Load the existing JSON data from the file.
        2- If the file exists, update the existing data with the new data.
        3- If the file doesn't exist, save the new data to the file.

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
        - Create the file path for the JSON file.
        - Try to open the file and load the JSON data.
        - Return the loaded JSON data as a dictionary, or None if the file is not found.

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
        - If the API call count is greater than or equal to the maximum limit, print a message and return True.

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
        1- Check if the API call limit has been reached.
        2- Check if the JSON file already exists.
        3- If the JSON file doesn't exist, save the data.
        4- If the JSON file exists, update the data.
        5- Return the JSON data retrieved from the API.

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
        1- Set the URL for the API call.
        2- Make the API call to get the data.

        Returns:
            None
        """
        url = "https://api-football-v1.p.rapidapi.com/v3/countries"
        self._get_api_data(url, "countries.json")



    def get_venues_info(self):
        """
        Retrieves information about venues from the API for each country in the database.
        1- Connect to the database and get the list of countries.
        2- For each country, make an API call to get the venues data in that country.
        3- Save the venues data as a JSON file.
        4- Return the list of venues data for each country.        

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
        




   
       
