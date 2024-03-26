import os
import json
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
        self.db_path = "C:/Users/Lavoro/Desktop/AIBetting/AIbetting/core/data/Ai_Betting.db"
        self.json_path = "C:/Users/Lavoro/Desktop/AIBetting/AIbetting/core/data/json_files"
        self.season = season
        self.max_api_calls = max_api_calls
        self.api_call_count = 0


    
    def date_range(self):
        """
        Create a series of dates form start to end of the 2020 year
        Args:
            season (str): The season for which data is being retrieved.
        Returns:
            pd.Series: A series of dates form start to end of the 2020 year
        """
        dates = pd.date_range(start=f"{self.season}-01-01", end=f"{self.season}-12-31", freq="D").to_frame(index=False, name="date") #Create a series of dates form start to end of the 2020 year
        dates['date'] = pd.to_datetime(dates['date']).dt.date # Covert the date in datetime format
        return dates
    
    
    
    
    def _check_api_limit(self):
        """
        Checks if the API call count has reached the maximum limit. 
        - If the API call count is greater than or equal to the maximum limit, print a message and return True.

        Returns:
            bool: True if the API call count has reached the maximum limit, False otherwise.
        """
        if self.api_call_count >= self.max_api_calls:
            print("Reached maximum API call limit")
            return True
        return False
    
    


    def make_api_call(self, url):
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




    def save_json(self, json_data, file_name):
        """
        Saves the JSON data to a file in the specified directory.
        
        Args:
            json_data (dict): The JSON data to be saved.
            file_name (str): The name of the file to save the JSON data to.
        
        Returns:
            None
        """
        file_path = os.path.join(self.json_path, self.season, file_name)
        # Create the directory if it does not exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as file:
            json.dump(json_data, file, indent=4)
        print(f"JSON data saved to file: {file_name}")




   
                
                
                

    def get_countries_info(self):
        url = "https://api-football-v1.p.rapidapi.com/v3/countries"
        response = self.make_api_call(url)
        json_countries = response
        return json_countries



    def get_venues_info(self, country_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/venues?country={country_id}"
        response = self.make_api_call(url)
        json_venues = response
        return json_venues
    
    
    
    def get_leagues_info(self, country_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/leagues/?season={self.season}&country={country_id}"
        response = self.make_api_call(url)
        json_leagues = response
        return json_leagues
        
        
    
    def get_teams_info(self, country_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/teams?country={country_id}"
        response = self.make_api_call(url)
        json_teams = response
        return json_teams
    
    
    def get_players_info(self, team_id):
        url = f"https://api-football-v1.p.rapidapi.com/v3/players?season={self.season}&team={team_id}"
        response = self.make_api_call(url)
        json_players = response
        return json_players


    def get_matches_info(self, date): 
        url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures?date={date}"
        response = self.make_api_call(url)
        json_matches = response
        return json_matches



   
       
