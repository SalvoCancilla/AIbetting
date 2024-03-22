import sys
import os
import sqlite3
import pandas as pd
import numpy as np

# Read the environment variables
base_dir = os.getenv("base_dir")
app_dir = os.getenv("app_dir")
scripts_dir = os.getenv("scripts_dir")
db_path = os.getenv("db_path")

# Add the paths to the sys.path list
sys.path.append(base_dir)

from scripts.get_data import GetHistoricData
from scripts.norm_data import NormData
from scripts.clean_data import CleanData
from scripts.save_data import SaveDataToDB





def main():
    """
    This is the main function that performs data retrieval, normalization, cleaning, and saving to the database.
    For now it retrieves countries and venues information, normalizes the data, cleans it, and saves it to the database.
    """
    get = GetHistoricData("2021", 10)
    norm = NormData("2021")
    clean = CleanData()
    save = SaveDataToDB()
    
    
    
    # Countries info
    get._check_api_limit()
    json_countries = get.get_countries_info()
    df_countries = norm.norm_countries_info(json_countries)
    clean.clean_countries_info(df_countries)
    save.countries_info_to_db(df_countries)
    
 
    
    
    # Venues info
    conn = sqlite3.connect(db_path) # Connect to the database
    countries = pd.read_sql_query("SELECT country_name FROM countries_info", conn) # Get the list of countries
    conn.close()
    
    for country_id in countries['country_name']:
        get._check_api_limit()
        json_venues = get.get_venues_info(country_id)
        df_venues = norm.norm_venues_info(json_venues)
        save.venues_info_to_db(df_venues)
    
   
    
    
   



if __name__ == "__main__":
    main()
