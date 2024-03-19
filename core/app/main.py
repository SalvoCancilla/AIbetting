import sys
import os

# Read the environment variables
base_dir = os.getenv("base_dir")
app_dir = os.getenv("app_dir")
scripts_dir = os.getenv("scripts_dir")

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
    get = GetHistoricData("2021", 500)
    norm = NormData("2021")
    clean = CleanData()
    save = SaveDataToDB()
    
    
    """""""""    
    # Countries info
    get.get_countries_info()
    df_countries = norm.norm_countries_info()
    clean.clean_countries_info(df_countries)
    save.countries_info_to_db(df_countries)
    """""""""
    
    
    # Venues info
    get.get_venues_info()
    df_venues = norm.norm_venues_info()
    print(df_venues.head())
    clean.clean_venues_info(df_venues)
    print(df_venues.head())
    save.venues_info_to_db(df_venues)
    
    
    
   



if __name__ == "__main__":
    main()
