import sqlite3



import sqlite3

class SaveDataToDB:
    """
    A class that provides methods to save data to a SQLite database.
    """

    def __init__(self):
        """
        Initializes the SaveDataToDB object.
        """
        self.db_path = "C:/Users/Lavoro/Desktop/AIBetting/AIbetting/core/data/Ai_Betting.db"



    def countries_info_to_db(self, df_countries_clean):
        """
        Saves the countries information DataFrame to the 'countries_info' table in the database.
        1- Connect to the database.
        2- Check if the table exists.
        3- If the table does not exist, create it and save the data.
        4- If the table exists, append the new data to it.
        5- Close the connection.

        Args:
            df_countries (pandas.DataFrame): The DataFrame containing the cleaned countries information.

        Returns:
            None
        """
        table_name = "countries_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_countries_clean.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_countries_clean.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()




    def venues_info_to_db(self, df_venues_clean):
        """
        Saves the venues information DataFrame to the 'venues_info' table in the database.
        1- Connect to the database.
        2- Check if the table exists.
        3- If the table does not exist, create it and save the data.
        4- If the table exists, append the new data to it.
        5- Close the connection.

        Args:
            df_venues_clean (pandas.DataFrame): The DataFrame containing the venues information.

        Returns:
            None
        """
        table_name = "venues_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_venues_clean.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_venues_clean.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()
        
        
        
    
    def leagues_info_to_db(self, df_leagues_clean):
        """
        Saves the seasons information DataFrame to the 'seasons_info' table in the database.
        1- Connect to the database.
        2- Check if the table exists.
        3- If the table does not exist, create it and save the data.
        4- If the table exists, append the new data to it.
        5- Close the connection.

        Args:
            df_seasons (pandas.DataFrame): The DataFrame containing the seasons information.

        Returns:
            None
        """
        table_name = "leagues_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_leagues_clean.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_leagues_clean.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()
        
        
    
    
    def teams_info_to_db(self, df_teams_clean):
        """
        Saves the teams information DataFrame to the 'teams_info' table in the database.
        1- Connect to the database.
        2- Check if the table exists.
        3- If the table does not exist, create it and save the data.
        4- If the table exists, append the new data to it.
        5- Close the connection.

        Args:
            df_teams (pandas.DataFrame): The DataFrame containing the teams information.

        Returns:
            None
        """
        table_name = "teams_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_teams_clean.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_teams_clean.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()
        
        
        
        
    def players_info_to_db(self, df_players_clean):
        """
        Saves the players information DataFrame to the 'players_info' table in the database.
        1- Connect to the database.
        2- Check if the table exists.
        3- If the table does not exist, create it and save the data.
        4- If the table exists, append the new data to it.
        5- Close the connection.

        Args:
            df_players (pandas.DataFrame): The DataFrame containing the players information.

        Returns:
            None
        """
        table_name = "players_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_players_clean.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_players_clean.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()
        
        
        
        
    def matches_info_to_db(self, df_matches_clean):
        """
        Saves the matches information DataFrame to the 'matches_info' table in the database.
        1- Connect to the database.
        2- Check if the table exists.
        3- If the table does not exist, create it and save the data.
        4- If the table exists, append the new data to it.
        5- Close the connection.

        Args:
            df_matches (pandas.DataFrame): The DataFrame containing the matches information.

        Returns:
            None
        """
        table_name = "matches_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_matches_clean.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_matches_clean.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()
