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
        self.db_path = "C:/Users/Lavoro/Desktop/AIBetting/core/data/Ai_Betting.db"



    def countries_info_to_db(self, df_countries):
        """
        Saves the countries information DataFrame to the 'countries_info' table in the database.

        Args:
            df_countries (pandas.DataFrame): The DataFrame containing the countries information.

        Returns:
            None
        """
        table_name = "countries_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_countries.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_countries.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()



    def venues_info_to_db(self, df_venues):
        """
        Saves the venues information DataFrame to the 'venues_info' table in the database.

        Args:
            df_venues (pandas.DataFrame): The DataFrame containing the venues information.

        Returns:
            None
        """
        table_name = "venues_info"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()

        if result is None:
            df_venues.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Table created and data saved to DB for: {table_name}")
        else:
            df_venues.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Data appended to DB for: {table_name}")

        conn.close()
