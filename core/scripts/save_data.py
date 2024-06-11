import os
import sqlite3

class SaveDataToDB:
    def __init__(self):
        self.db_path = os.getenv("DB_PATH")
        
    def _save_data_to_db(self, df, table_name):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                result = cursor.fetchone()
                if result is None:
                    df.to_sql(table_name, conn, if_exists="replace", index=False)
                    print(f"Table created and data saved to DB for: {table_name}")
                else:
                    df.to_sql(table_name, conn, if_exists="append", index=False)
                    print(f"Data appended to DB for: {table_name}")
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
        except Exception as e:
            print(f"Error: {e}")


    def countries(self, df_countries_clean):
        self._save_data_to_db(df_countries_clean, "countries_info")

        
    def venues(self, df_venues_clean):
        self._save_data_to_db(df_venues_clean, "venues_info")


    def leagues(self, df_leagues_clean):
        self._save_data_to_db(df_leagues_clean, "leagues_info")


    def teams(self, df_teams_clean):
        self._save_data_to_db(df_teams_clean, "teams_info")


    def players(self, df_players_clean):
        self._save_data_to_db(df_players_clean, "players_info")


    def matches(self, df_matches_clean):
        self._save_data_to_db(df_matches_clean, "matches_info")
        
        
    def home_lineups(self, lineups_home_norm):
        self._save_data_to_db(lineups_home_norm, "home_lineups_info")
    
    
    def away_lineups(self, lineups_away_norm):
        self._save_data_to_db(lineups_away_norm, "away_lineups_info")
        
    
    def home_stats(self, df_home_stats_clean):
        self._save_data_to_db(df_home_stats_clean, "match_statistics_home")
        
    
    def away_stats(self, df_away_stats_clean):
        self._save_data_to_db(df_away_stats_clean, "match_statistics_away")
        
    def standings(self, df_standings_clean):
        self._save_data_to_db(df_standings_clean, "standings_info")
        