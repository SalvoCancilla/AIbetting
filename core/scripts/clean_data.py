import pandas as pd






class CleanData:
    """
    A class that provides methods to clean data.
    """

    def __init__(self):
        pass



    def clean_countries_info(self, df_countries):
        """
        Cleans the countries info dataframe.

        Args:
            df_countries (pandas.DataFrame): The dataframe containing countries info.

        Returns:
            pandas.DataFrame: The cleaned dataframe.

        """
        df_countries_clean = df_countries.copy()             
        df_countries_clean.columns = ['country_' + col for col in df_countries.columns] # Add prefix 'country_' for each column
        df_countries_clean.sort_values(by=['country_name'], ascending=True) # order ascending by country_name
        print("Columns cleaned for: countries info")
        return df_countries_clean
    
    
    
    
    def clean_venues_info(self, df_venues):
        """
        Cleans the venues info dataframe.

        Args:
            df_venues (pandas.DataFrame): The dataframe containing venues info.

        Returns:
            pandas.DataFrame: The cleaned dataframe.

        """
        df_venues_clean = df_venues.copy()
        df_venues_clean.columns = ['venue_' + col for col in df_venues.columns] # Add prefix 'venue_' for each column
        df_venues_clean.drop(columns=['venue_address']) # Drop columns "address"
        df_venues_clean.sort_values(by=['venue_id'], ascending=True) # order ascending by venue_name
        print("Columns cleaned for: venues info")
        return df_venues_clean
    
    
    
    
    def clean_leagues_info(self, df_leagues):
        """
        Cleans the seasons info dataframe.

        Args:
            df_seasons (pandas.DataFrame): The dataframe containing seasons info.

        Returns:
            pandas.DataFrame: The cleaned dataframe.

        """
        df_leagues_clean = df_leagues.copy()
        df_leagues_clean.columns = df_leagues_clean.columns.str.replace(".", "_") # Replace . with _ in column names
        df_leagues_clean.drop(columns=['seasons'], inplace=True) # Drop columns
        print("Columns cleaned for: leagues info")
        return df_leagues_clean
    
    
    
    def clean_teams_info(self, df_teams):
        """
        Cleans the teams info dataframe.

        Args:
            df_teams (pandas.DataFrame): The dataframe containing teams info.

        Returns:
            pandas.DataFrame: The cleaned dataframe.

        """
        df_teams_clean = df_teams.copy()
        df_teams_clean = df_teams_clean.loc[:, ~df_teams_clean.columns.str.startswith('venue_')] # Drop all columns wich title starts with "venue_"
        df_teams_clean.sort_values(by=['team_id'], ascending=False) # sort by team_id descending
        print("Columns cleaned for: teams info")
        return df_teams_clean
    
    
    
    
    def clean_players_info(self, df_players):
        """
        Cleans the players' information dataframe by dropping unnecessary columns and sorting by player_id.

        Args:
            df_players (pandas.DataFrame): The dataframe containing the players' information to be cleaned.

        Returns:
            df_players_clean (pandas.DataFrame): The cleaned dataframe.
        """
        df_players_clean = df_players.copy()
        df_players_clean = df_players_clean.drop(columns=['statistics', 'player_birth_place', 'player_birth_country']) # Drop columns
        df_players_clean.sort_values(by=['player_id'], ascending=False) # Sort by player_id descending
        print("Columns cleaned for: players info")
        return df_players_clean
    
    
    
    
    
    def clean_matches_info(self, df_matches):
        """
        Clean the matches information dataframe.

        Args:
            df_matches (DataFrame): The dataframe containing the matches information.

        Returns:
            DataFrame: The cleaned dataframe.

        """
        df_matches_clean = df_matches.copy()

        # Rename columns
        df_matches_clean.rename(columns={'goals_home': 'total_goals_home',
                                         'goals_away': 'total_goals_away',
                                         'score_halftime_home': 'first_time_goals_home',
                                         'score_halftime_away': 'first_time_goals_away',
                                         'teams_home_id': 'home_team_id',
                                         'teams_away_id': 'away_team_id',
                                         'teams_home_name': 'home_team_name',
                                         'teams_away_name': 'away_team_name',
                                         'teams_home_winner': 'home_team_winner',
                                         'teams_away_winner': 'away_team_winner',
                                         'fixture_venue_id': 'venue_id',
                                         'fixture_status_long': 'status_long',
                                         'fixture_status_elapsed': 'status_elapsed'}, inplace=True)

        df_matches_clean['fixture_date'] = pd.to_datetime(df_matches_clean['fixture_date'])  # Convert Complete_fixture_date in datetime
        df_matches_clean = df_matches_clean.sort_values(by=['fixture_id'], ascending=True)  # order by fixture_id

        df_matches_clean['hours'] = df_matches_clean['fixture_date'].dt.strftime('%H:%M') # New column Hours

        df_matches_clean.insert(14, "day", df_matches_clean["league_round"].str.split("-").str[-1].str.strip()) # New column Day

        df_matches_clean['second_time_goals_home'] = df_matches_clean['total_goals_home'] - df_matches_clean['first_time_goals_home'] # New column Second_time_goals_home
        df_matches_clean['second_time_goals_away'] = df_matches_clean['total_goals_away'] - df_matches_clean['first_time_goals_away'] # New column Second_time_goals_away

        df_matches_clean['draws'] = ((df_matches_clean['home_team_winner'] == 0) & (df_matches_clean['away_team_winner'] == 0)).astype(int) # New column Draws

        df_matches_clean['home_team_fail_to_score'] = (df_matches_clean['total_goals_home'] == 0).astype(int) # New column Home_team_fail_to_score
        df_matches_clean['away_team_fail_to_score'] = (df_matches_clean['total_goals_away'] == 0).astype(int) # New column Away_team_fail_to_score
        
        df_matches_clean['home_team_clean'] = (df_matches_clean['total_goals_away'] == 0).astype(int) # New column Home_team_clean
        df_matches_clean['away_team_clean'] = (df_matches_clean['total_goals_home'] == 0).astype(int) # New column Away_team_clean

        # Drop columns
        df_matches_clean.drop(columns=['fixture_referee',
                                       'fixture_timezone',
                                       'fixture_timestamp',
                                       'fixture_status_short',
                                       'league_round',
                                       'fixture_periods_first',
                                       'fixture_periods_second',
                                       'fixture_venue_name',
                                       'fixture_venue_city',
                                       'score_extratime_home',
                                       'score_extratime_away',
                                       'score_penalty_home',
                                       'score_penalty_away',
                                       'score_fulltime_home',
                                       'score_fulltime_away'], axis=1, inplace=True)

        # Fill nan values in home_team_winner and away_team_winner with 0 because it means a draw
        df_matches_clean['home_team_winner'].fillna(0, inplace=True)
        df_matches_clean['away_team_winner'].fillna(0, inplace=True)
        df_matches_clean['status_elapsed'].fillna(0, inplace=True)

        print("Columns cleaned for: matches info")
        return df_matches_clean
        