






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