






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
            pandas.DataFrame: The cleaned dataframe with columns renamed and prefixed with 'country_'.

        """
        df_countries.columns = ['country_' + col for col in df_countries.columns] # Add prefix 'country_' for each column
        print("Columns cleaned for: countries info")
        return df_countries
    
    
    
    def clean_venues_info(self, df_venues):
        """
        Cleans the venues info dataframe.

        Args:
            df_venues (pandas.DataFrame): The dataframe containing venues info.

        Returns:
            pandas.DataFrame: The cleaned dataframe with columns renamed and 'venue_address' column dropped.

        """
        df_venues.columns = ['venue_' + col for col in df_venues.columns] # Add prefix 'venue_' for each column
        df_venues = df_venues.drop(columns=['venue_address']) # Drop columns "venue_address"
        print("Columns cleaned for: venues info")
        return df_venues