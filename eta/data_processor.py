import pandas as pd
import numpy as np
from .helper import equirectangular_approx_distance

# This class focuses on preparing and processing the GPS data.
# Useful function: get_speed, categorize_stop
class DataPreprocessor:

    def __init__(self, stop_mean_eta):
        self.stop_mean_eta = stop_mean_eta

    def preprocess_gps_data(self, df):
        """
        Preprocess the GPS data by removing specific columns, parsing dates, and extracting day and hour.

        Args:
            df (pd.DataFrame): DataFrame containing raw GPS data.
        Returns:
            pd.DataFrame: Preprocessed GPS data.
        """

        df = df.drop(['color'], axis=1)
        df['gpsdatetime'] = pd.to_datetime(df['gpsdatetime'])
        df['day'] = df['gpsdatetime'].dt.dayofweek
        df['hour'] = df['gpsdatetime'].dt.hour
        return df.sort_values(by='gpsdatetime').reset_index(drop=True)

    def categorize_stop(self, gps_data: pd.DataFrame, num_bins=6) -> pd.DataFrame:
        """
        Categorizes each stop in the GPS data based on the ETA into a specified number of bins.

        This method assigns a category to each stop in the GPS data based on the estimated time
        of arrival (ETA). It uses predefined ETA values in `self.stop_mean_eta` to determine the
        bins and then assigns each stop a category label.

        Args:
            gps_data (pd.DataFrame): The GPS data containing the stops to be categorized.
            num_bins (int): The number of categories (bins) to divide the stops into.

        Returns:
            pd.DataFrame: The GPS data with an additional column 'categorized_stop' representing
                          the category of each stop.
        """

        # Generate equally spaced bins based on the maximum ETA value
        bins = np.linspace(0, self.stop_mean_eta["eta"].max(), num_bins)

        # Categorize 'eta' values into bins and assign labels
        self.stop_mean_eta['categorized_stop'] = pd.cut(self.stop_mean_eta['eta'], bins=bins,
                                                    labels= [i for i in range(1, num_bins)])

        stop_mean_dct = self.stop_mean_eta.T.to_dict()

        # Map each stop in the GPS data to its corresponding category
        gps_data["categorized_stop"] = gps_data["next_stop_seq"].apply(lambda row: stop_mean_dct[row]["categorized_stop"])
        return gps_data

    def get_speed(self, gps, k):
        """
        Calculates the speed for each GPS entry in the dataframe.

        Args:
        gps (pd.DataFrame): DataFrame containing GPS data.
        k (int): The number of lag intervals to consider for speed calculation.

        Returns:
        pd.DataFrame: Updated DataFrame with calculated speed.
        """
        # Convert 'gpsdatetime' to datetime and calculate the time difference in seconds
        gps['lag_gpsdatetime'] = gps.groupby(['bus_code', 'koridor'])['gpsdatetime'].shift(1)
        gps['gpsdatetime'] = pd.to_datetime(gps['gpsdatetime'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        gps['lag_gpsdatetime'] = pd.to_datetime(gps['lag_gpsdatetime'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        gps['time_difference_seconds'] = (gps['gpsdatetime'] - gps['lag_gpsdatetime']).dt.total_seconds()
        gps = self._k_lag_speed(gps, k)
        return gps

    def _create_lag_columns(self, gps, k):
        """
        Create lagged speed columns in the GPS dataframe.

        Args:
            gps (pd.DataFrame): The GPS dataframe.
            k (int): The number of lag columns to create.

        Returns:
            pd.DataFrame: The GPS dataframe with lag columns added.
        """
        gps_copy = gps.copy()
        gps_copy.sort_values(by=['bus_code', 'koridor', 'gpsdatetime'], inplace=True)

        # Create the lagged speed columns with NaN values where the time difference exceeds the limit
        for i in range(1, k + 1):
            gps_copy[f'lag_{i}'] = gps_copy.groupby(['bus_code', 'koridor'])['gpsspeed'].shift(i)
            # Condition to reset the lag to NaN when time difference exceeds 60 seconds
            time_diff_exceeds_limit = gps_copy['time_difference_seconds'] > 60
            # Where condition to replace the values with NaN
            for j in range(i):
                gps_copy[f'lag_{i}'] = gps_copy[f'lag_{i}'].where(~time_diff_exceeds_limit.shift(j, fill_value=False), np.nan)

        return gps_copy

    def _k_lag_speed(self, gps, k):
        """
        Calculate k-lagged speed for the GPS dataframe.

        Args:
            gps (pd.DataFrame): The GPS dataframe.
            k (int): The number of lag columns to create and fill.

        Returns:
            pd.DataFrame: The GPS dataframe with filled lag columns.
        """
        gps = self._create_lag_columns(gps, k)

        for i in range(1, k + 1):
            # Determine rows where lag_i is NaN, we will only check distance for these
            nan_mask = gps[f'lag_{i}'].isna()

            # Initialize an empty list to hold the distances where the lag is NaN
            distances_where_nan = [np.nan] * len(gps)

            # Iterate over the rows where lag_i is NaN
            for idx in gps[nan_mask].index:
                if idx > 0:  # ensure we are not at the first row
                    prev_point = (gps.at[idx - 1, 'latitude'], gps.at[idx - 1, "longitude"])
                    curr_point = (gps.at[idx, 'latitude'], gps.at[idx, "longitude"])
                    distance = equirectangular_approx_distance(prev_point, curr_point)["meters"]
                    distances_where_nan[idx] = distance

            # Create a mask for where the distance is <= 75 meters and lag_i is NaN
            distance_mask = (pd.Series(distances_where_nan) <= 75) & nan_mask

            # Forward fill the speed for missing lag values where the condition meets
            gps.loc[distance_mask, f'lag_{i}'] = gps.loc[distance_mask].groupby(['bus_code', 'koridor'])['gpsspeed'].ffill()

            gps.loc[0, f'lag_{i}'] = gps.loc[0, 'gpsspeed']
            # For initial entries that are NaN or don't meet the mask condition, set to 5
            gps[f'lag_{i}'].fillna(5, inplace=True)

        return gps