from .data_processor import DataPreprocessor
from .route_analyzer import RouteAnalyzer
from .eta_predictor import ETAPredictor
from .trip_determiner import TripDeterminer
import time
import pandas as pd
from .gtfs_kit import read_feed
import asyncio

import warnings
warnings.filterwarnings("ignore")

class BusETAApplication:
    def __init__(self, folder_path):
        model_path = "model.pkl"
        stop_mean_eta_path = "categorized_stop_mean.pickle"
        map_path = "map.pickle"
        feed_path = "gtfs.zip"
        next_prev_path = "next_prev_df.pickle"

        col = ["koridor", "trip_id", "next_stop", "prev_stop", "next_stop_seq", "prev_stop_seq", "shape_pt_lat", "shape_pt_lon"]
        self.map = self.load_pickle(folder_path + map_path)
        self.model = self.load_pickle(folder_path + model_path)
        self.stop_mean_eta = self.load_pickle(folder_path + stop_mean_eta_path)
        self.next_prev = self.load_pickle(folder_path + next_prev_path)[col]
        self.feed = read_feed(folder_path + feed_path, dist_units='km')

        self.data_preprocessor = DataPreprocessor(self.stop_mean_eta)
        self.route_analyzer = RouteAnalyzer(self.feed, self.map, self.next_prev)
        self.eta_predictor = ETAPredictor(self.model, self.map)
        self.trip_determiner = TripDeterminer(self.feed)

        self.map_trip_id = {'4.B001': '4B-R01_shp', '4.B011': '4B-R02_shp', 
                            '9H.R04': '9H-R04_shp', '9H.L03': '9H-R05_shp', }

    def load_pickle(self, model_path):
        # Load the pickle file
        return pd.read_pickle(model_path)
    
    async def predict_async(self, df):
        results = {}

        tasks = []
        for bus in df['bus_code'].unique():
            gps = df[df['bus_code'] == bus]
            tasks.append(self.process_bus_async(bus, gps))

        results = await asyncio.gather(*tasks)
        return {bus: result for bus, result in zip(df['bus_code'].unique(), results)}
    
    async def process_bus_async(self, bus, gps):
        try:
            gps = self.data_preprocessor.preprocess_gps_data(gps)
            gps = gps[gps['is_new'] == 1]
            if len(gps) == 0:
                raise Exception("No incoming gps data")

            gps = self.determine_following_route(gps)

            if not gps['following_route'].iloc[-1]:
                return None

            gps = self.determine_trip(gps)
            gps = self.calculate_prev_next_stops(gps)
            gps = self.calculate_next_stop_distance(gps)
            gps = self.data_preprocessor.categorize_stop(gps, num_bins=8)

            return await self.eta_predictor.predict_eta_async(gps)

        except Exception as e:
            print(f"Error processing bus {bus}: {e}")
            return None
    
    def predict(self, df, debug=False):
        results = {}
        timings = {}

        def record_time(start_time, method_name):
            if method_name not in timings:
                timings[method_name] = []
            timings[method_name].append(time.time() - start_time)

        def calculate_mean_timings():
            for method, times in timings.items():
                # mean_time = sum(times) / len(times)
                sum_time = sum(times)
                # print(f"Mean running time for {method}: {mean_time:.2f} seconds")
                print(f"Sum running time for {method}: {sum_time:.2f} seconds")

        for bus in df['bus_code'].unique():
            start_time = time.time()
            gps = df[df['bus_code'] == bus]
            if debug: record_time(start_time, 'unique')

            start_time = time.time()
            gps = self.data_preprocessor.preprocess_gps_data(gps)
            if debug: record_time(start_time, 'preprocess_gps_data')

            # start_time = time.time()
            # gps = self.calculate_mean_speed(gps)
            # if debug: record_time(start_time, 'calculate_mean_speed')

            # Filter and reset the index of the DataFrame
            gps = gps[gps['is_new'] == 1]

            start_time = time.time()
            gps = self.determine_following_route(gps)
            if debug: record_time(start_time, 'determine_following_route')

            if not gps['following_route'].iloc[-1]:
                results[bus] = None
                continue

            start_time = time.time()
            gps = self.determine_trip(gps)
            if debug: record_time(start_time, 'determine_trip')

            start_time = time.time()
            gps = self.calculate_prev_next_stops(gps)
            if debug: record_time(start_time, 'calculate_prev_next_stops')

            start_time = time.time()
            gps = self.calculate_next_stop_distance(gps)
            if debug: record_time(start_time, 'calculate_next_stop_distance')

            start_time = time.time()
            gps = gps.pipe(self.data_preprocessor.categorize_stop, num_bins=8)
            if debug: record_time(start_time, 'categorize_stop')

            start_time = time.time()
            results[bus] = self.eta_predictor.predict_eta(gps)
            if debug: record_time(start_time, 'predict_eta')

        if debug: calculate_mean_timings()
        return results
    
    def calculate_next_stop_distance(self, gps):
        """
        Calculate the distance to the next stop for each GPS entry.

        Args:
            gps (DataFrame): DataFrame containing GPS data.

        Returns:
            DataFrame: Updated DataFrame with a new column 'next_stop_dist' for the distance to the next stop.
        """
        dists = []
        for _, row in gps.iterrows():
            trip_id = self.feed.trips[self.feed.trips.shape_id == row['trip_shape']]['trip_id'].to_list()[0]
            dist = self.route_analyzer.next_stop_distance(trip_id, row['prev_stop'], row['next_stop'], row['latitude'], row['longitude'])
            dists.append(dist)
        gps['next_stop_dist'] = dists
        return gps

    def calculate_prev_next_stops(self, gps):
        """
        Determine trips and the next and previous stops for each GPS entry.

        Args:
            gps (DataFrame): DataFrame containing GPS data with columns 'trip_shape' and others.

        Returns:
            DataFrame: Updated DataFrame with trip IDs and next/previous stop information.
        """
        for col in ["next_stop", "prev_stop", "next_stop_seq", "prev_stop_seq"]:
            gps[col] = ""

        gps['trip_id'] = [
            self.feed.trips[self.feed.trips.shape_id == row['trip_shape']]['trip_id'].to_list()[0]
            for idx, (i, row) in enumerate(gps.iterrows())
        ]

        for idx, ((koridor, bus_code, trip_shape), _df) in enumerate(gps.groupby(["koridor", "bus_code", "trip_shape"])):
            next_prev_res = self.route_analyzer.test_create_naive_next_prev(_df, self.next_prev[self.next_prev["trip_id"] == trip_shape.split("_")[0]])
            gps.loc[_df.index, ["next_stop", "prev_stop", "next_stop_seq", "prev_stop_seq"]] = list(zip(*next_prev_res))

        return gps

    def determine_following_route(self, gps):
        """
        Check if each GPS entry is deviating from its route.

        Args:
            gps (DataFrame): DataFrame containing GPS data with columns 'koridor' and 'distance_route'.

        Returns:
            DataFrame: Updated DataFrame with a new column 'following_route' indicating if the entry is on route.
        """

        gps = self.route_analyzer.calculate_distance_to_routes(gps)
        routes = gps.koridor.unique()
        thresh = 100

        for route in routes:
            route_condition = gps['koridor'] == route
            gps.loc[route_condition, "following_route"] = gps.loc[route_condition, "distance_route"] <= thresh
        return gps

    def calculate_mean_speed(self, gps):
        """
        Calculate the lagged spead and the mean lagged speed for each row in the GPS data

        Args:
            gps (pd.DataFrame): DataFrame containing GPS data

        Returns:
            pd.DataFrame: Updated DataFrame with the calculated mean speed and without the used columns.
        """

        gps = self.data_preprocessor.get_speed(gps, k=10)
        gps['mean_speed'] = pd.concat([gps.loc[:,"lag_1":], gps['gpsspeed']], axis=1).mean(axis=1)
        gps = gps.drop(['time_difference_seconds']+gps.columns[gps.columns.str.startswith('lag_')].tolist(), axis=1)
        return gps

    def determine_trip(self, gps):
        """
        Determine the trip shape based on the gps coordinate

        Args:
            gps (pd.DataFrame): DataFrame containing GPS data

        Returns:
            pd.DataFrame: Updated DataFrame with the predicted trip.
        """
        if gps["trip_id"].values[0] not in self.map_trip_id:
            gps['trip_shape'] = self.trip_determiner.determine_trip(gps, gps['koridor'].unique()[0])
        else:
            gps['trip_shape'] = [self.map_trip_id[trip] for trip in gps["trip_id"].values]

        return gps