import pandas as pd
import numpy as np
from shapely.geometry import LineString, Point
from shapely.ops import nearest_points
from scipy.spatial import cKDTree
from .helper import equirectangular_approx_distance

# This class is responsible for analyzing routes and stops.
# Useful function: calculate_distance_to_routes, test_create_naive_next_prev, next_stop_distance
class RouteAnalyzer:
    def __init__(self, feed, map, next_prev):
        self.feed = feed  # GTFS feed
        self.map = map
        self.next_prev = next_prev

    def calculate_distance_to_routes(self, gps):
        """
        Calculate the equirectangular_approx_distance distance of each bus point to the nearest point on its route.

        Args:
            gps (DataFrame): DataFrame containing GPS data of buses with columns 'latitude',
                            'longitude', and 'koridor' indicating the route corridor.

        Returns:
            DataFrame: The input DataFrame with an additional column 'distance_route' representing
                      the distance in meters from each bus point to the nearest point on its route.
        """
        for koridor in gps['koridor'].unique():
            # Extract relevant trips and shapes for the corridor
            trips = self.feed.trips[self.feed.trips["route_id"] == koridor]
            shapes = self.feed.shapes[self.feed.shapes["shape_id"].isin(trips["shape_id"])]

            # Create a LineString for the route
            line_points = shapes[['shape_pt_lon', 'shape_pt_lat']].apply(tuple, axis=1)
            route_line = LineString(line_points)

            # Function to calculate distance to the route line
            def distance_to_line(row):
                point = Point(row['longitude'], row['latitude'])
                nearest_point = nearest_points(route_line, point)[0]
                return equirectangular_approx_distance((row['latitude'], row['longitude']), (nearest_point.y, nearest_point.x))['meters']

            # Apply the function to each row for the current corridor
            mask = gps['koridor'] == koridor
            gps.loc[mask, 'distance_route'] = gps[mask].apply(distance_to_line, axis=1)

        return gps

    def test_create_naive_next_prev(self, gps_data: pd.DataFrame, next_prev: pd.DataFrame):
        """
        Determines the closest next and previous stops for each GPS data point.

        Args:
        gps_data (pd.DataFrame): A DataFrame containing GPS data.
                                Must include 'longitude', 'latitude', and 'koridor' columns.
        next_prev (pd.DataFrame): A DataFrame containing shape route data.
                                  Must include 'koridor', 'shape_pt_lat', 'shape_pt_lon',
                                  'next_stop', 'prev_stop', 'next_stop_seq', 'prev_stop_seq' columns.

        Returns:
        tuple: Contains arrays for next_stop, prev_stop, next_stop_seq, prev_stop_seq.
        """
        gps_data, next_prev = gps_data.copy(), next_prev.copy()
        shape_col_coor = ["shape_pt_lat", "shape_pt_lon"]
        koridor = gps_data["koridor"].values[0]

        for col in ["next_stop", "prev_stop", "next_stop_seq", "prev_stop_seq"]:
            gps_data[col] = ""

        np_df = next_prev[next_prev["koridor"] == koridor]
        lat_values, lon_values = gps_data["latitude"].values, gps_data["longitude"].values

        next_prev_coords = np_df[shape_col_coor].values

        kdtree = cKDTree(next_prev_coords)
        distances, indices = kdtree.query(np.column_stack((lat_values, lon_values)), k=1)

        next_stop_gps = np_df.iloc[indices]["next_stop"].values
        prev_stop_gps = np_df.iloc[indices]["prev_stop"].values
        next_stop_seq = np_df.iloc[indices]["next_stop_seq"].values
        prev_stop_seq = np_df.iloc[indices]["prev_stop_seq"].values

        return (next_stop_gps, prev_stop_gps, next_stop_seq, prev_stop_seq)

    def next_stop_distance(self, trip_id, prev_stop_id, next_stop_id, lat, lon):
        """
        Calculate the distance from a given point to the next stop along a trip.

        Args:
            trip_id (str): Identifier for the trip.
            prev_stop_id (str): Identifier for the previous stop.
            next_stop_id (str): Identifier for the next stop.
            lat (float): Latitude of the current point.
            lon (float): Longitude of the current point.

        Returns:
            float: The distance in kilometers from the current point to the next stop.

        Example:
            map = {
                'trip_1': {
                    'shape': [(0, 0), (1, 1), (2, 2), (3, 3)],  # Sample coordinates for the shape
                    'status': ['stop_A', 'stop_B', 'stop_C', 'stop_D']  # Sample stop identifiers
                }
            }

            trip_id = 'trip_1'
            prev_stop_id = 'stop_B'
            next_stop_id = 'stop_D'
            lat, lon = (1.5, 1.5)

            distance_to_next_stop = next_stop_distance(_,trip_id, prev_stop_id, next_stop_id, lat, lon) # Returns 235.25 km
        """
        shape = self.map[trip_id]['shape']
        status = self.map[trip_id]['status']
        start_index = status.index(prev_stop_id)
        end_index = status.index(next_stop_id)

        insert_index = self._insert_point_to_shape((lat, lon), shape, start_index, end_index)
        shape.insert(insert_index, (lat, lon))

        total_distance = 0
        for i in range(insert_index + 1, end_index + 2):
          total_distance += equirectangular_approx_distance((shape[i - 1][0], shape[i - 1][1]), (shape[i][0], shape[i][1]))['km']

        shape.pop(insert_index)
        return total_distance

    def _insert_point_to_shape(self, coord, shape, l, r):
        """
        Insert a point into a line segment of a shape and return the index where it should be inserted.

        Args:
            coord (tuple): The coordinates (latitude, longitude) of the point to be inserted.
            shape (list): The list of coordinates defining the shape.
            l (int): The index of the starting point of the line segment in the shape.
            r (int): The index of the ending point of the line segment in the shape.

        Returns:
            int: The index in the shape where the new point should be inserted.

        Example:
            new_point_coord = (1.5, 1.5)
            start_index = 0
            end_index = 3
            shape = [(0, 0), (1, 1), (2, 2), (3, 3)]

            _insert_point_to_shape(new_point_coord, shape, start_index, end_index) # Returns 2
        """

        segment = shape[l: r + 1]
        point = Point(coord)
        linestring = LineString(segment)
        projected_point = nearest_points(linestring, point)[0].coords[0]

        tree = cKDTree(linestring.coords)
        _, idx = tree.query(projected_point, 2)
        idx.sort()
        insertion_index = l + idx[0] + 1

        return insertion_index