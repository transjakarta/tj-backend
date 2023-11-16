from collections import Counter
from queue import Queue
from shapely.ops import nearest_points
from shapely.geometry import LineString, Point
from scipy.spatial import cKDTree
from .helper import equirectangular_approx_distance

# Class TripDeterminer: class that wrap up supporting function for determine_trip
# trip_determiner = TripDeterminer(feed, shape_koridor)
# trip_determiner.determine_trip(self, gps_data, "4B")
class TripDeterminer:
    def __init__(self, feed, K=5, debug=False):
        """
        Initialize the TripDeterminer class.

        Args:
            shape_koridor (dict): Dictionary containing predefined routes as Shapely LineString objects.
            K (int, optional): Maximum queue length for the voting mechanism. Defaults to 5.
            debug (bool, optional): Enable debugging information. Defaults to False.
        """
        self.K = K
        self.debug = debug
        self.feed = feed

    def determine_trip(self, gps_data, route_name):
        """
        Helper function to determine the trip based on GPS data and a predefined route.

        Args:
            gps_data (DataFrame): DataFrame containing GPS data with 'longitude' and 'latitude' columns.
            route_name (str): The name of the route to consider (e.g., "4B").

        Returns:
            Tuple: A tuple containing debugging information and the determined trip.
        """
        shape_koridor = self._create_shape_koridor(route_name)
        trip1_name, trip2_name = self._get_trip_names(shape_koridor)

        # Use the _determine_trip_helper method from TripDeterminer
        return self._determine_trip_helper(gps_data, shape_koridor, trip1_name, trip2_name)

    def _determine_trip_helper(self, gps_data, shape_koridor, trip1_name, trip2_name=None):
        """
        Determines the route based on GPS data and a set of predefined routes.

        Args:
            gps_data (DataFrame): DataFrame containing GPS data with 'longitude' and 'latitude' columns.
            trip1_name (str): Name of the primary route to consider.
            trip2_name (str, optional): Name of the secondary route for round trips. Defaults to None.

        Returns:
            Tuple: A tuple containing a list of chosen trips and a list of methods used for debugging.
        """
        results_trip = []
        no_methods = []  # For debugging purpose

        queueK = Queue(maxsize=self.K)
        previous_point = None
        trip1 = shape_koridor[trip1_name]
        trip2 = shape_koridor[trip2_name] if trip2_name else None

        for index, row in gps_data.iterrows():
            current_point = (row['longitude'], row['latitude'])
            chosen_trip, method = self._choose_trip(current_point, previous_point, trip1, trip2, trip1_name, trip2_name)
            no_methods.append(method)

            if self.debug:
                print(f'[{method}]', index, chosen_trip)

            if method != 3:  # Not skipping iteration
                previous_point = current_point
                queueK.put(chosen_trip)
                chosen_trip = self._get_most_common_trip(queueK, chosen_trip)
                results_trip.append(chosen_trip)
            else:
                results_trip.append(results_trip[-1])

        if self.debug:
          return no_methods, results_trip
        else:
          return results_trip

    def _choose_trip(self, current_point, previous_point, trip1, trip2, trip1_name, trip2_name):
        """
        Choose the appropriate trip based on the current and previous GPS points.

        Args:
            current_point (tuple): Current GPS point.
            previous_point (tuple): Previous GPS point.
            trip1 (LineString): First trip route.
            trip2 (LineString): Second trip route.

        Returns:
            Tuple: A tuple containing the chosen trip name and the method number used.
        """
        nearest_distance_trip1 = self._get_nearest_distance(current_point, trip1)

        if trip2:
            nearest_distance_trip2 = self._get_nearest_distance(current_point, trip2)
            if previous_point is None:
                return (trip1_name if nearest_distance_trip1 < nearest_distance_trip2 else trip2_name, 1)
            elif abs(nearest_distance_trip1 - nearest_distance_trip2) > 20:
                return (trip1_name if nearest_distance_trip1 < nearest_distance_trip2 else trip2_name, 2)
        else:
            if previous_point is None:
                return (trip1_name, 2)

        if previous_point and equirectangular_approx_distance(previous_point[::-1], current_point[::-1])["meters"] <= 15:
            return (None, 3)  # Use the same trip as before

        if trip2:
            line = trip1
            points = line.coords
            tree = cKDTree(points)
            first, idx = self._first_passed(previous_point, current_point, line, tree)
            if idx[0] <= 1:
                return (trip1_name, 4)
            elif idx[1] <= 1:
                return (trip2_name, 5)
            else:
                return (trip1_name if first == "A" else trip2_name, 6)
        else:
            return (trip1_name, 7)

    def _get_nearest_distance(self, point, line):
        """
        Get the nearest distance from a point to a line.

        Args:
            point (tuple): The GPS point.
            line (LineString): The line (route).

        Returns:
            float: The nearest distance in meters.
        """
        nearest_point = nearest_points(line, Point(point))[0]
        nearest_coords = (nearest_point.y, nearest_point.x)
        return equirectangular_approx_distance(point[::-1], nearest_coords)["meters"]

    def _first_passed(self, pointA, pointB, line, tree):
        """
        Determine which point, A or B, passed first on a given line.

        Parameters:
        - pointA (tuple): Coordinates of point A (longitude, latitude).
        - pointB (tuple): Coordinates of point B (longitude, latitude).
        - line (LineString): A Shapely LineString object representing the path.
        - tree (cKDTree): A scipy.spatial.cKDTree object built from the coordinates of the line.

        Returns:
        - str: 'A' if point A is passed first, 'B' if point B is passed first.
        - tuple: A tuple containing the indices of the closest points to A and B on the line.

        Example:
        line = LineString([(0, 0), (1, 1), (2, 1), (3, 3)])
        pointA = (0.5, 0.5)
        pointB = (2.5, 2.5)
        tree = cKDTree(line.coords)
        _first_passed(pointA, pointB, line, tree)  # Returns ('A', (0, 1))
        """

        # Find the nearest points on the LineString for pointA and pointB
        nearest_pointA = nearest_points(line, Point(pointA))[0]
        nearest_pointB = nearest_points(line, Point(pointB))[0]

        n = len(line.coords)

        # Find the nearest point indices in the KD-tree for pointA and pointB
        _, idxA = tree.query(nearest_pointA.coords[0])
        _, idxB = tree.query(nearest_pointB.coords[0])
        idxs = min(idxA,idxB), min(n-idxA, n-idxB)

        # Determine the direction based on the indices
        if idxA < idxB:
            return "A", idxs
        elif idxA > idxB:
            return "B", idxs
        else:
            # If indices are equal, compare distances to the previous point in the line
            prev_point = line.coords[idxA-1]
            distA = equirectangular_approx_distance(prev_point[::-1], pointA[::-1])["meters"]
            distB = equirectangular_approx_distance(prev_point[::-1], pointB[::-1])["meters"]
            return ("A" if distA < distB else "B", idxs)

    def _get_most_common_trip(self, queueK, default_trip):
        """
        Get the most common trip from the queue.

        Args:
            queueK (Queue): The queue containing recent trip choices.
            default_trip (str): Default trip name if the queue has only one element.

        Returns:
            str: The most common trip name.
        """
        queue_list = list(queueK.queue)
        if len(queue_list) == 1:
            return default_trip
        else:
            mode_item = Counter(queue_list).most_common(1)[0][0]
            if len(queue_list) == self.K:
                queueK.get()
            return mode_item

    def _create_shape_koridor(self, route_name):
        """
        Creates a dictionary of LineString objects representing the routes.

        Args:
            route_name (str): The name of the route.

        Returns:
            dict: A dictionary with shape IDs as keys and LineString objects as values.
        """
        shape_koridor = {}
        trips = self.feed.trips.loc[self.feed.trips["route_id"].isin([route_name])].reset_index()
        shapes = self.feed.shapes.loc[self.feed.shapes["shape_id"].isin(trips.shape_id)].reset_index()

        for i in shapes['shape_id'].unique():
            line_points = [(row['shape_pt_lon'], row['shape_pt_lat']) for index, row in shapes[shapes['shape_id'] == i].iterrows()]
            shape_koridor[i] = LineString(line_points)

        return shape_koridor

    def _get_trip_names(self, shape_koridor):
        """
        Retrieves the primary and secondary trip names from the shape_koridor.

        Args:
            shape_koridor (dict): A dictionary of route shapes.

        Returns:
            tuple: A tuple containing the primary and secondary trip names.
        """
        trip_names = list(shape_koridor.keys())
        trip1_name = trip_names[0]
        trip2_name = trip_names[1] if len(trip_names) > 1 else None
        return trip1_name, trip2_name