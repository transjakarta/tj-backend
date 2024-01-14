import pandas as pd
import lib.gtfs_kit as gk
from geopy.distance import geodesic


class GTFSManager:
    """Class to manage GTFS data and functionalities"""

    def __init__(self, gtfs_path, available_route_ids):
        # Read the GTFS data and set distance units to kilometers
        self.feed = gk.read_feed(gtfs_path, dist_units="km")
        self.available_route_ids = available_route_ids

        # Filter the routes dataframe to only include the routes specified in route_ids
        self._routes = self.feed.routes[
            self.feed.routes["route_id"].isin(available_route_ids)]

        # Filter the trips dataframe to include only trips that are part of the selected routes
        self._trips = self.feed.trips[
            self.feed.trips["route_id"].isin(available_route_ids)]

        # Filter the stop_times dataframe to include only stop times that belong to the selected trips
        self._stop_times = self.feed.stop_times[
            self.feed.stop_times["trip_id"].isin(self._trips["trip_id"])]

        # Prepare the stops dataframe which includes list of trips and routes that each stop is a part of
        self._stops = self.feed.stops[
            self.feed.stops["stop_id"].isin(self._stop_times["stop_id"])]
        self._stops = self._stops.merge(
            self._stop_times
            .merge(self._trips, on="trip_id")
            .merge(self._stops, on="stop_id")
            .groupby("stop_id")
            .agg({  # Group by stop_id and aggregate trip_ids and route_ids
                "trip_id": lambda x: list(set(x)),
                "route_id": lambda x: list(set(x))
            })
            .rename(columns={
                "trip_id": "trips",
                "route_id": "routes"
            })
            .reset_index(),
            on="stop_id",
            how="left"  # Merge using left join to keep all stops
        )

    def get_all_trips(self):
        """Return all available trips and its details"""

        routes = self._routes[["route_id",
                               "route_color",
                               "route_text_color"]]

        trips = self._trips[["route_id",
                             "trip_id",
                             "trip_headsign",
                             "direction_id"]]

        trip_stats = gk.compute_trip_stats(
            self.feed, route_ids=self.available_route_ids)
        trip_stats = trip_stats[["trip_id", "num_stops", "distance"]]

        # Merge trips dataframe with trip statistics, and then with route information
        merged = pd.merge(trips, trip_stats, on="trip_id")
        merged = pd.merge(merged, routes, on="route_id")

        merged["origin"] = merged.apply(
            lambda row: row["trip_headsign"].split(" - ")[0],
            axis=1)

        merged["destination"] = merged.apply(
            lambda row: row["trip_headsign"].split(" - ")[1],
            axis=1)

        merged["route_color"] = merged.apply(
            lambda row: f"0x{row['route_color']}FF",
            axis=1)

        merged["route_text_color"] = merged.apply(
            lambda row: f"0x{row['route_text_color']}FF",
            axis=1)

        merged["opposite_id"] = merged.apply(
            lambda row: self.get_opposite_trip(
                row["route_id"], row["trip_id"]),
            axis=1)

        merged = merged.drop(columns=["trip_headsign"])
        merged = merged.rename(columns={
            "trip_id": "id",
            "route_id": "route",
            "direction_id": "direction",
            "route_color": "color",
            "route_text_color": "text_color"
        })

        return merged

    def get_trip_details(self, trip_id: str):
        """Return details of a specific trip"""

        trip = self._trips[self._trips["trip_id"] == trip_id]

        # Return None if no trip is found
        if trip.shape[0] == 0:
            return None

        trip = trip[["route_id", "trip_id", "trip_headsign", "direction_id"]]
        trip_stats = gk.compute_trip_stats(
            self.feed, route_ids=trip["route_id"])
        trip_stats = trip_stats[["trip_id", "num_stops", "distance"]]

        trip = pd.merge(trip, trip_stats, on="trip_id")
        trip = trip.rename(columns={
            "trip_id": "id",
            "trip_headsign": "name",
            "direction_id": "direction",
        })

        trip["origin"] = trip.apply(
            lambda row: row["name"].split(" - ")[0],
            axis=1)

        trip["destination"] = trip.apply(
            lambda row: row["name"].split(" - ")[1],
            axis=1)

        return trip

    def get_trip_geojson(self, trip_id: str):
        """Return GeoJSON shape of a specific trip"""

        json = gk.trips_to_geojson(self.feed, trip_ids=[trip_id])
        json = json["features"][0]

        del json["properties"]

        return json

    def get_opposite_trip(self, route_id: str, trip_id: str):
        """Return trip with the opposite direction"""

        opposite_trips = self._trips[(self._trips["route_id"] == route_id)
                                     & (self._trips["trip_id"] != trip_id)]

        if opposite_trips.shape[0] == 0:
            return None

        return opposite_trips.iloc[0]["trip_id"]

    def get_stops(self, trip_id: str):
        """Return stops of a specific trip"""

        stop_times = self._stop_times.loc[
            self._stop_times["trip_id"] == trip_id,
            ["stop_id", "stop_sequence"]]

        if stop_times.shape[0] == 0:
            return None

        stops = self._stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]]

        merged = pd.merge(stop_times, stops, on="stop_id")
        merged = merged.sort_values(by=["stop_sequence"])

        merged = merged.rename(columns={
            "stop_id": "id",
            "stop_sequence": "order",
            "stop_name": "name",
            "stop_lat": "lat",
            "stop_lon": "lon"
        })

        return merged

    def get_nearest_stops(self, lat: float, lon: float, limit: int = 10):
        """Return stops nearest to a specific coordinate"""

        stops = self._stops.loc[:, ["stop_id", "stop_name",
                                    "stop_lat", "stop_lon", "routes"]].copy()

        # Sort stops by distance
        stops["distance"] = stops.apply(
            lambda row: geodesic(
                (lat, lon), (row["stop_lat"], row["stop_lon"])).m,
            axis=1
        )
        stops["walking_distance"] = stops["distance"]

        # Sort and limit results
        stops = stops.sort_values(by=["distance"])
        stops = stops.head(limit)

        # Rename to match model
        stops = stops.rename(columns={
            "stop_id": "id",
            "stop_name": "name",
            "stop_lat": "lat",
            "stop_lon": "lon",
        })

        # TODO: calculate walking distance
        stops["walking_duration"] = 5.0

        return stops

    def search_stops(self, query: str):
        """Return stops from a query"""
        pass

    # TODO: ini hack jelek banget, nanti refactor
    def get_start_time(self, trip_id):
        if trip_id not in self._trips["trip_id"].values:
            return "05:00:00"

        return self.feed.frequencies[self.feed.frequencies["trip_id"] == trip_id]["start_time"].iloc[0]

    # TODO: ini hack jelek banget, nanti refactor
    def get_start_date(self, trip_id):
        if trip_id not in self._trips["trip_id"].values:
            return "20040115"

        service_id = self._trips[
            self._trips["trip_id"] == trip_id]["service_id"].iloc[0]
        return self.feed.calendar[self.feed.calendar["service_id"] == service_id]["start_date"].iloc[0]
