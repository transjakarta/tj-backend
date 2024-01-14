import json, os
import pandas as pd
import numpy as np
import helpers as hp
import shapely.geometry as sg
import geopandas as gp
import dateutil.relativedelta as rd
from typing import Optional, Iterable


class Feed:
    def __init__(self) -> None:
        self.routes = None
        self.stop_times = None
        self.stops = None
        self.trips = None
        self.shapes = None
        self.calendar = None
        self.calendar_dates = None
        self.frequencies = None
        self.dist_units = None

    def read_feed(self, dir: str, dist_units: str) -> None:
        self.routes = pd.read_csv(os.path.join(dir, "routes.txt"))
        self.stop_times = pd.read_csv(os.path.join(dir, "stop_times.txt"))
        self.stops = pd.read_csv(os.path.join(dir, "stops.txt"))
        self.trips = pd.read_csv(os.path.join(dir, "trips.txt"))
        self.shapes = pd.read_csv(os.path.join(dir, "shapes.txt"))
        self.calendar = pd.read_csv(os.path.join(dir, "calendar.txt"))
        self.calendar_dates = pd.read_csv(os.path.join(dir, "calendar_dates.txt"))
        self.frequencies = pd.read_csv(os.path.join(dir, "frequencies.txt"))
        self.dist_units = dist_units


def geometrize_shapes_0(shapes: pd.DataFrame, *, use_utm: bool = False) -> pd.DataFrame:
    def my_agg(group):
        d = {}
        d["geometry"] = sg.LineString(group[["shape_pt_lon", "shape_pt_lat"]].values)
        return pd.Series(d)

    g = (
        shapes.sort_values(["shape_id", "shape_pt_sequence"])
        .groupby("shape_id", sort=False)
        .apply(my_agg)
        .reset_index()
        .pipe(gp.GeoDataFrame, crs="EPSG:4326")
    )

    if use_utm:
        lat, lon = shapes[["shape_pt_lat", "shape_pt_lon"]].values[0]
        crs = hp.get_utm_crs(lat, lon)
        g = g.to_crs(crs)

    return g


def geometrize_shapes(
    feed: "Feed" = None,
    shape_ids: Optional[Iterable[str]] = None,
    *,
    use_utm: bool = False,
) -> pd.DataFrame:
    if feed.shapes is None:
        raise ValueError("This Feed has no shapes.")

    if shape_ids is not None:
        shapes = feed.shapes.loc[lambda x: x.shape_id.isin(shape_ids)]
    else:
        shapes = feed.shapes

    return geometrize_shapes_0(shapes, use_utm=use_utm)


def geometrize_trips(
    feed: "Feed", trip_ids: Optional[Iterable[str]] = None, *, use_utm=False
):
    if feed.shapes is None:
        raise ValueError("This Feed has no shapes.")

    if trip_ids is not None:
        trips = feed.trips.loc[lambda x: x.trip_id.isin(trip_ids)].copy()
    else:
        trips = feed.trips.copy()

    return (
        geometrize_shapes(feed=feed, shape_ids=trips.shape_id.tolist(), use_utm=use_utm)
        .filter(["shape_id", "geometry"])
        .merge(trips, how="left")
    )


def trips_to_geojson(
    feed: "Feed",
    trip_ids: Optional[Iterable[str]] = None,
    *,
    include_stops: bool = False,
) -> dict:
    if trip_ids is None or not list(trip_ids):
        trip_ids = feed.trips.trip_id

    D = set(trip_ids) - set(feed.trips.trip_id)
    if D:
        raise ValueError(f"Trip IDs {D} not found in feed.")

    # Get trips
    g = geometrize_trips(feed, trip_ids=trip_ids)
    trips_gj = json.loads(g.to_json())

    # Get stops if desired
    if include_stops:
        st_gj = feed.stop_times_to_geojson(trip_ids)
        trips_gj["features"].extend(st_gj["features"])

    return hp.drop_feature_ids(trips_gj)


def geometrize_stops_0(
    stops: pd.DataFrame, *, use_utm: bool = False
) -> gp.GeoDataFrame:
    g = (
        stops.assign(geometry=gp.points_from_xy(x=stops.stop_lon, y=stops.stop_lat))
        .drop(["stop_lon", "stop_lat"], axis=1)
        .pipe(gp.GeoDataFrame, crs="EPSG:4326")
    )

    if use_utm:
        lat, lon = stops[["stop_lat", "stop_lon"]].values[0]
        crs = hp.get_utm_crs(lat, lon)
        g = g.to_crs(crs)

    return g


def geometrize_stops(
    feed: "Feed", stop_ids: Optional[Iterable[str]] = None, *, use_utm: bool = False
) -> gp.GeoDataFrame:
    if stop_ids is not None:
        stops = feed.stops.loc[lambda x: x.stop_id.isin(stop_ids)]
    else:
        stops = feed.stops

    return geometrize_stops_0(stops, use_utm=use_utm)


def build_geometry_by_stop(
    feed: "Feed", stop_ids: Optional[Iterable[str]] = None, *, use_utm: bool = False
) -> dict:
    return dict(
        geometrize_stops(feed, stop_ids=stop_ids, use_utm=use_utm)
        .filter(["stop_id", "geometry"])
        .values
    )


def build_geometry_by_shape(
    feed: "Feed", shape_ids: Optional[Iterable[str]] = None, *, use_utm: bool = False
) -> dict:
    return dict(
        geometrize_shapes(feed, shape_ids=shape_ids, use_utm=use_utm)
        .filter(["shape_id", "geometry"])
        .values
    )


def compute_trip_stats(
    feed: "Feed",
    route_ids: Optional[list[str]] = None,
    *,
    compute_dist_from_shapes: bool = False,
) -> pd.DataFrame:
    f = feed.trips.copy()

    # Restrict to given route IDs
    if route_ids is not None:
        f = f[f["route_id"].isin(route_ids)].copy()

    # Merge with stop times and extra trip info.
    # Convert departure times to seconds past midnight to
    # compute trip durations later.
    if "direction_id" not in f.columns:
        f["direction_id"] = np.nan
    if "shape_id" not in f.columns:
        f["shape_id"] = np.nan

    f = (
        f[["route_id", "trip_id", "direction_id", "shape_id"]]
        .merge(feed.routes[["route_id", "route_short_name", "route_type"]])
        .merge(feed.stop_times)
        .sort_values(["trip_id", "stop_sequence"])
        .assign(departure_time=lambda x: x["departure_time"].map(hp.timestr_to_seconds))
    )

    # Compute all trips stats except distance,
    # which is possibly more involved
    geometry_by_stop = build_geometry_by_stop(feed, use_utm=True)
    g = f.groupby("trip_id")

    def my_agg(group):
        d = dict()
        d["route_id"] = group["route_id"].iat[0]
        d["route_short_name"] = group["route_short_name"].iat[0]
        d["route_type"] = group["route_type"].iat[0]
        d["direction_id"] = group["direction_id"].iat[0]
        d["shape_id"] = group["shape_id"].iat[0]
        d["num_stops"] = group.shape[0]
        d["start_time"] = group["departure_time"].iat[0]
        d["end_time"] = group["departure_time"].iat[-1]
        d["start_stop_id"] = group["stop_id"].iat[0]
        d["end_stop_id"] = group["stop_id"].iat[-1]
        dist = geometry_by_stop[d["start_stop_id"]].distance(
            geometry_by_stop[d["end_stop_id"]]
        )
        d["is_loop"] = int(dist < 400)
        d["duration"] = (d["end_time"] - d["start_time"]) / 3600
        return pd.Series(d)

    # Apply my_agg, but don't reset index yet.
    # Need trip ID as index to line up the results of the
    # forthcoming distance calculation
    h = g.apply(my_agg)

    # Compute distance
    if hp.is_not_null(f, "shape_dist_traveled") and not compute_dist_from_shapes:
        # Compute distances using shape_dist_traveled column, converting to km or mi
        if hp.is_metric(feed.dist_units):
            convert_dist = hp.get_convert_dist(feed.dist_units, "km")
        else:
            convert_dist = hp.get_convert_dist(feed.dist_units, "mi")
        h["distance"] = g.apply(
            lambda group: convert_dist(group.shape_dist_traveled.max())
        )
    elif feed.shapes is not None:
        # Compute distances using the shapes and Shapely
        geometry_by_shape = build_geometry_by_shape(feed, use_utm=True)
        # Convert to km or mi
        if hp.is_metric(feed.dist_units):
            m_to_dist = hp.get_convert_dist("m", "km")
        else:
            m_to_dist = hp.get_convert_dist("m", "mi")

        def compute_dist(group):
            """
            Return the distance traveled along the trip between the
            first and last stops.
            If that distance is negative or if the trip's linestring
            intersects itfeed, then return the length of the trip's
            linestring instead.
            """
            shape = group["shape_id"].iat[0]
            try:
                # Get the linestring for this trip
                linestring = geometry_by_shape[shape]
            except KeyError:
                # Shape ID is NaN or doesn't exist in shapes.
                # No can do.
                return np.nan

            # If the linestring intersects itfeed, then that can cause
            # errors in the computation below, so just
            # return the length of the linestring as a good approximation
            D = linestring.length
            if not linestring.is_simple:
                return D

            # Otherwise, return the difference of the distances along
            # the linestring of the first and last stop
            start_stop = group["stop_id"].iat[0]
            end_stop = group["stop_id"].iat[-1]
            try:
                start_point = geometry_by_stop[start_stop]
                end_point = geometry_by_stop[end_stop]
            except KeyError:
                # One of the two stop IDs is NaN, so just
                # return the length of the linestring
                return D
            d1 = linestring.project(start_point)
            d2 = linestring.project(end_point)
            d = d2 - d1
            if 0 < d < D + 100:
                return d
            else:
                # Something is probably wrong, so just
                # return the length of the linestring
                return D

        h["distance"] = g.apply(compute_dist)
        # Convert from meters
        h["distance"] = h["distance"].map(m_to_dist)
    else:
        h["distance"] = np.nan

    # Reset index and compute final stats
    h = h.reset_index()
    h["speed"] = h["distance"] / h["duration"]
    h[["start_time", "end_time"]] = h[["start_time", "end_time"]].applymap(
        lambda x: hp.timestr_to_seconds(x, inverse=True)
    )

    return h.sort_values(["route_id", "direction_id", "start_time"])


def read_feed(dir: str, dist_units: str) -> Feed:
    feed = Feed()
    feed.read_feed(dir, dist_units)
    return feed
