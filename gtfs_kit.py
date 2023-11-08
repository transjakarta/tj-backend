import json, os
import pandas as pd
import helpers as hp
import shapely.geometry as sg
import geopandas as gp
from typing import Optional, Iterable


class Feed:
    def __init__(self) -> None:
        self.routes = None
        self.stop_times = None
        self.stops = None
        self.trips = None
        self.shapes = None

    def read_feed(self, dir: str) -> None:
        self.routes = pd.read_csv(os.path.join(dir, "routes.txt"))
        self.stop_times = pd.read_csv(os.path.join(dir, "stop_times.txt"))
        self.stops = pd.read_csv(os.path.join(dir, "stops.txt"))
        self.trips = pd.read_csv(os.path.join(dir, "trips.txt"))
        self.shapes = pd.read_csv(os.path.join(dir, "shapes.txt"))


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


def read_feed(dir: str) -> Feed:
    feed = Feed()
    feed.read_feed(dir)
    return feed
