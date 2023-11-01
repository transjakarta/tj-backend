import os
import pandas as pd
import requests
import gtfs_kit as gk
import gtfs_kit.helpers as gh
import gtfs_kit.constants as gc
from geopy.distance import geodesic

from json import loads
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

from fastapi import FastAPI
import models

load_dotenv()

app = FastAPI()

feed = gk.read_feed("./data/gtfs.zip", dist_units="km")
route_ids = ["4B", "D21", "9H"]

_routes = feed.routes[feed.routes["route_id"].isin(route_ids)]
_trips = feed.trips[feed.trips["route_id"].isin(route_ids)]

_stop_times = feed.stop_times[
    feed.stop_times["trip_id"].isin(_trips["trip_id"])]

_stops = feed.stops[feed.stops["stop_id"].isin(_stop_times["stop_id"])]
_stops = _stops.merge(
    _stop_times
    .merge(_trips, on="trip_id")
    .merge(_stops, on="stop_id")
    .groupby("stop_id").agg({
        "trip_id": lambda x: list(set(x)),
        "route_id": lambda x: list(set(x))
    })
    .rename(columns={
        "trip_id": "trips",
        "route_id": "routes"
    })
    .reset_index(),
    on="stop_id",
    how="left"
)


@app.get("/routes")
async def get_routes() -> list[models.Route]:
    routes = _routes[["route_id", "route_color", "route_text_color"]]
    trips = _trips[["route_id", "trip_id", "trip_headsign", "direction_id"]]

    trip_stats = gk.compute_trip_stats(feed, route_ids=route_ids)[
        ["trip_id", "num_stops", "distance"]]

    merged = pd.merge(trips, trip_stats, on="trip_id")
    merged = pd.merge(merged, routes, on="route_id")

    merged["origin"] = merged.apply(
        lambda row: row["trip_headsign"].split(" - ")[0], axis=1)
    merged["destination"] = merged.apply(
        lambda row: row["trip_headsign"].split(" - ")[1], axis=1)

    merged["route_color"] = merged.apply(
        lambda row: f"0x{row['route_color']}FF", axis=1)
    merged["route_text_color"] = merged.apply(
        lambda row: f"0x{row['route_text_color']}FF", axis=1)

    merged = merged.drop(columns=["trip_headsign"])
    merged = merged.rename(columns={
        "trip_id": "id",
        "route_id": "route",
        "direction_id": "direction",
        "route_color": "color",
        "route_text_color": "text_color"
    })

    ddict = defaultdict(list)
    for d in loads(merged.to_json(orient="records")):
        key = (d.pop("route"), d.pop("color"), d.pop("text_color"))
        ddict[key].append(d)

    json = [
        {"id": route, "color": color, "text_color": text_color, "trips": trips}
        for (route, color, text_color), trips in ddict.items()]

    return json


@app.get("/trip/{trip_id}/geojson")
async def get_trip_geojson_by_trip_id(trip_id: str):
    json = gk.trips_to_geojson(feed, trip_ids=[trip_id])["features"][0]
    del json["properties"]
    return json


@app.get("/stops/{trip_id}", response_model_exclude_none=True)
async def get_stops_by_route_id(trip_id: str, include_eta: bool = False) -> list[models.StopEta]:
    stop_times = _stop_times.loc[
        _stop_times["trip_id"] == trip_id,
        ["stop_id", "stop_sequence"]]

    stops = _stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]]

    merged = pd.merge(stop_times, stops, on="stop_id")
    merged = merged.sort_values(by=["stop_sequence"])

    if include_eta:
        merged["eta"] = datetime.now().replace(microsecond=0).isoformat()

    merged = merged.rename(columns={
        "stop_id": "id",
        "stop_sequence": "order",
        "stop_name": "name",
        "stop_lat": "lat",
        "stop_lon": "lon"
    })

    return loads(merged.to_json(orient="records"))


@app.get("/search", response_model_exclude_none=True)
async def get_place_by_distance_or_query(
    query: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    language_code: str = "id"
) -> list[models.Place]:
    global _stops
    stops = _stops.copy()

    # Search for stops which contains query
    if query:
        stops = stops.loc[
            stops["stop_name"].str.contains(query, case=False),
            ["stop_id", "stop_name", "stop_lat", "stop_lon", "routes"]
        ]

    # Set default is_stop to true
    stops["is_stop"] = True
    stops = stops.rename(columns={
        "stop_id": "id",
        "stop_name": "name",
        "stop_lat": "lat",
        "stop_lon": "lon",
    })

    # Sort stops by distance if coordinate is provided
    if lat and lon:
        stops["distance"] = stops.apply(
            lambda row: geodesic((lat, lon), (row["lat"], row["lon"])).km,
            axis=1
        )
        stops = stops.sort_values(by=["distance"])

    # Limit to top 10
    places = stops.head(10)

    if query:
        url = "https://places.googleapis.com/v1/places:searchText"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": os.environ.get("PLACES_API_KEY"),
            "X-Goog-FieldMask": ",".join([
                "places.displayName",
                "places.id",
                *(["places.location"] if lat and lon else [])
            ])
        }

        body = {
            "textQuery": query,
            "languageCode": language_code,
            "maxResultCount": 10 - places.shape[0],
            "locationBias": {
                "circle": {
                    "center": {
                        "latitude": lat,
                        "longitude": lon
                    },
                    "radius": 500.0
                }
            } if lat and lon else None
        }

        response = requests.post(url, json=body, headers=headers)
        google_places = pd.DataFrame(response.json()["places"])

        google_places["name"] = google_places["displayName"] \
            .apply(lambda x: x["text"])
        google_places["is_stop"] = False

        if lat and lon:
            google_places["distance"] = google_places.apply(
                lambda row: geodesic(
                    (lat, lon),
                    (row["location"]["latitude"], row["location"]["longitude"])
                ).km,
                axis=1
            )
            google_places = google_places.sort_values(by=["distance"])

        places = pd.concat(
            [places, google_places[["id", "name", "is_stop", "distance"]]],
            ignore_index=True
        )

    if lat and lon:
        places = places.sort_values(by=["distance"])

    return loads(places.to_json(orient="records"))
