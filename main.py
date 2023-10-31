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

feed = gk.read_feed("./data/gtfs.zip", dist_units='km')
route_ids = ["4B", "D21", "9H"]


@app.get("/routes")
async def get_routes() -> list[models.Route]:
    routes = feed.routes[feed.routes.route_id.isin(
        route_ids)][["route_id", "route_color", "route_text_color"]]

    trips = feed.trips[feed.trips["route_id"].isin(
        route_ids)][["route_id", "trip_id", "trip_headsign", "direction_id"]]

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
    stop_times = feed.stop_times[feed.stop_times["trip_id"] == trip_id][[
        "stop_id", "stop_sequence"]]

    stops = feed.stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]]

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



@app.get("/search")
async def get_place_by_distance_or_query(query: str | None = None, lat: float | None = None, long: float | None = None, language_code: str = "id") -> list[models.Place]:
    stops = feed.get_stops()

    if query:
        filtered_stops = stops.loc[stops["stop_name"].str.contains(query, case=False), ["stop_id", "stop_name", "stop_lat", "stop_lon"]]
    else:
        filtered_stops = stops.copy()
    filtered_stops["isStop"] = True

    if query and filtered_stops.shape[0] == 0:
        url = 'https://places.googleapis.com/v1/places:searchText'
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': os.environ.get("PLACES_API_KEY"),
            'X-Goog-FieldMask': 'places.displayName,places.id'
        }

        data = {
            "textQuery": query,
            "languageCode": language_code,
        }
        if lat and long:
            data["locationBias"] = {
                "circle": {
                    "center": {
                        "latitude": lat,
                        "longitude": long
                    },
                    "radius": 500.0
                }
            }


        response = requests.post(url, json=data, headers=headers)

        google_places_df = pd.DataFrame(response.json()["places"])
        google_places_df["name"] = google_places_df["displayName"].apply(lambda x: x["text"])
        google_places_df["isStop"] = False

        place_df = google_places_df[["id", "name", "isStop"]]
    else:
        if lat and long:
            filtered_stops["distance"] = filtered_stops.apply(lambda row: geodesic((lat, long), (row["stop_lat"], row["stop_lon"])).km, axis=1)
            filtered_stops.sort_values(by=["distance"], inplace=True)
        if query is None:
            filtered_stops = filtered_stops.head(10)

        filtered_stops.rename(columns={"stop_id": "id", "stop_name": "name"}, inplace=True)
        place_df = filtered_stops[["id", "name", "isStop"]]

    return loads(place_df.to_json(orient="records"))