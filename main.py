import os
import pandas as pd
import requests
import numpy as np

import gtfs_kit as gk
import gtfs_kit.helpers as gh
import gtfs_kit.constants as gc
from geopy.distance import geodesic

from json import loads
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException
import models

load_dotenv()

app = FastAPI()

feed = gk.read_feed("./data/gtfs.zip", dist_units="km")
route_ids = ["4B", "D21", "9H"]

_routes = feed.routes[feed.routes["route_id"].isin(route_ids)]
_trips = feed.trips[feed.trips["route_id"].isin(route_ids)]

_stop_times = feed.stop_times[
    feed.stop_times["trip_id"].isin(_trips["trip_id"])]

# Aggregate list of trips and routes which each stop is a part of
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
async def get_routes() -> list[models.RouteTrips]:
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


@app.get("/trip/{trip_id}")
async def get_trip_by_trip_id(trip_id: str) -> models.Trip:
    trip = _trips[_trips["trip_id"] == trip_id]

    if trip.shape[0] == 0:
        raise HTTPException(status_code=404, detail="Trip not found")

    trip = trip[["route_id", "trip_id", "trip_headsign", "direction_id"]]
    trip_stats = gk.compute_trip_stats(feed, route_ids=trip["route_id"])[
        ["trip_id", "num_stops", "distance"]]

    trip = pd.merge(trip, trip_stats, on="trip_id")
    trip = trip.rename(columns={
        "trip_id": "id",
        "trip_headsign": "name",
        "direction_id": "direction",
    })

    trip["origin"] = trip.apply(
        lambda row: row["name"].split(" - ")[0], axis=1)
    trip["destination"] = trip.apply(
        lambda row: row["name"].split(" - ")[1], axis=1)

    return loads(trip.to_json(orient="records"))[0]


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


@app.get("/search/stops", response_model_exclude_none=True)
async def get_stops_by_query(query: str) -> list[models.RouteTripsStops]:
    # Search for stops which contains query
    stops = _stops.loc[
        _stops["stop_name"].str.contains(query, case=False),
        ["stop_id", "stop_name", "trips", "routes"]
    ]

    merged = pd.merge(_trips, _routes, on="route_id")

    merged["origin"] = merged.apply(
        lambda row: row["trip_headsign"].split(" - ")[0], axis=1)
    merged["destination"] = merged.apply(
        lambda row: row["trip_headsign"].split(" - ")[1], axis=1)

    merged["route_color"] = merged.apply(
        lambda row: f"0x{row['route_color']}FF", axis=1)
    merged["route_text_color"] = merged.apply(
        lambda row: f"0x{row['route_text_color']}FF", axis=1)

    merged = merged[["route_id", "trip_id", "direction_id",
                     "route_color", "route_text_color", "origin", "destination"]]
    merged = merged.rename(columns={
        "trip_id": "id",
        "route_id": "route",
        "direction_id": "direction",
        "route_color": "color",
        "route_text_color": "text_color"
    })

    # Create route-trips aggregate
    routes_ddict = defaultdict(list)
    for d in loads(merged.to_json(orient="records")):
        key = (d.pop("route"), d.pop("color"), d.pop("text_color"))
        routes_ddict[key].append(d)

    json = [
        {"id": route, "color": color, "text_color": text_color, "trips": trips}
        for (route, color, text_color), trips in routes_ddict.items()]

    # Create trip-stops aggregate
    stops_ddict = defaultdict(list)
    for _, row in stops.iterrows():
        for trip_id in row["trips"]:
            stops_ddict[trip_id].append({
                "id": row["stop_id"],
                "name": row["stop_name"]
            })

    for route in json:
        for trip in route["trips"]:
            trip["stops"] = stops_ddict.get(trip["id"], [])

    return json


@app.get("/search", response_model_exclude_none=True)
async def get_place_by_distance_or_query(
    query: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    language_code: str = "id"
) -> list[models.PlaceDetails]:
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

        places = pd.concat([places, google_places], ignore_index=True)

    if lat and lon:
        places = places.sort_values(by=["distance"])

    return loads(places.to_json(orient="records"))


@app.get("/nearest-stops", response_model_exclude_none=True)
async def get_stops_by_distance(
    lat: float,
    lon: float,
) -> list[models.PlaceDetails]:
    stops = _stops.loc[:, ["stop_id", "stop_name", "stop_lat", "stop_lon", "routes"]].copy()

    # Sort stops by distance
    stops["walking_distance"] = stops.apply(
        lambda row: geodesic((lat, lon), (row["stop_lat"], row["stop_lon"])).m,
        axis=1
    )
    stops = stops.sort_values(by=["walking_distance"])

    # rename to match model
    stops = stops.rename(columns={
        "stop_id": "id",
        "stop_name": "name",
        "stop_lat": "lat",
        "stop_lon": "lon",
    })

    # Limit to top 10
    stops = stops.head(10)

    # calculate walking distance
    stops["walking_duration"] = 5.0 # dummy: 5 minutes

    return loads(stops.to_json(orient="records"))


@app.post("/places", response_model_exclude_none=True)
async def get_places_by_ids(body: models.GetPlacesByIdBody) -> list[models.PlaceDetails]:
    stops = _stops.loc[:, ["stop_id", "stop_name", "stop_lat", "stop_lon", "routes"]].copy()
    stops = stops.rename(columns={
        "stop_id": "id",
        "stop_name": "name",
        "stop_lat": "lat",
        "stop_lon": "lon",
    })
    stops["is_stop"] = True
    lat, lon = body.lat, body.lon

    places = []
    for d in body.list_of_ids:
        if d.is_stop:
            place = stops.loc[stops["id"] == d.id, :].to_dict(orient="records")[0]
        else:
            # get place details from google places api
            url = f"https://places.googleapis.com/v1/places/{d.id}?languageCode={body.language_code}"
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": os.environ.get("PLACES_API_KEY"),
                "X-Goog-FieldMask": ",".join([
                    "displayName",
                    "id",
                    "formattedAddress",
                    "location"
                ])
            }

            response = requests.get(url, headers=headers)
            place = {
                "id": response.json()["id"],
                "name": response.json()["displayName"]["text"],
                "address": response.json()["formattedAddress"],
                "lat": response.json()["location"]["latitude"],
                "lon": response.json()["location"]["longitude"],
                "is_stop": False,
            }
        
        if lat and lon:
            # calculate walking distance
            place["walking_distance"] = geodesic((lat, lon), (place["lat"], place["lon"])).m
            
            # calculate walking duration
            place["walking_duration"] = 5.0 # dummy: 5 minutes

        places.append(place)

    return places