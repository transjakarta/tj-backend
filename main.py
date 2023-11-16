import os
import pandas as pd
import requests
import json
import asyncio

import gtfs_kit as gk
from geopy.distance import geodesic

from json import loads
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from socket_manager import PubSubWebSocketManager
from contextlib import asynccontextmanager
from redis import Redis

import models
import utils
from eta.bus_eta_application import BusETAApplication


load_dotenv()

redis = Redis(
    db=0,
    host=os.environ.get("REDIS_HOST"),
    port=os.environ.get("REDIS_PORT"),
    password=os.environ.get("REDIS_PASSWORD"),
    decode_responses=True
)

psws_manager = PubSubWebSocketManager(
    redis_host=os.environ.get("REDIS_HOST"),
    redis_port=os.environ.get("REDIS_PORT"),
    redis_password=os.environ.get("REDIS_PASSWORD")
)

# Global variables
token = ""
eta_engine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global eta_engine

    # Startup events
    redis.ping()
    eta_engine = BusETAApplication('./eta/assets/')
    asyncio.create_task(heartbeat())
    yield

    # Shutdown events
    await psws_manager.close_subscribers()


app = FastAPI(lifespan=lifespan)


# GTFS dataframes
feed = gk.read_feed("./data/gtfs", dist_units="km")
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
async def get_routes() -> list[models.TripRoute]:
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

    merged["opposite_id"] = merged.apply(
        lambda row: get_opposite_trip(row["route_id"], row["trip_id"]), axis=1)

    merged = merged.drop(columns=["trip_headsign"])
    merged = merged.rename(columns={
        "trip_id": "id",
        "route_id": "route",
        "direction_id": "direction",
        "route_color": "color",
        "route_text_color": "text_color"
    })

    return loads(merged.to_json(orient="records"))


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
        def try_get_eta(row):
            try:
                return get_etas(row["stop_id"])[0]["eta"]
            except:
                return None

        merged["eta"] = merged.apply(try_get_eta, axis=1)

    merged = merged.rename(columns={
        "stop_id": "id",
        "stop_sequence": "order",
        "stop_name": "name",
        "stop_lat": "lat",
        "stop_lon": "lon"
    })

    return loads(merged.to_json(orient="records"))


@app.get("/search/stops", response_model_exclude_none=True)
async def get_stops_by_query(query: str) -> list[models.TripRouteStops]:
    # Search for stops which contains query
    stops = _stops.loc[
        _stops["stop_name"].str.contains(query, case=False),
        ["stop_id", "stop_name", "trips", "routes"]
    ]

    # Get relevant trips and routes
    trip_ids = stops["trips"].explode().unique()
    route_ids = stops["routes"].explode().unique()

    trips = _trips[_trips["trip_id"].isin(trip_ids)]
    routes = _routes[_routes["route_id"].isin(route_ids)]

    merged = pd.merge(trips, routes, on="route_id")

    merged["origin"] = merged.apply(
        lambda row: row["trip_headsign"].split(" - ")[0], axis=1)
    merged["destination"] = merged.apply(
        lambda row: row["trip_headsign"].split(" - ")[1], axis=1)

    merged["route_color"] = merged.apply(
        lambda row: f"0x{row['route_color']}FF", axis=1)
    merged["route_text_color"] = merged.apply(
        lambda row: f"0x{row['route_text_color']}FF", axis=1)

    merged["opposite_id"] = merged.apply(
        lambda row: get_opposite_trip(row["route_id"], row["trip_id"]), axis=1)

    merged = merged[["route_id", "trip_id", "opposite_id", "direction_id",
                     "route_color", "route_text_color", "origin", "destination"]]
    merged = merged.rename(columns={
        "trip_id": "id",
        "route_id": "route",
        "direction_id": "direction",
        "route_color": "color",
        "route_text_color": "text_color"
    })

    # Create trip-stops aggregate
    json = loads(merged.to_json(orient="records"))

    stops_ddict = defaultdict(list)
    for _, row in stops.iterrows():
        for trip_id in row["trips"]:
            stops_ddict[trip_id].append({
                "id": row["stop_id"],
                "name": row["stop_name"]
            })

    for trip in json:
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
    stops = _stops.loc[:, ["stop_id", "stop_name",
                           "stop_lat", "stop_lon", "routes"]].copy()

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
    stops["walking_duration"] = 5.0  # dummy: 5 minutes

    return loads(stops.to_json(orient="records"))


@app.post("/places", response_model_exclude_none=True)
async def get_places_by_ids(body: models.GetPlacesByIdBody) -> list[models.PlaceDetails]:
    stops = _stops.loc[:, ["stop_id", "stop_name",
                           "stop_lat", "stop_lon", "routes"]].copy()
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
            place = stops.loc[stops["id"] == d.id, :].to_dict(orient="records")[
                0]
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
            place["walking_distance"] = geodesic(
                (lat, lon), (place["lat"], place["lon"])).m

            # calculate walking duration
            place["walking_duration"] = 5.0  # dummy: 5 minutes

        places.append(place)

    return places


@app.post("/navigate")
async def get_navigation(body: models.Endpoints):
    now = datetime.now()
    query = f"""
        query Navigate {{
            plan(
                from: {{lat: {body.origin_lat}, lon: {body.origin_lon}}}
                to: {{lat: {body.destination_lat}, lon: {body.destination_lon}}}
                date: "{now.strftime("%Y-%m-%d")}"
                time: "{now.strftime("%H:%M")}"
                transportModes: [{{mode: WALK}}, {{mode: TRANSIT}}]
            ) {{
                itineraries {{
                    startTime
                    endTime
                    legs {{
                        mode
                        startTime
                        endTime
                        from {{
                            name
                            lat
                            lon
                        }}
                        to {{
                            name
                            lat
                            lon
                        }}
                        route {{
                            longName
                            shortName
                            stops {{
                                gtfsId
                                name
                            }}
                        }}
                        legGeometry {{
                            points
                        }}
                    }}
                }}
            }}
        }}
    """

    response = requests.post(
        url="http://graph:8080/otp/routers/default/index/graphql", json={"query": query})

    data = response.json()
    itineraries = data["data"]["plan"]["itineraries"]

    for itinerary in itineraries:
        for leg in itinerary["legs"]:
            if leg["mode"] == "BUS" and "route" in leg and leg["route"]:
                for stop in leg["route"]["stops"]:
                    try:
                        stop_id = stop["gtfsId"].split(":")[-1]
                        eta = get_etas(stop_id)[0]["eta"]

                        if eta:
                            stop["eta"] = eta
                    except:
                        stop["eta"] = None

                    del stop["gtfsId"]

    return data


@app.websocket("/bus/{bus_code}/ws")
async def websocket_bus_gps(websocket: WebSocket, bus_code: str) -> None:
    channel = f"bus.{bus_code}"
    await psws_manager.subscribe_to_channel(channel, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await psws_manager.disconnect_from_channel(channel, websocket)


@app.websocket("/trips/{trip_id}/ws")
async def websocket_bus_gps(websocket: WebSocket, trip_id: str) -> None:
    mapped_trip_id = utils.map_gtfs_trip(trip_id)

    channel = f"trip.{mapped_trip_id}"
    await psws_manager.subscribe_to_channel(channel, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await psws_manager.disconnect_from_channel(channel, websocket)


# Fetch access token if not already available or already stale
async def tj_login():
    global token

    url = "http://esb.transjakarta.co.id/api/v2/auth/signin"

    payload = json.dumps({
        "username": os.environ.get("TJ_USERNAME"),
        "password": os.environ.get("TJ_PASSWORD"),
    })

    headers = {
        "api_key": os.environ.get("TJ_API_KEY"),
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, data=payload)
    token = response.json()["accessToken"]


# Fetch real-time GPS data periodically
async def tj_fetch():
    global token

    url = "http://esb.transjakarta.co.id/api/v2/gps/listGPSBusTripUI"
    headers = {"x-access-token": token}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception()

    data = response.json()["data"]
    return pd.DataFrame.from_dict(data)


# Broadcast real-time GPS data and save history
async def broadcast_gps(df):
    async def broadcast_to_bus_channel(row):
        channel = f"bus.{row['bus_code']}"

        redis.lpush(channel, json.dumps(row.to_dict()))
        redis.ltrim(channel, 0, 19)

        await psws_manager.broadcast_to_channel(channel, json.dumps({
            "id": row["bus_code"],
            "route_id": row["koridor"],
            "trip_id": row["trip_id"],
            "timestamp": row["gpsdatetime"],
            "lat": row["latitude"],
            "lon": row["longitude"],
            "head": row["gpsheading"],
            "speed": row["gpsspeed"],
        }))

    async def broadcast_to_trip_channel(trip_id, rows):
        channel = f"trip.{trip_id}"
        await psws_manager.broadcast_to_channel(channel, json.dumps(rows.to_dict(('records'))))

    tasks = []
    for _, row in df.iterrows():
        tasks.append(broadcast_to_bus_channel(row))

    renamed_df = df.drop(columns=["color"]) \
        .rename(columns={
            "bus_code": "id",
            "koridor": "route_id",
            "gpsdatetime": "timestamp",
            "latitude": "lat",
            "longitude": "lon",
            "gpsheading": "head",
            "gpsspeed": "speed"
        })

    for trip_id in renamed_df["trip_id"].unique():
        rows = renamed_df[renamed_df["trip_id"] == trip_id]
        tasks.append(broadcast_to_trip_channel(trip_id, rows))

    await asyncio.gather(*tasks)


# Predict ETA for each bus based on GPS data
async def predict_eta(df):
    new_df = pd.DataFrame(columns=["bus_code", "koridor", "gpsdatetime", "latitude",
                          "longitude", "color", "gpsheading", "gpsspeed", "is_new", "trip_id"])

    for _, row in df.iterrows():
        row["is_new"] = True
        new_df = pd.concat([new_df, row.to_frame().T], ignore_index=True)

        history_df = get_bus_history(row["bus_code"])
        if history_df.shape[0] >= 10:
            new_df = pd.concat([new_df, history_df], ignore_index=True)

    new_df.reset_index(drop=True, inplace=True)
    if new_df.groupby(["bus_code"]).count()["gpsdatetime"].max() < 10:
        return

    prediction = await eta_engine.predict_async(new_df)

    for bus_id, stops in prediction.items():
        if not stops:
            continue

        for stop_id, eta in stops.items():
            stop_key = f"stop.{stop_id}"
            value = json.dumps({"eta": eta, "bus_id": bus_id})

            redis.hset(stop_key, bus_id, value)


async def poll_api():
    try:
        df = await tj_fetch()
        df = df.drop(columns=["trip_desc"])
        df["trip_id"] = df.apply(
            lambda x: utils.map_gps_trip(x["trip_id"]), axis=1)

        await broadcast_gps(df)
        await predict_eta(df)
    except Exception as e:
        print(e)
        await tj_login()


def get_opposite_trip(route: str, trip: str):
    opposite_trips = _trips[(_trips["route_id"] == route)
                            & (_trips["trip_id"] != trip)]

    if opposite_trips.shape[0] == 0:
        return None

    return opposite_trips.iloc[0]["trip_id"]


# Fetch latest bus history from redis
def get_bus_history(bus_id):
    channel = f"bus.{bus_id}"
    entries = redis.lrange(channel, 0, 19)

    history_dicts = [json.loads(entry) for entry in entries]
    if history_dicts:
        return pd.DataFrame(history_dicts)

    return pd.DataFrame()


# Fetch all ETA of a stop from redis
def get_etas(stop_id):
    stop_key = f"stop.{stop_id}"
    etas = []

    all_etas = redis.hgetall(stop_key)
    for eta_info in all_etas.values():
        etas.append(json.loads(eta_info))

    etas.sort(key=lambda x: x["eta"])
    return etas


async def heartbeat():
    print("Heartbeat started")
    await tj_login()

    while True:
        print(
            f"Heartbeat received on {datetime.now().strftime('%Y/%m/%d, %H:%M:%S')}")
        await poll_api()
        await asyncio.sleep(5)
