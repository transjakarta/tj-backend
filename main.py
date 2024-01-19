# Standard library imports
import asyncio
import json
import os

from collections import defaultdict
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from json import loads

# Related third-party imports
import pandas as pd
import pytz
import requests

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from geopy.distance import geodesic
from redis import Redis

# Local application/library-specific import
import lib.gtfs_kit as gk
import models
import utils

from eta.bus_eta_application import BusETAApplication
from gtfs_manager import GTFSManager
from gtfs_realtime_manager import GTFSRealtimeManager
from lib.socket_manager import PubSubWebSocketManager


# Load environment variables from .env file
load_dotenv()


# Set timezone for Jakarta
timezone = pytz.timezone("Asia/Jakarta")


# Set up Redis connection
redis = Redis(
    db=0,
    host=os.environ.get("REDIS_HOST"),
    port=os.environ.get("REDIS_PORT"),
    password=os.environ.get("REDIS_PASSWORD"),
    decode_responses=True
)

# Initialize PubSub WebSocket Manager
psws_manager = PubSubWebSocketManager(
    redis_host=os.environ.get("REDIS_HOST"),
    redis_port=os.environ.get("REDIS_PORT"),
    redis_password=os.environ.get("REDIS_PASSWORD")
)


# Instance for GTFS manager
gtfs_manager = GTFSManager("./gtfs", ["4B", "D21", "9H"])
realtime_manager = GTFSRealtimeManager()

# Instance for ETA engine class
eta_engine = None

# Store bearer token for TransJakarta API authentication
token = ""


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Define an asynchronous context manager for managing the lifecycle of the FastAPI app"""

    global eta_engine

    # Check Redis connection
    redis.ping()

    # Initialize ETA engine instance
    eta_engine = BusETAApplication("./eta/assets/")

    # Start polling task
    asyncio.create_task(poll())

    # Yield control back to the FastAPI app
    yield

    # Shutdown events
    # Gracefully close WebSocket subscribers
    await psws_manager.close_subscribers()

# Initialize the FastAPI application
app = FastAPI(lifespan=lifespan)


# GTFS dataframes setup
# Read the GTFS data and set distance units to kilometers
feed = gk.read_feed("./gtfs", dist_units="km")

# Define specific route IDs that are of interest for the application
available_route_ids = ["4B", "D21", "9H"]

# Filter the routes dataframe to only include the routes specified in route_ids
_routes = feed.routes[feed.routes["route_id"].isin(available_route_ids)]

# Filter the trips dataframe to include only trips that are part of the selected routes
_trips = feed.trips[feed.trips["route_id"].isin(available_route_ids)]

# Filter the stop_times dataframe to include only stop times that belong to the selected trips
_stop_times = feed.stop_times[
    feed.stop_times["trip_id"].isin(_trips["trip_id"])]

# Prepare the stops dataframe
# Start by filtering the stops to include only those that are part of the selected stop times
_stops = feed.stops[feed.stops["stop_id"].isin(_stop_times["stop_id"])]

# Merge the stops dataframe with aggregated data
# Aggregate data includes list of trips and routes that each stop is a part of
_stops = _stops.merge(
    _stop_times
    .merge(_trips, on="trip_id")  # Merge stop times with trips
    .merge(_stops, on="stop_id")  # Merge the result with stops
    .groupby("stop_id").agg({     # Group by stop_id and aggregate trip_ids and route_ids
        # Unique list of trip_ids for each stop
        "trip_id": lambda x: list(set(x)),
        # Unique list of route_ids for each stop
        "route_id": lambda x: list(set(x))
    })
    .rename(columns={  # Rename columns for clarity
        "trip_id": "trips",
        "route_id": "routes"
    })
    .reset_index(),
    on="stop_id",
    how="left"  # Merge using left join to keep all stops
)


# TODO: Change to /trips
@app.get("/routes")
async def get_all_trips() -> list[models.TripRoute]:
    """Read all available trips and its details"""

    trips = gtfs_manager.get_all_trips()
    return loads(trips.to_json(orient="records"))


@app.get("/trip/{trip_id}")
async def get_trip_details_by_trip_id(trip_id: str) -> models.Trip:
    """Read details of a specific trip"""

    trip = gtfs_manager.get_trip_details(trip_id)

    if trip is None:
        raise HTTPException(status_code=404, detail="Trip not found")

    return loads(trip.to_json(orient="records"))[0]


@app.get("/trip/{trip_id}/geojson")
async def get_trip_geojson_by_trip_id(trip_id: str):
    """Read GeoJSON shape of a specific trip"""

    geojson = gtfs_manager.get_trip_geojson(trip_id)
    return geojson


# TODO: Change to /trip/{trip_id}/stops
@app.get("/stops/{trip_id}", response_model_exclude_none=True)
async def get_trip_stops_by_trip_id(trip_id: str, include_eta: bool = False) -> list[models.StopEta]:
    """Read stops of a specific trip"""

    stops = gtfs_manager.get_stops(trip_id)

    if include_eta:
        def try_get_etas(row):
            try:
                return get_etas(row["id"])[0]["eta"]
            except:
                return None

        stops["eta"] = stops.apply(try_get_etas, axis=1)

    return loads(stops.to_json(orient="records"))


import pytz
import requests
# TODO: Extract logic to gtfs_manager
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


# TODO: Extract logic to gtfs_manager and possibly google places manager
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

    places = stops
    if len(stops) > 0:
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
                "places.formattedAddress",
                "places.location"
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
        google_places["lat"] = google_places["location"] \
            .apply(lambda x: x["latitude"])
        google_places["lon"] = google_places["location"] \
            .apply(lambda x: x["longitude"])

        google_places.rename(
            columns={"formattedAddress": "address"},
            inplace=True)

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
async def get_nearest_stops(
    lat: float,
    lon: float,
) -> list[models.PlaceDetails]:
    """Get stops nearest to a specific coordinate"""

    stops = gtfs_manager.get_nearest_stops(lat, lon)
    return loads(stops.to_json(orient="records"))


# TODO: Extract logic to gtfs_manager
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
    query = f"""
        query Navigate {{
            plan(
                from: {{lat: {body.origin_lat}, lon: {body.origin_lon}}}
                to: {{lat: {body.destination_lat}, lon: {body.destination_lon}}}
            ) {{
                itineraries {{
                    startTime
                    endTime
                    legs {{
                        mode
                        duration
                        distance
                        startTime
                        endTime
                        from {{
                            name
                            lat
                            lon
                            stop {{
                                gtfsId
                            }}
                        }}
                        to {{
                            name
                            lat
                            lon
                            stop {{
                                gtfsId
                            }}
                        }}
                        trip {{
                            gtfsId
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
        url="http://graph:8080/otp/routers/default/index/graphql",
        json={"query": query})

    data = response.json()

    itineraries = data["data"]["plan"]["itineraries"]
    valid_itineraries = [deepcopy(itinerary)
                         for itinerary in itineraries
                         for leg in itinerary["legs"]
                         if leg["mode"] == "BUS"]

    for itinerary in valid_itineraries:
        itinerary["startTime"] = utils \
            .convert_epoch_to_isostring(itinerary["startTime"])
        itinerary["endTime"] = utils \
            .convert_epoch_to_isostring(itinerary["endTime"])

        for leg in itinerary["legs"]:
            leg["startTime"] = utils \
                .convert_epoch_to_isostring(leg["startTime"])
            leg["endTime"] = utils.convert_epoch_to_isostring(leg["endTime"])

            if leg["mode"] == "BUS" and "trip" in leg and leg["trip"]:
                origin_stop = leg["from"]["stop"]["gtfsId"].split(":")[-1]
                destination_stop = leg["to"]["stop"]["gtfsId"].split(":")[-1]

                # Whether we have passed the origin / destination stop or not
                stop_passed = False
                stops = []

                for stop in leg["trip"]["stops"]:
                    stop_id = stop["gtfsId"].split(":")[-1]

                    if stop_passed:
                        stops += [stop]
                        if stop_id == destination_stop:
                            stop_passed = False
                    elif stop_id == origin_stop:
                        stop_passed = True
                        stops += [stop]

                bus = None
                for stop in stops:
                    stop_id = stop["gtfsId"].split(":")[-1]

                    try:
                        eta_data = get_etas(stop_id, bus)[0]
                        eta = eta_data["eta"]

                        if eta:
                            stop["eta"] = eta

                            if not bus:
                                bus = eta_data["bus_id"]
                    except:
                        stop["eta"] = None

                    del stop["gtfsId"]

                leg["trip"]["stops"] = stops

    data["data"]["plan"]["itineraries"] = valid_itineraries
    return data


@app.get("/rt/vehicle")
def get_realtime_vehicle_positions():
    content = realtime_manager.generate_vehicle_positions()

    with open("vehicle.pb", "wb") as file:
        file.write(content)

    return FileResponse("vehicle.pb", filename="vehicle.pb", media_type="application/octet-stream")


@app.websocket("/bus/{bus_code}/ws")
async def subscribe_bus_location(websocket: WebSocket, bus_code: str) -> None:
    channel = f"bus.{bus_code}"
    await psws_manager.subscribe_to_channel(channel, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await psws_manager.disconnect_from_channel(channel, websocket)


@app.websocket("/trips/{trip_id}/ws")
async def subscribe_trip_etas(websocket: WebSocket, trip_id: str) -> None:
    mapped_trip_id = utils.map_gtfs_trip(trip_id)

    channel = f"trip.{mapped_trip_id}"
    await psws_manager.subscribe_to_channel(channel, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await psws_manager.disconnect_from_channel(channel, websocket)


async def tj_login():
    """Fetch access token if not already available or already stale"""

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


async def tj_fetch():
    """Fetch real-time GPS data periodically"""

    global token

    url = "http://esb.transjakarta.co.id/api/v2/gps/listGPSBusTripUI"
    headers = {"x-access-token": token}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception()

    data = response.json()["data"]
    return pd.DataFrame.from_dict(data)


def append_history(df):
    """Append historical data to each bus data points"""

    new_df = pd.DataFrame(columns=["bus_code", "koridor", "gpsdatetime", "latitude",
                                   "longitude", "color", "gpsheading", "gpsspeed", "is_new", "trip_id"])
    for _, row in df.iterrows():
        row["is_new"] = True
        new_df = pd.concat([new_df, row.to_frame().T], ignore_index=True)

        history_df = get_bus_history(row["bus_code"])
        if history_df.shape[0] >= 10:
            new_df = pd.concat([new_df, history_df], ignore_index=True)

    new_df.reset_index(drop=True, inplace=True)
    return new_df


def append_bus_stops(df):
    """Append previous and next stops data to each bus"""

    for bus in df["bus_code"].unique():
        gps = df[df["bus_code"] == bus]

        gps = eta_engine.data_preprocessor.preprocess_gps_data(gps)
        gps = eta_engine.determine_following_route(gps)
        gps = eta_engine.determine_trip(gps)

        # New columns: "next_stop", "prev_stop", "next_stop_seq", "prev_stop_seq"]
        gps = eta_engine.calculate_prev_next_stops(gps)

        next_stop_name = _stops.loc[
            _stops["stop_id"] == gps["next_stop"].values[0],
            "stop_name"
        ].values[0]

        prev_stop_name = _stops.loc[
            _stops["stop_id"] == gps["prev_stop"].values[0],
            "stop_name"
        ].values[0]

        df.loc[df["bus_code"] == bus, "next_stop"] = next_stop_name
        df.loc[df["bus_code"] == bus, "prev_stop"] = prev_stop_name

    return df


async def broadcast_gps(df):
    """Broadcast real-time GPS data and save history"""

    async def broadcast_to_bus_channel(row):
        channel = f"bus.{row['bus_code']}"

        redis.lpush(channel, json.dumps(row.to_dict()))
        redis.ltrim(channel, 0, 19)
        set_expired(channel)

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

    df = append_bus_stops(df)

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


async def predict_eta(df):
    """Predict ETA for each bus based on GPS data"""

    new_df = append_history(df)
    if new_df.groupby(["bus_code"]).count()["gpsdatetime"].max() < 10:
        return

    prediction = await eta_engine.predict_async(new_df)

    for bus_id, stops in prediction.items():
        if not stops:
            continue

        for stop_id, eta in stops.items():
            stop_key = f"stop.{stop_id}"
            eta_timestamp = utils.convert_seconds_to_isostring(eta)

            value = json.dumps({"eta": eta_timestamp, "bus_id": bus_id})

            redis.hset(stop_key, bus_id, value)


async def poll_api():
    try:
        df = await tj_fetch()
        df = df.drop(columns=["trip_desc"])

        df["trip_id"] = df.apply(
            lambda x: utils.map_gps_trip(x["trip_id"]), axis=1)

        # Notes: we already have a function in gtfs_manager
        df["start_time_"] = "05:00:00"
        df["start_date_"] = "20040115"
        
        realtime_manager.update_vehicle_positions(df)

        await broadcast_gps(df)
        await predict_eta(df)
    except Exception as e:
        print(e)
        await tj_login()


async def prune_trip_eta():
    now = datetime.now()
    stops = redis.keys("stop.*")

    for stop in stops:
        etas = redis.hgetall(stop)

        for bus, value in etas.items():
            eta_str = json.loads(value)["eta"]
            eta = datetime.fromisoformat(eta_str)

            if eta < now:
                redis.hdel(stop, bus)


def get_opposite_trip(route: str, trip: str):
    opposite_trips = _trips[(_trips["route_id"] == route)
                            & (_trips["trip_id"] != trip)]

    if opposite_trips.shape[0] == 0:
        return None

    return opposite_trips.iloc[0]["trip_id"]


def get_bus_history(bus_id):
    """Fetch latest bus history from redis"""

    channel = f"bus.{bus_id}"
    entries = redis.lrange(channel, 0, 19)

    history_dicts = [json.loads(entry) for entry in entries]
    if history_dicts:
        return pd.DataFrame(history_dicts)

    return pd.DataFrame()


def get_etas(stop_id, bus_id=None):
    """Fetch all ETA of a stop from redis"""

    stop_key = f"stop.{stop_id}"
    etas = []

    if bus_id:
        return [json.loads(redis.hget(stop_key, bus_id))]

    all_etas = redis.hgetall(stop_key)
    for eta_info in all_etas.values():
        etas.append(json.loads(eta_info))

    etas.sort(key=lambda x: x["eta"])
    return etas


def set_expired(key: str):
    """Set default expire at 1am the next day"""

    now = datetime.now(timezone)
    expiry = datetime(now.year, now.month, now.day, 1, 0) + timedelta(days=1)

    redis.expireat(key, expiry)


async def poll():
    print("Poll started")
    await tj_login()

    while True:
        current_time = datetime.now(timezone)

        if not (1 <= current_time.hour < 5):
            print(
                f"Poll received on {current_time.strftime('%Y/%m/%d, %H:%M:%S')}")
            await poll_api()
            await prune_trip_eta()
        else:
            print(
                f"Skipping poll during off hours: {current_time.strftime('%Y/%m/%d, %H:%M:%S')}")

        await asyncio.sleep(5)
