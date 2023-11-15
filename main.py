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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup events
    redis.ping()
    asyncio.create_task(heartbeat())
    yield

    # Shutdown events
    await psws_manager.close_subscribers()


app = FastAPI(lifespan=lifespan)


# Global variables
token = ""


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
                            departureTime
                            arrivalTime
                        }}
                        to {{
                            name
                            lat
                            lon
                            departureTime
                            arrivalTime
                        }}
                        route {{
                            gtfsId
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
    return response.json()


@app.websocket("/bus/{bus_code}/ws")
async def websocket_bus_gps(websocket: WebSocket, bus_code: str) -> None:
    channel = f"bus.{bus_code}"
    await psws_manager.subscribe_to_channel(channel, websocket)
    try:
        while True:
            await websocket.receive_text()  # wait for client to disconnect
    except WebSocketDisconnect:
        await psws_manager.disconnect_from_channel(channel, websocket)


@app.get("/bus/gps")
async def get_bus_gps() -> None:
    # fetch bus gps data

    # preprocess data

    # predict eta
    predicted_eta = {
        "MYS23375": None,
        "TJ0843": {
            "B02017P": 101.06355667114258,
            "B03078P": 137.54007148742676,
            "B02470P": 170.0546760559082,
            "B00693P": 193.9820556640625,
            "B02160P": 236.48881912231445,
            "B02647P": 288.22187995910645,
            "B05716P": 323.5605945587158,
            "B02278P": 374.0245876312256,
            "B01581P": 414.91749000549316,
            "B00185P": 441.18727111816406,
            "B02892P": 459.1596784591675,
            "B00231P": 482.5269184112549,
            "B00731P": 508.31426429748535,
            "B00337P": 528.875394821167,
            "B02921P": 547.914776802063,
            "B02300P": 581.7295656204224,
            "B02982P": 636.2539739608765,
            "B00683P": 668.446683883667,
            "B03183P": 686.9114837646484,
            "B02393P": 728.6144218444824,
            "B02562P": 781.4506721496582,
            "B05686P": 816.512882232666,
            "B05836P": 859.780029296875,
            "B02045P": 887.4479713439941,
            "B02426P": 916.8913145065308,
            "B01889P": 938.9977750778198,
            "B04573P": 979.9901838302612,
            "B00105P": 1013.0730657577515,
            "B02553P": 1048.428708076477,
            "B01753P": 1083.4766302108765,
            "B03334P": 1115.6089544296265,
            "B03126P": 1136.7423858642578,
            "B02342P": 1171.3638591766357,
            "B02456P": 1214.3314151763916,
            "P00257": 1386.4140338897705,
            "G00639": 1581.9419269561768,
            "B00799P": 1666.6671772003174,
            "B03304P": 1707.9064903259277,
            "B01567P": 1743.8742218017578,
            "B00025P": 1792.2513332366943,
            "B01550P": 1828.1307964324951,
            "P00275": 1858.7412204742432,
            "B05398P": 1893.7748336791992,
            "B05685P": 1931.4798984527588,
            "B05803P": 1973.134744644165,
            "B01888P": 2014.5241661071777,
            "B02658P": 2050.5989627838135,
            "B05835P": 2080.863712310791,
            "B05649P": 2110.1886463165283,
            "B02718P": 2146.0982551574707,
            "B00667P": 2184.9868717193604,
            "B03311P": 2233.029453277588,
            "B02559P": 2255.597025871277,
            "B02736P": 2292.7015409469604,
            "B01449P": 2313.782431602478,
            "B00938P": 2339.1432905197144,
            "B02762P": 2364.7099809646606,
            "B05296P": 2397.207622528076,
            "B02918P": 2446.858034133911,
            "B02532P": 2465.616506576538,
            "B03616P": 2488.911533355713,
            "B00907P": 2517.5468940734863,
            "B05931P": 2542.021020889282,
            "B05874P": 2563.180486679077,
            "B02277P": 2585.108917236328,
            "B05494P": 2640.1093559265137,
            "B02915P": 2774.1077156066895,
        },
        "TJ0516": {
            "B05830P": 159.49011993408203,
            "B05834P": 334.0459289550781,
            "B05514P": 399.92786407470703,
            "B04423P": 503.0145263671875,
            "B00821P": 590.5544357299805,
            "B05464P": 626.2329597473145,
            "B01809P": 814.1040153503418,
            "B01780P": 954.1394996643066,
            "B02269P": 1055.5098915100098,
            "B02134P": 1210.2096366882324,
            "B05509P": 1258.8244018554688,
            "B02017P": 1468.2365036010742,
            "B03078P": 1571.0209579467773,
            "B02470P": 1665.932487487793,
            "B00693P": 1721.6584777832031,
            "B02160P": 1841.640968322754,
            "B02647P": 1922.9099502563477,
            "B05716P": 1986.8824996948242,
            "B02278P": 2141.8582305908203,
            "B01581P": 2252.936424255371,
            "B00185P": 2317.9829864501953,
            "B02892P": 2364.1304626464844,
            "B00231P": 2412.400520324707,
            "B00731P": 2484.2641983032227,
            "B00337P": 2556.1895294189453,
            "B02921P": 2604.979202270508,
            "B02300P": 2707.1710815429688,
            "B02982P": 2811.044536590576,
            "B00683P": 2895.588840484619,
            "B03183P": 2954.9867515563965,
            "B02393P": 3074.306968688965,
            "B02562P": 3204.5052642822266,
            "B05686P": 3268.2637367248535,
            "B05836P": 3392.4420051574707,
            "B02045P": 3450.930896759033,
            "B02426P": 3515.213405609131,
            "B01889P": 3556.7533531188965,
            "B04573P": 3641.8804969787598,
            "B00105P": 3719.7786445617676,
            "B02553P": 3794.456325531006,
            "B01753P": 3902.439556121826,
            "B03334P": 3958.346076965332,
            "B03126P": 3993.139793395996,
            "B02342P": 4072.2980422973633,
            "B02456P": 4160.561683654785,
            "P00257": 4569.319557189941,
            "G00639": 4922.3090896606445,
            "B00799P": 5179.1960372924805,
            "B03304P": 5248.251708984375,
            "B01567P": 5311.629821777344,
            "B00025P": 5426.066589355469,
            "B01550P": 5483.4630699157715,
            "P00275": 5545.9435386657715,
            "B05398P": 5604.5657958984375,
            "B05685P": 5686.842758178711,
            "B05803P": 5776.651832580566,
            "B01888P": 5872.400657653809,
            "B02658P": 5953.749977111816,
            "B05835P": 6015.725605010986,
            "B05649P": 6076.37121963501,
            "B02718P": 6161.527156829834,
            "B00667P": 6235.701961517334,
            "B03311P": 6323.397441864014,
            "B02559P": 6367.6547927856445,
            "B02736P": 6457.354400634766,
            "B01449P": 6528.860000610352,
            "B00938P": 6603.879737854004,
            "B02762P": 6663.041961669922,
            "B05296P": 6730.44197845459,
            "B02918P": 6830.321422576904,
            "B02532P": 6862.480911254883,
            "B03616P": 6937.688400268555,
            "B00907P": 7032.581619262695,
            "B05931P": 7098.364601135254,
            "B05874P": 7163.542503356934,
            "B02277P": 7258.633583068848,
            "B05494P": 7428.571350097656,
            "B02915P": 7701.520751953125,
            "B00374P": 7941.789039611816,
            "B05508P": 8116.368896484375,
            "B05680P": 8167.056465148926,
            "B00726P": 8206.249969482422,
            "B02916P": 8280.140983581543,
            "B00491P": 8409.386276245117,
            "B05408P": 8453.696102142334,
            "B02133P": 8564.729206085205,
            "B05612P": 8597.830978393555,
            "B05463P": 8780.38990020752,
            "B05881P": 8831.772388458252,
            "B05266P": 8920.188343048096,
            "B05259P": 9104.749172210693,
        },
    }

    for bus_code, eta in predicted_eta.items():
        channel = f"bus.{bus_code}"

        # dummy data
        data = {
            "bus_code": bus_code,
            "koridor": "4B",
            "gpsdatetime": "19/09/2023 05:26:05",
            "latitude": -6.312733,
            "longitude": 106.883828,
            "color": "PUTIH ORG",
            "gpsheading": 262,
            "gpsspeed": 3.7,
            "eta": eta,
        }

        # broadcast to channel
        await psws_manager.broadcast_to_channel(channel, json.dumps(data))

    return None


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
    df = pd.DataFrame.from_dict(data) \
        .drop(columns=["color", "trip_desc"]) \
        .rename(columns={
            "bus_code": "id",
            "koridor": "route_id",
            "gpsdatetime": "timestamp",
            "latitude": "lat",
            "longitude": "lon",
            "gpsheading": "head",
            "gpsspeed": "speed"
        })

    print(df.head())
    return df


# Broadcast real-time GPS data
async def broadcast_gps(df):
    for _, row in df.iterrows():
        channel = f"bus.{row['bus_code']}"
        await psws_manager.broadcast_to_channel(channel, json.dumps(row))


async def poll_api():
    try:
        df = await tj_fetch()
        await broadcast_gps(df)
    except:
        await tj_login()


def get_opposite_trip(route: str, trip: str):
    opposite_trips = _trips[(_trips["route_id"] == route)
                            & (_trips["trip_id"] != trip)]

    if opposite_trips.shape[0] == 0:
        return None

    return opposite_trips.iloc[0]["trip_id"]


async def heartbeat():
    print("Heartbeat started")
    await tj_login()

    while True:
        print(
            f"Heartbeat received on {datetime.now().strftime('%Y/%m/%d, %H:%M:%S')}")
        await poll_api()
        await asyncio.sleep(5)
