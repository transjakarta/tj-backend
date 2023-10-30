import pandas as pd
import gtfs_kit as gk
import gtfs_kit.helpers as gh
import gtfs_kit.constants as gc

from json import loads
from fastapi import FastAPI

app = FastAPI()

feed = gk.read_feed("./data/gtfs.zip", dist_units='km')
route_ids = ["4B", "D21", "9H"]


@app.get("/routes")
async def get_routes():
    filtered_routes = feed.routes[feed.routes.route_id.isin(route_ids)]
    filtered_trips = feed.trips[(feed.trips["direction_id"] == 0) & (
        feed.trips["route_id"].isin(route_ids))]

    merged = pd.merge(filtered_routes, filtered_trips, on="route_id")
    filtered = merged[["route_id", "trip_headsign", "route_color", "route_text_color"]].rename(
        columns={"route_id": "id", "trip_headsign": "name", "route_color": "color", "route_text_color": "text_color"})

    json = filtered.to_json(orient="records")
    return loads(json)
