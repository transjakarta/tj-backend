import pandas as pd
import gtfs_kit as gk
import gtfs_kit.helpers as gh
import gtfs_kit.constants as gc

from json import loads
from collections import defaultdict

from fastapi import FastAPI

app = FastAPI()

feed = gk.read_feed("./data/gtfs.zip", dist_units='km')
route_ids = ["4B", "D21", "9H"]


@app.get("/routes")
async def get_routes():
    filtered_routes = feed.routes[feed.routes.route_id.isin(
        route_ids)][["route_id", "route_color", "route_text_color"]]

    filtered_trips = feed.trips[feed.trips["route_id"].isin(
        route_ids)][["route_id", "trip_id", "trip_headsign", "direction_id"]]

    trip_stats = gk.compute_trip_stats(feed, route_ids=route_ids)[
        ["trip_id", "num_stops", "distance"]]

    merged = pd.merge(filtered_trips, trip_stats, on="trip_id")
    merged = pd.merge(merged, filtered_routes, on="route_id")

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
        {"route": route, "color": color, "text_color": text_color, "trips": trips}
        for (route, color, text_color), trips in ddict.items()]

    return json
