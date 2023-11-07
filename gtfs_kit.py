import pandas as pd
import os


class Feed:
    def __init__(self) -> None:
        self.routes = None
        self.stop_times = None
        self.stops = None
        self.trips = None

    def read_feed(self, dir: str) -> None:
        self.routes = pd.read_csv(os.path.join(dir, "routes.txt"))
        self.stop_times = pd.read_csv(os.path.join(dir, "stop_times.txt"))
        self.stops = pd.read_csv(os.path.join(dir, "stops.txt"))
        self.trips = pd.read_csv(os.path.join(dir, "trips.txt"))


def read_feed(dir: str) -> Feed:
    feed = Feed()
    feed.read_feed(dir)
    return feed
