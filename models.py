from pydantic import BaseModel
from datetime import datetime


class Stop(BaseModel):
    id: str
    name: str | None = None
    order: int | None = None
    lat: float | None = None
    lon: float | None = None


class StopEta(Stop):
    eta: datetime | None = None

class StopDetails(Stop):
    walking_distance: float
    walking_duration: float
    routes: list[str] | None = None


class Trip(BaseModel):
    id: str
    direction: int
    origin: str
    destination: str
    num_stops: int | None = None
    distance: float | None = None


class TripStops(Trip):
    stops: list[Stop] | None = None


class Route(BaseModel):
    id: str
    color: str
    text_color: str


class RouteTrips(Route):
    trips: list[Trip] | None = None


class RouteTripsStops(Route):
    trips: list[TripStops] | None = None


class Place(BaseModel):
    id: str
    name: str
    is_stop: bool = False
    distance: float | None = None
    routes: list[str] | None = None
