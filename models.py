from pydantic import BaseModel
from datetime import datetime


class Place(BaseModel):
    id: str
    name: str | None = None
    address: str | None = None
    is_stop: bool = False


class PlaceDetails(Place):
    distance: float | None = None
    lat: float | None = None
    lon: float | None = None
    walking_distance: float | None = None
    walking_duration: float | None = None
    routes: list[str] | None = None


class Stop(Place):
    is_stop: bool | None = None
    order: int | None = None
    lat: float | None = None
    lon: float | None = None


class StopEta(Stop):
    eta: float | None = None


class Trip(BaseModel):
    id: str
    direction: int
    origin: str
    destination: str
    num_stops: int | None = None
    distance: float | None = None


class TripRoute(Trip):
    opposite_id: str | None = None
    route: str
    color: str
    text_color: str


class TripStops(Trip):
    stops: list[Stop] | None = None


class TripRouteStops(TripRoute):
    stops: list[Stop] | None = None


class Route(BaseModel):
    id: str
    color: str
    text_color: str


class RouteTrips(Route):
    trips: list[Trip] | None = None


class RouteTripsStops(Route):
    trips: list[TripStops] | None = None


class GetPlacesByIdBody(BaseModel):
    list_of_ids: list[Place]
    lat: float | None = None
    lon: float | None = None
    language_code: str = "id"


class Endpoints(BaseModel):
    origin_lat: float
    origin_lon: float
    destination_lat: float
    destination_lon: float
