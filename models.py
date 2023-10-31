from pydantic import BaseModel
from datetime import datetime


class Stop(BaseModel):
    id: str
    order: int
    name: str
    lat: float
    lon: float


class StopEta(Stop):
    eta: datetime = None


class Trip(BaseModel):
    id: str
    direction: int
    num_stops: int
    distance: float
    origin: str
    destination: str


class Route(BaseModel):
    id: str
    color: str
    text_color: str
    trips: list[Trip]
