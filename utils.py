from datetime import datetime, timedelta


# Map GPS trip data to GTFS trip id
def map_gps_trip(trip_id: str) -> str:
    mapper = {
        "4.B001": "4B-R01",
        "4.B011": "4B-R02",
        "9H.L03": "9H",
        "D21.003": "D21",
    }

    if trip_id in mapper:
        return mapper[trip_id]
    return None


# Map GTFS trip id to available trip data
def map_gtfs_trip(trip_id: str) -> str:
    mapper = {
        "4B-R01": "4B-R01",
        "4B-R02": "4B-R02",
        "9H-R04": "9H",
        "9H-R05": "9H",
        "D21-R01": "D21",
    }

    if trip_id in mapper:
        return mapper[trip_id]
    return None


# Convert delta time in seconds to ISO string
def convert_seconds_to_isostring(seconds: float) -> str:
    eta_duration = timedelta(seconds=seconds)
    eta_time = datetime.now() + eta_duration

    return eta_time.isoformat()


# Conver epoch in milliseconds to ISO string
def convert_epoch_to_isostring(epoch: int) -> str:
    epoch_seconds = epoch / 1000
    timestamp = datetime.utcfromtimestamp(epoch_seconds)

    return timestamp.isoformat()
