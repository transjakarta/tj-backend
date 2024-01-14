import time
from datetime import datetime
from uuid import uuid4

import gtfs_realtime_pb2 as gtfsrt
from gtfs_realtime_pb2 import FeedHeader, FeedMessage, VehiclePosition


class GTFSRealtimeManager:
    """Class to manage GTFS realtime data and functionalities"""

    def update_trip_updates(self, df):
        pass

    def update_vehicle_positions(self, df):
        self.vehicle_positions = df.copy()[["bus_code", "koridor", "trip_id", "gpsdatetime",
                                            "latitude", "longitude", "gpsheading", "gpsspeed",
                                            "start_time", "start_date"]]

    def generate_vehicle_positions(self):
        feed_message = gtfsrt.FeedMessage()

        feed_header = feed_message.header
        feed_header.gtfs_realtime_version = "2.0"
        feed_header.incrementality = gtfsrt.FeedHeader.FULL_DATASET
        feed_header.timestamp = int(time.time())

        if self.vehicle_positions.empty:
            return feed_message.SerializeToString()

        def add_entity(row):
            feed_entity = feed_message.entity.add()
            feed_entity.id = str(uuid4())

            vehicle_position = feed_entity.vehicle

            trip_descriptor = vehicle_position.trip
            trip_descriptor.route_id = row["koridor"]
            trip_descriptor.trip_id = row["trip_id"]
            trip_descriptor.start_time = row["start_time"]
            trip_descriptor.start_date = row["start_date"]

            vehicle_descriptor = vehicle_position.vehicle
            vehicle_descriptor.id = row["bus_code"]
            vehicle_descriptor.label = row["bus_code"]

            position = vehicle_position.position
            position.latitude = row["latitude"]
            position.longitude = row["longitude"]
            position.bearing = row["gpsheading"]
            position.speed = row["gpsspeed"]

            vehicle_position.timestamp = int(
                datetime.fromisoformat(row["gpsdatetime"]).timestamp())

        self.vehicle_positions.apply(add_entity, axis=1)

        return feed_message.SerializeToString()
