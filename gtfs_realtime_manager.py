import time
from datetime import datetime
from uuid import uuid4

import gtfs_realtime_pb2 as gtfsrt


class GTFSRealtimeManager:
    """Class to manage GTFS realtime data and functionalities"""

    def generate_trip_updates(self, updates):
        feed_message = gtfsrt.FeedMessage()

        feed_header = feed_message.header
        feed_header.gtfs_realtime_version = "2.0"
        feed_header.incrementality = gtfsrt.FeedHeader.FULL_DATASET
        feed_header.timestamp = int(time.time())

        if updates.empty:
            return feed_message.SerializeToString()
        
        def add_entity(trip_id):
            feed_entity = feed_message.entity.add()
            feed_entity.id = str(uuid4())

            trip_update = feed_entity.trip_update

            trip_descriptor = trip_update.trip
            trip_descriptor.trip_id = trip_id
            trip_descriptor.route_id = trip_id.split("-")[0]
            trip_descriptor.start_time = "05:00:00"
            trip_descriptor.start_date = "20040115"

            def add_stop_time_update(row):
                stop_time_update = trip_update.stop_time_update.add()
                stop_time_update.stop_id = row["stop_id"]

                arrival = stop_time_update.arrival
                arrival.time = int(datetime.strptime(row["eta"], '%Y-%m-%dT%H:%M:%S.%f').timestamp())
            
            updates[updates["trips"] == trip_id].apply(add_stop_time_update, axis=1)

        trip_ids = updates["trips"].unique()
        for trip_id in trip_ids:
            add_entity(trip_id)
        
        return feed_message.SerializeToString()


    def update_vehicle_positions(self, updates):
        self.vehicle_positions = updates.copy()[["bus_code", "koridor", "trip_id", "gpsdatetime",
                                                 "latitude", "longitude", "gpsheading", "gpsspeed"]]

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
            trip_descriptor.start_time = "05:00:00"
            trip_descriptor.start_date = "20040115"

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
