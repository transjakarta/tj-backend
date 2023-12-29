from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf.internal import python_message as _python_message
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FeedMessage(_message.Message):
    __slots__ = ("header", "entity")
    Extensions: _python_message._ExtensionDict
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    header: FeedHeader
    entity: _containers.RepeatedCompositeFieldContainer[FeedEntity]
    def __init__(self, header: _Optional[_Union[FeedHeader, _Mapping]] = ..., entity: _Optional[_Iterable[_Union[FeedEntity, _Mapping]]] = ...) -> None: ...

class FeedHeader(_message.Message):
    __slots__ = ("gtfs_realtime_version", "incrementality", "timestamp")
    Extensions: _python_message._ExtensionDict
    class Incrementality(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        FULL_DATASET: _ClassVar[FeedHeader.Incrementality]
        DIFFERENTIAL: _ClassVar[FeedHeader.Incrementality]
    FULL_DATASET: FeedHeader.Incrementality
    DIFFERENTIAL: FeedHeader.Incrementality
    GTFS_REALTIME_VERSION_FIELD_NUMBER: _ClassVar[int]
    INCREMENTALITY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    gtfs_realtime_version: str
    incrementality: FeedHeader.Incrementality
    timestamp: int
    def __init__(self, gtfs_realtime_version: _Optional[str] = ..., incrementality: _Optional[_Union[FeedHeader.Incrementality, str]] = ..., timestamp: _Optional[int] = ...) -> None: ...

class FeedEntity(_message.Message):
    __slots__ = ("id", "is_deleted", "trip_update", "vehicle", "alert")
    Extensions: _python_message._ExtensionDict
    ID_FIELD_NUMBER: _ClassVar[int]
    IS_DELETED_FIELD_NUMBER: _ClassVar[int]
    TRIP_UPDATE_FIELD_NUMBER: _ClassVar[int]
    VEHICLE_FIELD_NUMBER: _ClassVar[int]
    ALERT_FIELD_NUMBER: _ClassVar[int]
    id: str
    is_deleted: bool
    trip_update: TripUpdate
    vehicle: VehiclePosition
    alert: Alert
    def __init__(self, id: _Optional[str] = ..., is_deleted: bool = ..., trip_update: _Optional[_Union[TripUpdate, _Mapping]] = ..., vehicle: _Optional[_Union[VehiclePosition, _Mapping]] = ..., alert: _Optional[_Union[Alert, _Mapping]] = ...) -> None: ...

class TripUpdate(_message.Message):
    __slots__ = ("trip", "vehicle", "stop_time_update", "timestamp", "delay")
    Extensions: _python_message._ExtensionDict
    class StopTimeEvent(_message.Message):
        __slots__ = ("delay", "time", "uncertainty")
        Extensions: _python_message._ExtensionDict
        DELAY_FIELD_NUMBER: _ClassVar[int]
        TIME_FIELD_NUMBER: _ClassVar[int]
        UNCERTAINTY_FIELD_NUMBER: _ClassVar[int]
        delay: int
        time: int
        uncertainty: int
        def __init__(self, delay: _Optional[int] = ..., time: _Optional[int] = ..., uncertainty: _Optional[int] = ...) -> None: ...
    class StopTimeUpdate(_message.Message):
        __slots__ = ("stop_sequence", "stop_id", "arrival", "departure", "schedule_relationship")
        Extensions: _python_message._ExtensionDict
        class ScheduleRelationship(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            SCHEDULED: _ClassVar[TripUpdate.StopTimeUpdate.ScheduleRelationship]
            SKIPPED: _ClassVar[TripUpdate.StopTimeUpdate.ScheduleRelationship]
            NO_DATA: _ClassVar[TripUpdate.StopTimeUpdate.ScheduleRelationship]
        SCHEDULED: TripUpdate.StopTimeUpdate.ScheduleRelationship
        SKIPPED: TripUpdate.StopTimeUpdate.ScheduleRelationship
        NO_DATA: TripUpdate.StopTimeUpdate.ScheduleRelationship
        STOP_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
        STOP_ID_FIELD_NUMBER: _ClassVar[int]
        ARRIVAL_FIELD_NUMBER: _ClassVar[int]
        DEPARTURE_FIELD_NUMBER: _ClassVar[int]
        SCHEDULE_RELATIONSHIP_FIELD_NUMBER: _ClassVar[int]
        stop_sequence: int
        stop_id: str
        arrival: TripUpdate.StopTimeEvent
        departure: TripUpdate.StopTimeEvent
        schedule_relationship: TripUpdate.StopTimeUpdate.ScheduleRelationship
        def __init__(self, stop_sequence: _Optional[int] = ..., stop_id: _Optional[str] = ..., arrival: _Optional[_Union[TripUpdate.StopTimeEvent, _Mapping]] = ..., departure: _Optional[_Union[TripUpdate.StopTimeEvent, _Mapping]] = ..., schedule_relationship: _Optional[_Union[TripUpdate.StopTimeUpdate.ScheduleRelationship, str]] = ...) -> None: ...
    TRIP_FIELD_NUMBER: _ClassVar[int]
    VEHICLE_FIELD_NUMBER: _ClassVar[int]
    STOP_TIME_UPDATE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DELAY_FIELD_NUMBER: _ClassVar[int]
    trip: TripDescriptor
    vehicle: VehicleDescriptor
    stop_time_update: _containers.RepeatedCompositeFieldContainer[TripUpdate.StopTimeUpdate]
    timestamp: int
    delay: int
    def __init__(self, trip: _Optional[_Union[TripDescriptor, _Mapping]] = ..., vehicle: _Optional[_Union[VehicleDescriptor, _Mapping]] = ..., stop_time_update: _Optional[_Iterable[_Union[TripUpdate.StopTimeUpdate, _Mapping]]] = ..., timestamp: _Optional[int] = ..., delay: _Optional[int] = ...) -> None: ...

class VehiclePosition(_message.Message):
    __slots__ = ("trip", "vehicle", "position", "current_stop_sequence", "stop_id", "current_status", "timestamp", "congestion_level", "occupancy_status")
    Extensions: _python_message._ExtensionDict
    class VehicleStopStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        INCOMING_AT: _ClassVar[VehiclePosition.VehicleStopStatus]
        STOPPED_AT: _ClassVar[VehiclePosition.VehicleStopStatus]
        IN_TRANSIT_TO: _ClassVar[VehiclePosition.VehicleStopStatus]
    INCOMING_AT: VehiclePosition.VehicleStopStatus
    STOPPED_AT: VehiclePosition.VehicleStopStatus
    IN_TRANSIT_TO: VehiclePosition.VehicleStopStatus
    class CongestionLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN_CONGESTION_LEVEL: _ClassVar[VehiclePosition.CongestionLevel]
        RUNNING_SMOOTHLY: _ClassVar[VehiclePosition.CongestionLevel]
        STOP_AND_GO: _ClassVar[VehiclePosition.CongestionLevel]
        CONGESTION: _ClassVar[VehiclePosition.CongestionLevel]
        SEVERE_CONGESTION: _ClassVar[VehiclePosition.CongestionLevel]
    UNKNOWN_CONGESTION_LEVEL: VehiclePosition.CongestionLevel
    RUNNING_SMOOTHLY: VehiclePosition.CongestionLevel
    STOP_AND_GO: VehiclePosition.CongestionLevel
    CONGESTION: VehiclePosition.CongestionLevel
    SEVERE_CONGESTION: VehiclePosition.CongestionLevel
    class OccupancyStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        EMPTY: _ClassVar[VehiclePosition.OccupancyStatus]
        MANY_SEATS_AVAILABLE: _ClassVar[VehiclePosition.OccupancyStatus]
        FEW_SEATS_AVAILABLE: _ClassVar[VehiclePosition.OccupancyStatus]
        STANDING_ROOM_ONLY: _ClassVar[VehiclePosition.OccupancyStatus]
        CRUSHED_STANDING_ROOM_ONLY: _ClassVar[VehiclePosition.OccupancyStatus]
        FULL: _ClassVar[VehiclePosition.OccupancyStatus]
        NOT_ACCEPTING_PASSENGERS: _ClassVar[VehiclePosition.OccupancyStatus]
    EMPTY: VehiclePosition.OccupancyStatus
    MANY_SEATS_AVAILABLE: VehiclePosition.OccupancyStatus
    FEW_SEATS_AVAILABLE: VehiclePosition.OccupancyStatus
    STANDING_ROOM_ONLY: VehiclePosition.OccupancyStatus
    CRUSHED_STANDING_ROOM_ONLY: VehiclePosition.OccupancyStatus
    FULL: VehiclePosition.OccupancyStatus
    NOT_ACCEPTING_PASSENGERS: VehiclePosition.OccupancyStatus
    TRIP_FIELD_NUMBER: _ClassVar[int]
    VEHICLE_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    CURRENT_STOP_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    STOP_ID_FIELD_NUMBER: _ClassVar[int]
    CURRENT_STATUS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CONGESTION_LEVEL_FIELD_NUMBER: _ClassVar[int]
    OCCUPANCY_STATUS_FIELD_NUMBER: _ClassVar[int]
    trip: TripDescriptor
    vehicle: VehicleDescriptor
    position: Position
    current_stop_sequence: int
    stop_id: str
    current_status: VehiclePosition.VehicleStopStatus
    timestamp: int
    congestion_level: VehiclePosition.CongestionLevel
    occupancy_status: VehiclePosition.OccupancyStatus
    def __init__(self, trip: _Optional[_Union[TripDescriptor, _Mapping]] = ..., vehicle: _Optional[_Union[VehicleDescriptor, _Mapping]] = ..., position: _Optional[_Union[Position, _Mapping]] = ..., current_stop_sequence: _Optional[int] = ..., stop_id: _Optional[str] = ..., current_status: _Optional[_Union[VehiclePosition.VehicleStopStatus, str]] = ..., timestamp: _Optional[int] = ..., congestion_level: _Optional[_Union[VehiclePosition.CongestionLevel, str]] = ..., occupancy_status: _Optional[_Union[VehiclePosition.OccupancyStatus, str]] = ...) -> None: ...

class Alert(_message.Message):
    __slots__ = ("active_period", "informed_entity", "cause", "effect", "url", "header_text", "description_text")
    Extensions: _python_message._ExtensionDict
    class Cause(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN_CAUSE: _ClassVar[Alert.Cause]
        OTHER_CAUSE: _ClassVar[Alert.Cause]
        TECHNICAL_PROBLEM: _ClassVar[Alert.Cause]
        STRIKE: _ClassVar[Alert.Cause]
        DEMONSTRATION: _ClassVar[Alert.Cause]
        ACCIDENT: _ClassVar[Alert.Cause]
        HOLIDAY: _ClassVar[Alert.Cause]
        WEATHER: _ClassVar[Alert.Cause]
        MAINTENANCE: _ClassVar[Alert.Cause]
        CONSTRUCTION: _ClassVar[Alert.Cause]
        POLICE_ACTIVITY: _ClassVar[Alert.Cause]
        MEDICAL_EMERGENCY: _ClassVar[Alert.Cause]
    UNKNOWN_CAUSE: Alert.Cause
    OTHER_CAUSE: Alert.Cause
    TECHNICAL_PROBLEM: Alert.Cause
    STRIKE: Alert.Cause
    DEMONSTRATION: Alert.Cause
    ACCIDENT: Alert.Cause
    HOLIDAY: Alert.Cause
    WEATHER: Alert.Cause
    MAINTENANCE: Alert.Cause
    CONSTRUCTION: Alert.Cause
    POLICE_ACTIVITY: Alert.Cause
    MEDICAL_EMERGENCY: Alert.Cause
    class Effect(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NO_SERVICE: _ClassVar[Alert.Effect]
        REDUCED_SERVICE: _ClassVar[Alert.Effect]
        SIGNIFICANT_DELAYS: _ClassVar[Alert.Effect]
        DETOUR: _ClassVar[Alert.Effect]
        ADDITIONAL_SERVICE: _ClassVar[Alert.Effect]
        MODIFIED_SERVICE: _ClassVar[Alert.Effect]
        OTHER_EFFECT: _ClassVar[Alert.Effect]
        UNKNOWN_EFFECT: _ClassVar[Alert.Effect]
        STOP_MOVED: _ClassVar[Alert.Effect]
    NO_SERVICE: Alert.Effect
    REDUCED_SERVICE: Alert.Effect
    SIGNIFICANT_DELAYS: Alert.Effect
    DETOUR: Alert.Effect
    ADDITIONAL_SERVICE: Alert.Effect
    MODIFIED_SERVICE: Alert.Effect
    OTHER_EFFECT: Alert.Effect
    UNKNOWN_EFFECT: Alert.Effect
    STOP_MOVED: Alert.Effect
    ACTIVE_PERIOD_FIELD_NUMBER: _ClassVar[int]
    INFORMED_ENTITY_FIELD_NUMBER: _ClassVar[int]
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    HEADER_TEXT_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_TEXT_FIELD_NUMBER: _ClassVar[int]
    active_period: _containers.RepeatedCompositeFieldContainer[TimeRange]
    informed_entity: _containers.RepeatedCompositeFieldContainer[EntitySelector]
    cause: Alert.Cause
    effect: Alert.Effect
    url: TranslatedString
    header_text: TranslatedString
    description_text: TranslatedString
    def __init__(self, active_period: _Optional[_Iterable[_Union[TimeRange, _Mapping]]] = ..., informed_entity: _Optional[_Iterable[_Union[EntitySelector, _Mapping]]] = ..., cause: _Optional[_Union[Alert.Cause, str]] = ..., effect: _Optional[_Union[Alert.Effect, str]] = ..., url: _Optional[_Union[TranslatedString, _Mapping]] = ..., header_text: _Optional[_Union[TranslatedString, _Mapping]] = ..., description_text: _Optional[_Union[TranslatedString, _Mapping]] = ...) -> None: ...

class TimeRange(_message.Message):
    __slots__ = ("start", "end")
    Extensions: _python_message._ExtensionDict
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    start: int
    end: int
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class Position(_message.Message):
    __slots__ = ("latitude", "longitude", "bearing", "odometer", "speed")
    Extensions: _python_message._ExtensionDict
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    BEARING_FIELD_NUMBER: _ClassVar[int]
    ODOMETER_FIELD_NUMBER: _ClassVar[int]
    SPEED_FIELD_NUMBER: _ClassVar[int]
    latitude: float
    longitude: float
    bearing: float
    odometer: float
    speed: float
    def __init__(self, latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., bearing: _Optional[float] = ..., odometer: _Optional[float] = ..., speed: _Optional[float] = ...) -> None: ...

class TripDescriptor(_message.Message):
    __slots__ = ("trip_id", "route_id", "direction_id", "start_time", "start_date", "schedule_relationship")
    Extensions: _python_message._ExtensionDict
    class ScheduleRelationship(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SCHEDULED: _ClassVar[TripDescriptor.ScheduleRelationship]
        ADDED: _ClassVar[TripDescriptor.ScheduleRelationship]
        UNSCHEDULED: _ClassVar[TripDescriptor.ScheduleRelationship]
        CANCELED: _ClassVar[TripDescriptor.ScheduleRelationship]
    SCHEDULED: TripDescriptor.ScheduleRelationship
    ADDED: TripDescriptor.ScheduleRelationship
    UNSCHEDULED: TripDescriptor.ScheduleRelationship
    CANCELED: TripDescriptor.ScheduleRelationship
    TRIP_ID_FIELD_NUMBER: _ClassVar[int]
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_ID_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_RELATIONSHIP_FIELD_NUMBER: _ClassVar[int]
    trip_id: str
    route_id: str
    direction_id: int
    start_time: str
    start_date: str
    schedule_relationship: TripDescriptor.ScheduleRelationship
    def __init__(self, trip_id: _Optional[str] = ..., route_id: _Optional[str] = ..., direction_id: _Optional[int] = ..., start_time: _Optional[str] = ..., start_date: _Optional[str] = ..., schedule_relationship: _Optional[_Union[TripDescriptor.ScheduleRelationship, str]] = ...) -> None: ...

class VehicleDescriptor(_message.Message):
    __slots__ = ("id", "label", "license_plate")
    Extensions: _python_message._ExtensionDict
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    LICENSE_PLATE_FIELD_NUMBER: _ClassVar[int]
    id: str
    label: str
    license_plate: str
    def __init__(self, id: _Optional[str] = ..., label: _Optional[str] = ..., license_plate: _Optional[str] = ...) -> None: ...

class EntitySelector(_message.Message):
    __slots__ = ("agency_id", "route_id", "route_type", "trip", "stop_id")
    Extensions: _python_message._ExtensionDict
    AGENCY_ID_FIELD_NUMBER: _ClassVar[int]
    ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    ROUTE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TRIP_FIELD_NUMBER: _ClassVar[int]
    STOP_ID_FIELD_NUMBER: _ClassVar[int]
    agency_id: str
    route_id: str
    route_type: int
    trip: TripDescriptor
    stop_id: str
    def __init__(self, agency_id: _Optional[str] = ..., route_id: _Optional[str] = ..., route_type: _Optional[int] = ..., trip: _Optional[_Union[TripDescriptor, _Mapping]] = ..., stop_id: _Optional[str] = ...) -> None: ...

class TranslatedString(_message.Message):
    __slots__ = ("translation",)
    Extensions: _python_message._ExtensionDict
    class Translation(_message.Message):
        __slots__ = ("text", "language")
        Extensions: _python_message._ExtensionDict
        TEXT_FIELD_NUMBER: _ClassVar[int]
        LANGUAGE_FIELD_NUMBER: _ClassVar[int]
        text: str
        language: str
        def __init__(self, text: _Optional[str] = ..., language: _Optional[str] = ...) -> None: ...
    TRANSLATION_FIELD_NUMBER: _ClassVar[int]
    translation: _containers.RepeatedCompositeFieldContainer[TranslatedString.Translation]
    def __init__(self, translation: _Optional[_Iterable[_Union[TranslatedString.Translation, _Mapping]]] = ...) -> None: ...
