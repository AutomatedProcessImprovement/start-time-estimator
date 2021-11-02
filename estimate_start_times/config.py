import enum
from dataclasses import dataclass
from datetime import datetime

import pytz


class ReEstimationMethod(enum.Enum):
    SET_INSTANT = 1
    MODE = 2


class ConcurrencyOracleType(enum.Enum):
    NONE = 1
    ALPHA = 2


class ResourceAvailabilityType(enum.Enum):
    SIMPLE = 1  # Consider all the events that each resource performs
    WITH_CALENDAR = 2  # Future possibility considering also the resource calendars and non-working days


@dataclass
class EventLogIDs:
    case: str = 'case'
    activity: str = 'activity'
    start_timestamp: str = 'start_timestamp'
    end_timestamp: str = 'end_timestamp'
    resource: str = 'resource'


DEFAULT_CSV_IDS = EventLogIDs()
DEFAULT_XES_IDS = EventLogIDs(case='concept:name',
                              activity='concept:name',
                              start_timestamp='start:timestamp',
                              end_timestamp='time:timestamp',
                              resource='org:resource')


@dataclass
class Configuration:
    log_ids: EventLogIDs = DEFAULT_CSV_IDS
    concurrency_oracle_type: ConcurrencyOracleType = ConcurrencyOracleType.ALPHA
    resource_availability_type: ResourceAvailabilityType = ResourceAvailabilityType.SIMPLE
    missing_resource: str = "missing_resource"
    non_estimated_time: datetime = datetime.min.replace(tzinfo=pytz.UTC)
    re_estimation_method: ReEstimationMethod = ReEstimationMethod.MODE