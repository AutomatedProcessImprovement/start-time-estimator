import enum
from dataclasses import dataclass
from datetime import datetime

import pytz


class ReEstimationMethod(enum.Enum):
    SET_INSTANT = 1
    MODE = 2


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
    missing_resource: str = "missing_resource"
    non_estimated_time: datetime = datetime.min.replace(tzinfo=pytz.UTC)
    re_estimation_method: ReEstimationMethod = ReEstimationMethod.MODE
