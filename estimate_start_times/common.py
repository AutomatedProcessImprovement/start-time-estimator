import itertools
from enum import Enum
from typing import Union

import pandas as pd
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.objects.log.obj import EventLog

from config import EventLogIDs


class EventLogType(Enum):
    EVENT_LOG = 1
    DATA_FRAME = 2


def zip_with_next(iterable):
    # s -> (s0,s1), (s1,s2), (s2, s3), ...
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def get_activities(event_log: Union[EventLog, pd.DataFrame], log_ids: EventLogIDs):
    if type(event_log) is pd.DataFrame:
        activities = event_log[log_ids.activity].unique()
    elif type(event_log) is EventLog:
        activities = attributes_filter.get_attribute_values(event_log, log_ids.activity)
    else:
        raise ValueError("Unknown event log file type! Only [PM4PY.EventLog] and [pandas.DataFrame] supported.")
    return activities
