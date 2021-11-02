import itertools
from enum import Enum


class EventLogType(Enum):
    EVENT_LOG = 1
    DATA_FRAME = 2


def zip_with_next(iterable):
    # s -> (s0,s1), (s1,s2), (s2, s3), ...
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)
