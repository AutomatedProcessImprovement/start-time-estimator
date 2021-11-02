import enum
import itertools


class FixMethod(enum.Enum):
    SET_INSTANT = 1
    RE_ESTIMATE = 2


def zip_with_next(iterable):
    # s -> (s0,s1), (s1,s2), (s2, s3), ...
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)
