from datetime import datetime
from datetime import timedelta

from config import Configuration, DEFAULT_XES_IDS
from event_log.concurrency_oracle import NoConcurrencyOracle
from event_log.estimate_start_times import FixMethod
from event_log.estimate_start_times import estimate_start_timestamps
from event_log.estimate_start_times import re_estimate_non_estimated_start_times
from event_log.estimate_start_times import set_instant_non_estimated_start_times
from event_log.resource_availability import ResourceAvailability
from event_log_readers import read_xes_log


def test_estimate_start_times_instant():
    config = Configuration(log_ids=DEFAULT_XES_IDS,
                           fix_method=FixMethod.SET_INSTANT)
    event_log = read_xes_log('../assets/test_event_log_1.xes', config)
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
    resource_availability = ResourceAvailability(event_log, config)
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle, resource_availability, config)
    # The start time of initial events is the end time (instant events)
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == extended_event_log[0][0][config.log_ids.end_timestamp]
    assert extended_event_log[3][0][config.log_ids.start_timestamp] == extended_event_log[3][0][config.log_ids.end_timestamp]
    # The start time of an event with its resource free but immediately
    # following its previous one is the end time of the previous one.
    assert extended_event_log[1][3][config.log_ids.start_timestamp] == event_log[1][2][config.log_ids.end_timestamp]
    assert extended_event_log[2][3][config.log_ids.start_timestamp] == event_log[2][2][config.log_ids.end_timestamp]
    # The start time of an event enabled for a long time but with its resource
    # busy in other activities is the end time of its resource's last activity.
    assert extended_event_log[3][3][config.log_ids.start_timestamp] == event_log[2][2][config.log_ids.end_timestamp]
    assert extended_event_log[3][4][config.log_ids.start_timestamp] == event_log[1][4][config.log_ids.end_timestamp]


def test_estimate_start_times_re_estimate():
    config = Configuration(log_ids=DEFAULT_XES_IDS,
                           fix_method=FixMethod.RE_ESTIMATE)
    event_log = read_xes_log('../assets/test_event_log_1.xes', config)
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
    resource_availability = ResourceAvailability(event_log, config)
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle,
                                                   resource_availability, config)
    # The start time of initial events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == extended_event_log[0][0][config.log_ids.end_timestamp] - \
           (extended_event_log[2][0][config.log_ids.end_timestamp] - extended_event_log[0][0][config.log_ids.end_timestamp])
    assert extended_event_log[1][0][config.log_ids.start_timestamp] == extended_event_log[1][0][config.log_ids.end_timestamp] - \
           (extended_event_log[2][0][config.log_ids.end_timestamp] - extended_event_log[0][0][config.log_ids.end_timestamp])


def test_set_instant_non_estimated_start_times():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z'),
        fix_method=FixMethod.SET_INSTANT
    )
    event_log = read_xes_log('../assets/test_event_log_2.xes', config)
    extended_event_log = set_instant_non_estimated_start_times(event_log, config)
    # The start time of non-estimated events is the end time (instant events)
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == extended_event_log[0][0][config.log_ids.end_timestamp]
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == extended_event_log[1][1][config.log_ids.end_timestamp]


def test_re_estimate_non_estimated_start_times():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z'),
        fix_method=FixMethod.RE_ESTIMATE
    )
    event_log = read_xes_log('../assets/test_event_log_2.xes', config)
    extended_event_log = re_estimate_non_estimated_start_times(event_log, config)
    # The start time of non-estimated events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == \
           extended_event_log[0][0][config.log_ids.end_timestamp] - timedelta(minutes=15)
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == \
           extended_event_log[1][1][config.log_ids.end_timestamp] - timedelta(minutes=30)
