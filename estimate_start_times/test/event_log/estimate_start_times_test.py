from datetime import datetime
from datetime import timedelta

from config import ConcurrencyOracleType, Configuration, DEFAULT_XES_IDS, ReEstimationMethod, ResourceAvailabilityType
from event_log.estimate_start_times import StartTimeEstimator
from event_log_readers import read_xes_log


def test_estimate_start_times_instant():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_xes_log('../assets/test_event_log_1.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
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
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_xes_log('../assets/test_event_log_1.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # The start time of initial events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == extended_event_log[0][0][config.log_ids.end_timestamp] - \
           (extended_event_log[2][0][config.log_ids.end_timestamp] - extended_event_log[0][0][config.log_ids.end_timestamp])
    assert extended_event_log[1][0][config.log_ids.start_timestamp] == extended_event_log[1][0][config.log_ids.end_timestamp] - \
           (extended_event_log[2][0][config.log_ids.end_timestamp] - extended_event_log[0][0][config.log_ids.end_timestamp])


def test_set_instant_non_estimated_start_times():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_xes_log('../assets/test_event_log_2.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._set_instant_non_estimated_start_times()
    # The start time of non-estimated events is the end time (instant events)
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == extended_event_log[0][0][config.log_ids.end_timestamp]
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == extended_event_log[1][1][config.log_ids.end_timestamp]


def test_re_estimate_non_estimated_start_times():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_xes_log('../assets/test_event_log_2.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times()
    # The start time of non-estimated events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == \
           extended_event_log[0][0][config.log_ids.end_timestamp] - timedelta(minutes=15)
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == \
           extended_event_log[1][1][config.log_ids.end_timestamp] - timedelta(minutes=30)
