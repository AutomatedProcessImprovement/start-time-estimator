from datetime import datetime
from datetime import timedelta

from config import Configuration
from data_frame.concurrency_oracle import NoConcurrencyOracle
from data_frame.estimate_start_times import FixMethod
from data_frame.estimate_start_times import estimate_start_timestamps
from data_frame.estimate_start_times import re_estimate_non_estimated_start_times
from data_frame.estimate_start_times import set_instant_non_estimated_start_times
from data_frame.resource_availability import ResourceAvailability
from event_log_readers import read_csv_log


def test_estimate_start_times_instant():
    config = Configuration(fix_method=FixMethod.SET_INSTANT)
    event_log = read_csv_log('../assets/test_event_log_1.csv', config)
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
    resource_availability = ResourceAvailability(event_log, config)
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle, resource_availability, config)
    # The start time of initial events is the end time (instant events)
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == first_trace.iloc[0][config.log_ids.end_timestamp]
    fourth_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-04']
    assert fourth_trace.iloc[0][config.log_ids.start_timestamp] == fourth_trace.iloc[0][config.log_ids.end_timestamp]
    # The start time of an event with its resource free but immediately
    # following its previous one is the end time of the previous one.
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[3][config.log_ids.start_timestamp] == second_trace.iloc[2][config.log_ids.end_timestamp]
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    assert third_trace.iloc[3][config.log_ids.start_timestamp] == third_trace.iloc[2][config.log_ids.end_timestamp]
    # The start time of an event enabled for a long time but with its resource
    # busy in other activities is the end time of its resource's last activity.
    assert fourth_trace.iloc[3][config.log_ids.start_timestamp] == third_trace.iloc[2][config.log_ids.end_timestamp]
    assert fourth_trace.iloc[4][config.log_ids.start_timestamp] == second_trace.iloc[4][config.log_ids.end_timestamp]


def test_estimate_start_times_re_estimate():
    config = Configuration(fix_method=FixMethod.RE_ESTIMATE)
    event_log = read_csv_log('../assets/test_event_log_1.csv', config)
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
    resource_availability = ResourceAvailability(event_log, config)
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle, resource_availability, config)
    # The start time of initial events is the most frequent processing time
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == first_trace.iloc[0][config.log_ids.end_timestamp] - \
           (third_trace.iloc[0][config.log_ids.end_timestamp] - first_trace.iloc[0][config.log_ids.end_timestamp])
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[0][config.log_ids.start_timestamp] == second_trace.iloc[0][config.log_ids.end_timestamp] - \
           (third_trace.iloc[0][config.log_ids.end_timestamp] - first_trace.iloc[0][config.log_ids.end_timestamp])


def test_set_instant_non_estimated_start_times():
    config = Configuration(
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z'),
        fix_method=FixMethod.SET_INSTANT
    )
    event_log = read_csv_log('../assets/test_event_log_2.csv', config)
    extended_event_log = set_instant_non_estimated_start_times(event_log, config)
    # The start time of non-estimated events is the end time (instant events)
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == first_trace.iloc[0][config.log_ids.end_timestamp]
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == second_trace.iloc[1][config.log_ids.end_timestamp]


def test_re_estimate_non_estimated_start_times():
    config = Configuration(
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z'),
        fix_method=FixMethod.RE_ESTIMATE
    )
    event_log = read_csv_log('../assets/test_event_log_2.csv', config)
    extended_event_log = re_estimate_non_estimated_start_times(event_log, config)
    # The start time of non-estimated events is the most frequent processing time
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == \
           first_trace.iloc[0][config.log_ids.end_timestamp] - timedelta(minutes=15)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == \
           second_trace.iloc[1][config.log_ids.end_timestamp] - timedelta(minutes=30)
