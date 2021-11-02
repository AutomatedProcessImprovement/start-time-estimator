from datetime import datetime
from datetime import timedelta

from config import ConcurrencyOracleType, Configuration, ReEstimationMethod, ResourceAvailabilityType
from data_frame.estimate_start_times import StartTimeEstimator
from event_log_readers import read_csv_log


def test_estimate_start_times_instant():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_csv_log('../assets/test_event_log_1.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
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
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_csv_log('../assets/test_event_log_1.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
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
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('../assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._set_instant_non_estimated_start_times()
    # The start time of non-estimated events is the end time (instant events)
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == first_trace.iloc[0][config.log_ids.end_timestamp]
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == second_trace.iloc[1][config.log_ids.end_timestamp]


def test_re_estimate_non_estimated_start_times():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('../assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times()
    # The start time of non-estimated events is the most frequent processing time
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == \
           first_trace.iloc[0][config.log_ids.end_timestamp] - timedelta(minutes=15)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == \
           second_trace.iloc[1][config.log_ids.end_timestamp] - timedelta(minutes=30)
