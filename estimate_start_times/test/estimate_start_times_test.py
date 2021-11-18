from datetime import datetime
from datetime import timedelta

from config import ConcurrencyOracleType, Configuration, DEFAULT_XES_IDS, ReEstimationMethod, ResourceAvailabilityType
from estimate_start_times.estimate_start_times import StartTimeEstimator
from event_log_readers import read_csv_log, read_xes_log


def test_estimate_start_times_instant_df():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_csv_log('./assets/test_event_log_1.csv', config)
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


def test_estimate_start_times_instant_el():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_xes_log('./assets/test_event_log_1.xes', config)
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


def test_estimate_start_times_mode_df():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_csv_log('./assets/test_event_log_1.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # The start time of initial events is the most frequent processing time
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == first_trace.iloc[0][config.log_ids.end_timestamp] - \
           (third_trace.iloc[0][config.log_ids.end_timestamp] - first_trace.iloc[0][config.log_ids.end_timestamp])
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[0][config.log_ids.start_timestamp] == second_trace.iloc[0][config.log_ids.end_timestamp] - \
           (third_trace.iloc[0][config.log_ids.end_timestamp] - first_trace.iloc[0][config.log_ids.end_timestamp])


def test_estimate_start_times_mode_el():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_xes_log('./assets/test_event_log_1.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # The start time of initial events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == extended_event_log[0][0][config.log_ids.end_timestamp] - \
           (extended_event_log[2][0][config.log_ids.end_timestamp] - extended_event_log[0][0][config.log_ids.end_timestamp])
    assert extended_event_log[1][0][config.log_ids.start_timestamp] == extended_event_log[1][0][config.log_ids.end_timestamp] - \
           (extended_event_log[2][0][config.log_ids.end_timestamp] - extended_event_log[0][0][config.log_ids.end_timestamp])


def test_set_instant_non_estimated_start_times_df():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._set_instant_non_estimated_start_times_data_frame()
    # The start time of non-estimated events is the end time (instant events)
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == first_trace.iloc[0][config.log_ids.end_timestamp]
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == second_trace.iloc[1][config.log_ids.end_timestamp]


def test_set_instant_non_estimated_start_times_el():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_xes_log('./assets/test_event_log_2.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._set_instant_non_estimated_start_times_event_log()
    # The start time of non-estimated events is the end time (instant events)
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == extended_event_log[0][0][config.log_ids.end_timestamp]
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == extended_event_log[1][1][config.log_ids.end_timestamp]


def test_set_mode_non_estimated_start_times_df():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times_data_frame()
    # The start time of non-estimated events is the most frequent processing time
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == \
           first_trace.iloc[0][config.log_ids.end_timestamp] - timedelta(minutes=15)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == \
           second_trace.iloc[1][config.log_ids.end_timestamp] - timedelta(minutes=30)


def test_set_mode_non_estimated_start_times_el():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_xes_log('./assets/test_event_log_2.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times_event_log()
    # The start time of non-estimated events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == \
           extended_event_log[0][0][config.log_ids.end_timestamp] - timedelta(minutes=15)
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == \
           extended_event_log[1][1][config.log_ids.end_timestamp] - timedelta(minutes=30)


def test_set_mean_non_estimated_start_times_df():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MEAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times_data_frame()
    # The start time of non-estimated events is the most frequent processing time
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == \
           first_trace.iloc[0][config.log_ids.end_timestamp] - timedelta(minutes=13)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == \
           second_trace.iloc[1][config.log_ids.end_timestamp] - timedelta(minutes=24.5)


def test_set_mean_non_estimated_start_times_el():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_xes_log('./assets/test_event_log_2.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times_event_log()
    # The start time of non-estimated events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == \
           extended_event_log[0][0][config.log_ids.end_timestamp] - timedelta(minutes=13)
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == \
           extended_event_log[1][1][config.log_ids.end_timestamp] - timedelta(minutes=24.5)


def test_set_median_non_estimated_start_times_df():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times_data_frame()
    # The start time of non-estimated events is the most frequent processing time
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_timestamp] == \
           first_trace.iloc[0][config.log_ids.end_timestamp] - timedelta(minutes=13.5)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_timestamp] == \
           second_trace.iloc[1][config.log_ids.end_timestamp] - timedelta(minutes=25)


def test_set_median_non_estimated_start_times_el():
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_xes_log('./assets/test_event_log_2.xes', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator._re_estimate_non_estimated_start_times_event_log()
    # The start time of non-estimated events is the most frequent processing time
    assert extended_event_log[0][0][config.log_ids.start_timestamp] == \
           extended_event_log[0][0][config.log_ids.end_timestamp] - timedelta(minutes=13.5)
    assert extended_event_log[1][1][config.log_ids.start_timestamp] == \
           extended_event_log[1][1][config.log_ids.end_timestamp] - timedelta(minutes=25)
