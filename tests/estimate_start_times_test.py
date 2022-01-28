from datetime import timedelta

import pandas as pd

from estimate_start_times.config import ConcurrencyOracleType, Configuration, ReEstimationMethod, ResourceAvailabilityType, OutlierStatistic
from estimate_start_times.estimator import StartTimeEstimator
from estimate_start_times.utils import read_csv_log


def test_estimate_start_times_only_resource():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.DEACTIVATED,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # Traces
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    fourth_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-04']
    # The start time of initial events is their end time (instant events)
    assert first_trace.iloc[0][config.log_ids.start_time] == first_trace.iloc[0][config.log_ids.end_time]
    assert fourth_trace.iloc[0][config.log_ids.start_time] == fourth_trace.iloc[0][config.log_ids.end_time]
    # The start time of all other events is the availability of the resource (concurrency deactivated)
    assert second_trace.iloc[3][config.log_ids.start_time] == first_trace.iloc[2][config.log_ids.end_time]
    assert third_trace.iloc[3][config.log_ids.start_time] == third_trace.iloc[1][config.log_ids.end_time]
    assert fourth_trace.iloc[3][config.log_ids.start_time] == third_trace.iloc[2][config.log_ids.end_time]
    assert fourth_trace.iloc[4][config.log_ids.start_time] == second_trace.iloc[4][config.log_ids.end_time]
    assert first_trace.iloc[2][config.log_ids.start_time] == fourth_trace.iloc[3][config.log_ids.end_time]


def test_estimate_start_times_instant():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    # Set one start timestamp manually
    manually_added_timestamp = pd.to_datetime('2002-11-07 12:33:00+02:00', format='%Y-%m-%d %H:%M:%S%z', utc=True)
    event_log.loc[
        (event_log[config.log_ids.case] == 'trace-01') & (event_log[config.log_ids.activity] == 'C'),
        config.log_ids.start_time
    ] = manually_added_timestamp
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # The start time of initial events is the end time (instant events)
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_time] == first_trace.iloc[0][config.log_ids.end_time]
    fourth_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-04']
    assert fourth_trace.iloc[0][config.log_ids.start_time] == fourth_trace.iloc[0][config.log_ids.end_time]
    # The start time of an event with its resource free but immediately
    # following its previous one is the end time of the previous one.
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[3][config.log_ids.start_time] == second_trace.iloc[2][config.log_ids.end_time]
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    assert third_trace.iloc[3][config.log_ids.start_time] == third_trace.iloc[2][config.log_ids.end_time]
    # The start time of an event enabled for a long time but with its resource
    # busy in other activities is the end time of its resource's last activity.
    assert fourth_trace.iloc[3][config.log_ids.start_time] == third_trace.iloc[2][config.log_ids.end_time]
    assert fourth_trace.iloc[4][config.log_ids.start_time] == second_trace.iloc[4][config.log_ids.end_time]
    # The event with predefined start time was not predicted
    assert first_trace.iloc[2][config.log_ids.start_time] == manually_added_timestamp


def test_bot_resources_and_instant_activities():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={'Marcus'},
        instant_activities={'H', 'I'}
    )
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # The events performed by bot resources, or being instant activities are instant
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    fourth_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-04']
    assert second_trace.iloc[2][config.log_ids.start_time] == second_trace.iloc[2][config.log_ids.end_time]
    assert fourth_trace.iloc[6][config.log_ids.start_time] == fourth_trace.iloc[6][config.log_ids.end_time]
    assert fourth_trace.iloc[7][config.log_ids.start_time] == fourth_trace.iloc[7][config.log_ids.end_time]
    # The start time of initial events (with no bot resources nor instant activities) is the end time (instant events)
    assert second_trace.iloc[0][config.log_ids.start_time] == second_trace.iloc[0][config.log_ids.end_time]
    assert fourth_trace.iloc[0][config.log_ids.start_time] == fourth_trace.iloc[0][config.log_ids.end_time]
    # The start time of an event (no bot resource nor instant activity) with its resource
    # free but immediately following its previous one is the end time of the previous one.
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[3][config.log_ids.start_time] == second_trace.iloc[2][config.log_ids.end_time]
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    assert third_trace.iloc[3][config.log_ids.start_time] == third_trace.iloc[2][config.log_ids.end_time]
    # The start time of an event (no bot resource nor instant activity) enabled for a long time
    # but with its resource busy in other activities is the end time of its resource's last activity.
    assert fourth_trace.iloc[3][config.log_ids.start_time] == third_trace.iloc[2][config.log_ids.end_time]
    assert fourth_trace.iloc[4][config.log_ids.start_time] == second_trace.iloc[4][config.log_ids.end_time]


def test_repair_activities_with_duration_over_threshold():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=1.6
    )
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # The start time of an event (with duration under the threshold) with its resource
    # free but immediately following its previous one is the end time of the previous one.
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[3][config.log_ids.start_time] == second_trace.iloc[2][config.log_ids.end_time]
    # The start time of an event (with duration under the threshold) enabled for a long time
    # but with its resource busy in other activities is the end time of its resource's last activity.
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    fourth_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-04']
    assert fourth_trace.iloc[3][config.log_ids.start_time] == third_trace.iloc[2][config.log_ids.end_time]
    assert fourth_trace.iloc[4][config.log_ids.start_time] == second_trace.iloc[4][config.log_ids.end_time]
    # The events with estimated durations over the threshold where re-estimated
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[1][config.log_ids.start_time] == \
           first_trace.iloc[1][config.log_ids.end_time] - timedelta(minutes=49.6)
    assert third_trace.iloc[2][config.log_ids.start_time] == \
           third_trace.iloc[2][config.log_ids.end_time] - timedelta(minutes=11.2)
    assert first_trace.iloc[6][config.log_ids.start_time] == \
           first_trace.iloc[6][config.log_ids.end_time] - timedelta(minutes=38.4)


def test_estimate_start_times_mode():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # The start time of initial events is the most frequent duration
    third_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-03']
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_time] == first_trace.iloc[0][config.log_ids.end_time] - \
           (third_trace.iloc[0][config.log_ids.end_time] - first_trace.iloc[0][config.log_ids.end_time])
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[0][config.log_ids.start_time] == second_trace.iloc[0][config.log_ids.end_time] - \
           (third_trace.iloc[0][config.log_ids.end_time] - first_trace.iloc[0][config.log_ids.end_time])


def test_set_instant_non_estimated_start_times():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.SET_INSTANT,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=pd.to_datetime('2000-01-01T10:00:00.000+02:00', format='%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./tests/assets/test_event_log_2.csv', config, False)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    start_time_estimator._set_instant_non_estimated_start_times()
    extended_event_log = start_time_estimator.event_log
    # The start time of non-estimated events is the end time (instant events)
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_time] == first_trace.iloc[0][config.log_ids.end_time]
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_time] == second_trace.iloc[1][config.log_ids.end_time]


def test_set_mode_non_estimated_start_times():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=pd.to_datetime('2000-01-01T10:00:00.000+02:00', format='%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./tests/assets/test_event_log_2.csv', config, False)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    start_time_estimator._re_estimate_non_estimated_start_times()
    extended_event_log = start_time_estimator.event_log
    # The start time of non-estimated events is the most frequent duration
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_time] == first_trace.iloc[0][config.log_ids.end_time] - timedelta(minutes=15)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_time] == second_trace.iloc[1][config.log_ids.end_time] - timedelta(minutes=30)


def test_set_mean_non_estimated_start_times():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MEAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=pd.to_datetime('2000-01-01T10:00:00.000+02:00', format='%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./tests/assets/test_event_log_2.csv', config, False)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    start_time_estimator._re_estimate_non_estimated_start_times()
    extended_event_log = start_time_estimator.event_log
    # The start time of non-estimated events is the most frequent duration
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_time] == first_trace.iloc[0][config.log_ids.end_time] - timedelta(minutes=13)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_time] == second_trace.iloc[1][config.log_ids.end_time] - timedelta(minutes=24.5)


def test_set_median_non_estimated_start_times():
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=pd.to_datetime('2000-01-01T10:00:00.000+02:00', format='%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./tests/assets/test_event_log_2.csv', config, False)
    # Estimate start times
    start_time_estimator = StartTimeEstimator(event_log, config)
    start_time_estimator._re_estimate_non_estimated_start_times()
    extended_event_log = start_time_estimator.event_log
    # The start time of non-estimated events is the most frequent duration
    first_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-01']
    assert first_trace.iloc[0][config.log_ids.start_time] == first_trace.iloc[0][config.log_ids.end_time] - timedelta(minutes=13.5)
    second_trace = extended_event_log[extended_event_log[config.log_ids.case] == 'trace-02']
    assert second_trace.iloc[1][config.log_ids.start_time] == second_trace.iloc[1][config.log_ids.end_time] - timedelta(minutes=25)


def test_get_activity_duration():
    durations = {
        'A': [timedelta(2), timedelta(2), timedelta(4), timedelta(6), timedelta(7), timedelta(9)],
        'B': [timedelta(2), timedelta(2), timedelta(4), timedelta(8)],
        'C': [timedelta(2), timedelta(2), timedelta(3)]
    }
    # MEAN
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MEAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=pd.to_datetime('2000-01-01T10:00:00.000+02:00', format='%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./tests/assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    assert start_time_estimator._get_activity_duration(durations, 'A') == timedelta(5)
    assert start_time_estimator._get_activity_duration(durations, 'B') == timedelta(4)
    assert start_time_estimator._get_activity_duration(durations, 'C') == timedelta(days=2, hours=8)
    assert start_time_estimator._get_activity_duration(durations, 'Z') == timedelta(0)
    # MEDIAN
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=pd.to_datetime('2000-01-01T10:00:00.000+02:00', format='%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./tests/assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    assert start_time_estimator._get_activity_duration(durations, 'A') == timedelta(5)
    assert start_time_estimator._get_activity_duration(durations, 'B') == timedelta(3)
    assert start_time_estimator._get_activity_duration(durations, 'C') == timedelta(2)
    assert start_time_estimator._get_activity_duration(durations, 'Z') == timedelta(0)
    # MODE
    config = Configuration(
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.NONE,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        non_estimated_time=pd.to_datetime('2000-01-01T10:00:00.000+02:00', format='%Y-%m-%dT%H:%M:%S.%f%z')
    )
    event_log = read_csv_log('./tests/assets/test_event_log_2.csv', config)
    start_time_estimator = StartTimeEstimator(event_log, config)
    assert start_time_estimator._get_activity_duration(durations, 'A') == timedelta(2)
    assert start_time_estimator._get_activity_duration(durations, 'B') == timedelta(2)
    assert start_time_estimator._get_activity_duration(durations, 'C') == timedelta(2)
    assert start_time_estimator._get_activity_duration(durations, 'Z') == timedelta(0)
