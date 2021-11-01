import pm4py
import pytz
from event_log.concurrency_oracle import NoConcurrencyOracle
from event_log.estimate_start_times import estimate_start_timestamps
from event_log.estimate_start_times import set_instant_non_estimated_start_times
from event_log.estimate_start_times import re_estimate_non_estimated_start_times
from event_log.estimate_start_times import FixMethod
from event_log.resource_availability import ResourceAvailability
from datetime import datetime
from datetime import timedelta


def test_estimate_start_times_instant():
    initial_time = datetime.min.replace(tzinfo=pytz.UTC)
    event_log = pm4py.read_xes('../assets/test_event_log_1.xes')
    concurrency_oracle = NoConcurrencyOracle(event_log, initial_time)
    resource_availability = ResourceAvailability(event_log, initial_time, 'missing_resource')
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle, resource_availability)
    # The start time of initial events is the end time (instant events)
    assert extended_event_log[0][0]['start:timestamp'] == extended_event_log[0][0]['time:timestamp']
    assert extended_event_log[3][0]['start:timestamp'] == extended_event_log[3][0]['time:timestamp']
    # The start time of an event with its resource free but immediately
    # following its previous one is the end time of the previous one.
    assert extended_event_log[1][3]['start:timestamp'] == event_log[1][2]['time:timestamp']
    assert extended_event_log[2][3]['start:timestamp'] == event_log[2][2]['time:timestamp']
    # The start time of an event enabled for a long time but with its resource
    # busy in other activities is the end time of its resource's last activity.
    assert extended_event_log[3][3]['start:timestamp'] == event_log[2][2]['time:timestamp']
    assert extended_event_log[3][4]['start:timestamp'] == event_log[1][4]['time:timestamp']


def test_estimate_start_times_re_estimate():
    initial_time = datetime.min.replace(tzinfo=pytz.UTC)
    event_log = pm4py.read_xes('../assets/test_event_log_1.xes')
    concurrency_oracle = NoConcurrencyOracle(event_log, initial_time)
    resource_availability = ResourceAvailability(event_log, initial_time, 'missing_resource')
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle,
                                                   resource_availability, FixMethod.RE_ESTIMATE)
    # The start time of initial events is the most frequent processing time
    assert extended_event_log[0][0]['start:timestamp'] == extended_event_log[0][0]['time:timestamp'] - \
           (extended_event_log[2][0]['time:timestamp'] - extended_event_log[0][0]['time:timestamp'])
    assert extended_event_log[1][0]['start:timestamp'] == extended_event_log[1][0]['time:timestamp'] - \
           (extended_event_log[2][0]['time:timestamp'] - extended_event_log[0][0]['time:timestamp'])


def test_set_instant_non_estimated_start_times():
    non_estimated_time = datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    event_log = pm4py.read_xes('../assets/test_event_log_2.xes')
    extended_event_log = set_instant_non_estimated_start_times(event_log, non_estimated_time)
    # The start time of non-estimated events is the end time (instant events)
    assert extended_event_log[0][0]['start:timestamp'] == extended_event_log[0][0]['time:timestamp']
    assert extended_event_log[1][1]['start:timestamp'] == extended_event_log[1][1]['time:timestamp']


def test_re_estimate_non_estimated_start_times():
    non_estimated_time = datetime.strptime('2000-01-01T10:00:00.000+02:00', '%Y-%m-%dT%H:%M:%S.%f%z')
    event_log = pm4py.read_xes('../assets/test_event_log_2.xes')
    extended_event_log = re_estimate_non_estimated_start_times(event_log, non_estimated_time)
    # The start time of non-estimated events is the most frequent processing time
    assert extended_event_log[0][0]['start:timestamp'] == extended_event_log[0][0]['time:timestamp'] - \
           timedelta(minutes=15)
    assert extended_event_log[1][1]['start:timestamp'] == extended_event_log[1][1]['time:timestamp'] - \
           timedelta(minutes=30)
