import pm4py
import pytz
from datetime import datetime
from event_log.concurrency_oracle import AlphaConcurrencyOracle
from event_log.concurrency_oracle import NoConcurrencyOracle

initial_time = datetime.min.replace(tzinfo=pytz.UTC)
event_log_1 = pm4py.read_xes('../assets/test_event_log_1.xes')


def test_no_concurrency_oracle():
    concurrency_oracle = NoConcurrencyOracle(event_log_1, initial_time)
    # The initial time to use as default is the passed
    assert concurrency_oracle.initial_time == initial_time
    # No concurrency by default
    assert concurrency_oracle.concurrency == {'A': [], 'B': [], 'C': [], 'D': [], 'E': [], 'F': [], 'G': [], 'H': [],
                                              'I': []}
    # There is no concurrency, so always enabled since the last event finished
    assert concurrency_oracle.enabled_since(event_log_1[0], event_log_1[0][4]) == event_log_1[0][3]['time:timestamp']
    # There is no concurrency, so always enabled since the last event finished
    assert concurrency_oracle.enabled_since(event_log_1[2], event_log_1[2][3]) == event_log_1[2][2]['time:timestamp']
    # initial_time as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log_1[3], event_log_1[3][0]) == initial_time


def test_alpha_concurrency_oracle():
    concurrency_oracle = AlphaConcurrencyOracle(event_log_1, initial_time)
    # The initial time to use as default is the passed
    assert concurrency_oracle.initial_time == initial_time
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': [], 'B': [], 'C': ['D'], 'D': ['C'], 'E': [], 'F': [], 'G': [],
                                              'H': [], 'I': []}
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log_1[0], event_log_1[0][6]) == event_log_1[0][5]['time:timestamp']
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log_1[2], event_log_1[2][5]) == event_log_1[2][4]['time:timestamp']
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log_1[1], event_log_1[1][3]) == event_log_1[1][1]['time:timestamp']
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log_1[3], event_log_1[3][3]) == event_log_1[3][1]['time:timestamp']
    # initial_time as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log_1[3], event_log_1[3][0]) == initial_time
