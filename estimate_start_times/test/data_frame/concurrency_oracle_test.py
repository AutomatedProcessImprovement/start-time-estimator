import pytz
from datetime import datetime
from data_frame.concurrency_oracle import AlphaConcurrencyOracle
from data_frame.concurrency_oracle import NoConcurrencyOracle
from event_log_readers import read_csv_log

initial_time = datetime.min.replace(tzinfo=pytz.UTC)
event_log_1 = read_csv_log('../assets/test_event_log_1.csv')


def test_no_concurrency_oracle():
    concurrency_oracle = NoConcurrencyOracle(event_log_1, initial_time)
    # No concurrency by default
    assert concurrency_oracle.concurrency == {'A': [], 'B': [], 'C': [], 'D': [], 'E': [],
                                              'F': [], 'G': [], 'H': [], 'I': []}
    # The initial time to use as default is the passed
    assert concurrency_oracle.initial_time == initial_time
    # There is no concurrency, so always enabled since the last event finished
    first_trace = event_log_1[event_log_1['caseID'] == 'trace-01']
    assert concurrency_oracle.enabled_since(first_trace, first_trace.iloc[4]) == first_trace.iloc[3]['end_timestamp']
    # There is no concurrency, so always enabled since the last event finished
    third_trace = event_log_1[event_log_1['caseID'] == 'trace-03']
    assert concurrency_oracle.enabled_since(third_trace, third_trace.iloc[3]) == third_trace.iloc[2]['end_timestamp']
    # initial_time as the enablement time of the first event in the trace
    fourth_trace = event_log_1[event_log_1['caseID'] == 'trace-04']
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]) == initial_time


def test_alpha_concurrency_oracle():
    concurrency_oracle = AlphaConcurrencyOracle(event_log_1, initial_time)
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': [], 'B': [], 'C': ['D'], 'D': ['C'],
                                              'E': [], 'F': [], 'G': [], 'H': [], 'I': []}
    # The initial time to use as default is the passed
    assert concurrency_oracle.initial_time == initial_time
    # Enabled since the previous event when there is no concurrency
    first_trace = event_log_1[event_log_1['caseID'] == 'trace-01']
    assert concurrency_oracle.enabled_since(first_trace, first_trace.iloc[6]) == first_trace.iloc[5]['end_timestamp']
    # Enabled since the previous event when there is no concurrency
    third_trace = event_log_1[event_log_1['caseID'] == 'trace-03']
    assert concurrency_oracle.enabled_since(third_trace, third_trace.iloc[5]) == third_trace.iloc[4]['end_timestamp']
    # Enabled since its causal input for an event when the previous one is concurrent
    second_trace = event_log_1[event_log_1['caseID'] == 'trace-02']
    assert concurrency_oracle.enabled_since(second_trace, second_trace.iloc[3]) == second_trace.iloc[1]['end_timestamp']
    # Enabled since its causal input for an event when the previous one is concurrent
    fourth_trace = event_log_1[event_log_1['caseID'] == 'trace-04']
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[3]) == fourth_trace.iloc[1]['end_timestamp']
    # initial_time as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]) == initial_time
