from config import Configuration
from data_frame.concurrency_oracle import AlphaConcurrencyOracle
from data_frame.concurrency_oracle import NoConcurrencyOracle
from event_log_readers import read_csv_log


def test_no_concurrency_oracle():
    config = Configuration()
    event_log = read_csv_log('../assets/test_event_log_1.csv', config)
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
    # No concurrency by default
    assert concurrency_oracle.concurrency == {'A': [], 'B': [], 'C': [], 'D': [], 'E': [], 'F': [], 'G': [], 'H': [], 'I': []}
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # There is no concurrency, so always enabled since the last event finished
    first_trace = event_log[event_log[config.log_ids.case] == 'trace-01']
    assert concurrency_oracle.enabled_since(first_trace, first_trace.iloc[4]) == first_trace.iloc[3][config.log_ids.end_timestamp]
    # There is no concurrency, so always enabled since the last event finished
    third_trace = event_log[event_log[config.log_ids.case] == 'trace-03']
    assert concurrency_oracle.enabled_since(third_trace, third_trace.iloc[3]) == third_trace.iloc[2][config.log_ids.end_timestamp]
    # [non_estimated_time] as the enablement time of the first event in the trace
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]) == config.non_estimated_time


def test_alpha_concurrency_oracle():
    config = Configuration()
    event_log = read_csv_log('../assets/test_event_log_1.csv', config)
    concurrency_oracle = AlphaConcurrencyOracle(event_log, config)
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': [], 'B': [], 'C': ['D'], 'D': ['C'], 'E': [], 'F': [], 'G': [], 'H': [], 'I': []}
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Enabled since the previous event when there is no concurrency
    first_trace = event_log[event_log[config.log_ids.case] == 'trace-01']
    assert concurrency_oracle.enabled_since(first_trace, first_trace.iloc[6]) == first_trace.iloc[5][config.log_ids.end_timestamp]
    # Enabled since the previous event when there is no concurrency
    third_trace = event_log[event_log[config.log_ids.case] == 'trace-03']
    assert concurrency_oracle.enabled_since(third_trace, third_trace.iloc[5]) == third_trace.iloc[4][config.log_ids.end_timestamp]
    # Enabled since its causal input for an event when the previous one is concurrent
    second_trace = event_log[event_log[config.log_ids.case] == 'trace-02']
    assert concurrency_oracle.enabled_since(second_trace, second_trace.iloc[3]) == second_trace.iloc[1][config.log_ids.end_timestamp]
    # Enabled since its causal input for an event when the previous one is concurrent
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[3]) == fourth_trace.iloc[1][config.log_ids.end_timestamp]
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]) == config.non_estimated_time
