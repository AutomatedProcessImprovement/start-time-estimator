from datetime import datetime

import pandas as pd

from estimate_start_times.concurrency_oracle import AlphaConcurrencyOracle, HeuristicsConcurrencyOracle, \
    DirectlyFollowsConcurrencyOracle, DeactivatedConcurrencyOracle
from estimate_start_times.config import Configuration, HeuristicsThresholds
from estimate_start_times.utils import read_csv_log


def test_deactivated_concurrency_oracle():
    config = Configuration()
    concurrency_oracle = DeactivatedConcurrencyOracle(config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Empty set as concurrency by default
    assert concurrency_oracle.concurrency == {}
    # The concurrency option is deactivated, so always return pd.NaT
    assert pd.isna(concurrency_oracle.enabled_since(None, datetime.now()))
    # There is no concurrency, so always enabled since the last event finished
    assert pd.isna(concurrency_oracle.enabled_since(None, datetime.fromisoformat('2012-11-07T10:00:00.000+02:00')))
    # pd.NaT as the enablement time of the first event in the trace
    assert pd.isna(concurrency_oracle.enabled_since(None, datetime.fromisoformat('2006-07-20T22:03:11.000+02:00')))


def test_no_concurrency_oracle():
    config = Configuration()
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    concurrency_oracle = DirectlyFollowsConcurrencyOracle(event_log, config)
    # No concurrency by default
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': set(), 'D': set(), 'E': set(), 'F': set(), 'G': set(),
                                              'H': set(), 'I': set()}
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # There is no concurrency, so always enabled since the last event finished
    first_trace = event_log[event_log[config.log_ids.case] == 'trace-01']
    assert concurrency_oracle.enabled_since(first_trace, first_trace.iloc[4]) == first_trace.iloc[3][config.log_ids.end_time]
    # There is no concurrency, so always enabled since the last event finished
    third_trace = event_log[event_log[config.log_ids.case] == 'trace-03']
    assert concurrency_oracle.enabled_since(third_trace, third_trace.iloc[3]) == third_trace.iloc[2][config.log_ids.end_time]
    # pd.NaT as the enablement time of the first event in the trace
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert pd.isna(concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]))


def test_alpha_concurrency_oracle():
    config = Configuration()
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    concurrency_oracle = AlphaConcurrencyOracle(event_log, config)
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': {'D'}, 'D': {'C'}, 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Enabled since the previous event when there is no concurrency
    first_trace = event_log[event_log[config.log_ids.case] == 'trace-01']
    assert concurrency_oracle.enabled_since(first_trace, first_trace.iloc[6]) == first_trace.iloc[5][config.log_ids.end_time]
    # Enabled since the previous event when there is no concurrency
    third_trace = event_log[event_log[config.log_ids.case] == 'trace-03']
    assert concurrency_oracle.enabled_since(third_trace, third_trace.iloc[5]) == third_trace.iloc[4][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    second_trace = event_log[event_log[config.log_ids.case] == 'trace-02']
    assert concurrency_oracle.enabled_since(second_trace, second_trace.iloc[3]) == second_trace.iloc[1][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[3]) == fourth_trace.iloc[1][config.log_ids.end_time]
    # pd.NaT as the enablement time of the first event in the trace
    assert pd.isna(concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]))


def test_heuristics_concurrency_oracle_simple():
    config = Configuration()
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, config)
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': {'D'}, 'D': {'C'}, 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Enabled since the previous event when there is no concurrency
    first_trace = event_log[event_log[config.log_ids.case] == 'trace-01']
    assert concurrency_oracle.enabled_since(first_trace, first_trace.iloc[6]) == first_trace.iloc[5][config.log_ids.end_time]
    # Enabled since the previous event when there is no concurrency
    third_trace = event_log[event_log[config.log_ids.case] == 'trace-03']
    assert concurrency_oracle.enabled_since(third_trace, third_trace.iloc[5]) == third_trace.iloc[4][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    second_trace = event_log[event_log[config.log_ids.case] == 'trace-02']
    assert concurrency_oracle.enabled_since(second_trace, second_trace.iloc[3]) == second_trace.iloc[1][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[3]) == fourth_trace.iloc[1][config.log_ids.end_time]
    # pd.NaT as the enablement time of the first event in the trace
    assert pd.isna(concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]))


def test_heuristics_concurrency_oracle_multi_parallel():
    config = Configuration()
    event_log = read_csv_log('./tests/assets/test_event_log_3.csv', config)
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {
        'A': set(),
        'B': set(),
        'C': {'D', 'F', 'G'},
        'D': {'C', 'E'},
        'E': {'D', 'F', 'G'},
        'F': {'C', 'E'},
        'G': {'C', 'E'},
        'H': set(),
        'I': set()
    }


def test_heuristics_concurrency_oracle_multi_parallel_noise():
    config = Configuration()
    event_log = read_csv_log('./tests/assets/test_event_log_3_noise.csv', config)
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {
        'A': set(),
        'B': set(),
        'C': {'D', 'F', 'G'},
        'D': {'C', 'E'},
        'E': {'D', 'F', 'G'},
        'F': {'C', 'E'},
        'G': {'C', 'E'},
        'H': set(),
        'I': set()
    }
    # Increasing the thresholds so the directly-follows relations and the length-2 loops
    # detection only detect when the relation happens all the times the activities appear.
    config = Configuration(heuristics_thresholds=HeuristicsThresholds(df=1.0, l2l=1.0))
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {
        'A': set(),
        'B': set(),
        'C': {'D', 'F', 'G'},
        'D': {'C', 'E'},
        'E': {'D', 'F', 'G'},
        'F': {'C', 'E'},
        'G': {'C', 'E'},
        'H': {'I'},
        'I': {'H'}
    }
