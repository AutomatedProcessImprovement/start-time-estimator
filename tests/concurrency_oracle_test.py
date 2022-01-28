from datetime import datetime

import pm4py

from estimate_start_times.concurrency_oracle import AlphaConcurrencyOracle, HeuristicsConcurrencyOracle, \
    NoConcurrencyOracle, DeactivatedConcurrencyOracle
from estimate_start_times.event_log_readers import read_csv_log
from estimate_start_times.start_time_config import Configuration, DEFAULT_XES_IDS, HeuristicsThresholds


def test_deactivated_concurrency_oracle():
    config = Configuration()
    concurrency_oracle = DeactivatedConcurrencyOracle(config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Empty set as concurrency by default
    assert concurrency_oracle.concurrency == {}
    # The concurrency option is deactivated, so always return [non_estimated_time]
    assert concurrency_oracle.enabled_since(None, datetime.now()) == config.non_estimated_time
    # There is no concurrency, so always enabled since the last event finished
    assert concurrency_oracle.enabled_since(None, datetime.fromisoformat('2012-11-07T10:00:00.000+02:00')) == config.non_estimated_time
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(None, datetime.fromisoformat('2006-07-20T22:03:11.000+02:00')) == config.non_estimated_time


def test_no_concurrency_oracle_el():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('./tests/assets/test_event_log_1.xes')
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # No concurrency by default
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': set(), 'D': set(), 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # There is no concurrency, so always enabled since the last event finished
    assert concurrency_oracle.enabled_since(event_log[0], event_log[0][4]) == event_log[0][3][config.log_ids.end_time]
    # There is no concurrency, so always enabled since the last event finished
    assert concurrency_oracle.enabled_since(event_log[2], event_log[2][3]) == event_log[2][2][config.log_ids.end_time]
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][0]) == config.non_estimated_time


def test_no_concurrency_oracle_df():
    config = Configuration()
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
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
    # [non_estimated_time] as the enablement time of the first event in the trace
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]) == config.non_estimated_time


def test_alpha_concurrency_oracle_el():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('./tests/assets/test_event_log_1.xes')
    concurrency_oracle = AlphaConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': {'D'}, 'D': {'C'}, 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[0], event_log[0][6]) == event_log[0][5][config.log_ids.end_time]
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[2], event_log[2][5]) == event_log[2][4][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[1], event_log[1][3]) == event_log[1][1][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][3]) == event_log[3][1][config.log_ids.end_time]
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][0]) == config.non_estimated_time


def test_alpha_concurrency_oracle_df():
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
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]) == config.non_estimated_time


def test_heuristics_concurrency_oracle_simple_el():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('./tests/assets/test_event_log_1.xes')
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': {'D'}, 'D': {'C'}, 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[0], event_log[0][6]) == event_log[0][5][config.log_ids.end_time]
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[2], event_log[2][5]) == event_log[2][4][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[1], event_log[1][3]) == event_log[1][1][config.log_ids.end_time]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][3]) == event_log[3][1][config.log_ids.end_time]
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][0]) == config.non_estimated_time


def test_heuristics_concurrency_oracle_simple_df():
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
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(fourth_trace, fourth_trace.iloc[0]) == config.non_estimated_time


def test_heuristics_concurrency_oracle_multi_parallel_el():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('./tests/assets/test_event_log_3.xes')
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


def test_heuristics_concurrency_oracle_multi_parallel_df():
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


def test_heuristics_concurrency_oracle_multi_parallel_noise_el():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('./tests/assets/test_event_log_3_noise.xes')
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
    config = Configuration(log_ids=DEFAULT_XES_IDS,
                           heuristics_thresholds=HeuristicsThresholds(df=1.0, l2l=1.0))
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


def test_heuristics_concurrency_oracle_multi_parallel_noise_df():
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
