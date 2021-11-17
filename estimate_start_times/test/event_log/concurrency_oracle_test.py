import pm4py

from config import Configuration, DEFAULT_XES_IDS, HeuristicsThresholds
from event_log.concurrency_oracle import AlphaConcurrencyOracle, HeuristicsConcurrencyOracle, NoConcurrencyOracle


def test_no_concurrency_oracle():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('../assets/test_event_log_1.xes')
    concurrency_oracle = NoConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # No concurrency by default
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': set(), 'D': set(), 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # There is no concurrency, so always enabled since the last event finished
    assert concurrency_oracle.enabled_since(event_log[0], event_log[0][4]) == event_log[0][3][config.log_ids.end_timestamp]
    # There is no concurrency, so always enabled since the last event finished
    assert concurrency_oracle.enabled_since(event_log[2], event_log[2][3]) == event_log[2][2][config.log_ids.end_timestamp]
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][0]) == config.non_estimated_time


def test_alpha_concurrency_oracle():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('../assets/test_event_log_1.xes')
    concurrency_oracle = AlphaConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': {'D'}, 'D': {'C'}, 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[0], event_log[0][6]) == event_log[0][5][config.log_ids.end_timestamp]
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[2], event_log[2][5]) == event_log[2][4][config.log_ids.end_timestamp]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[1], event_log[1][3]) == event_log[1][1][config.log_ids.end_timestamp]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][3]) == event_log[3][1][config.log_ids.end_timestamp]
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][0]) == config.non_estimated_time


def test_heuristics_concurrency_oracle_simple():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('../assets/test_event_log_1.xes')
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, config)
    # The configuration for the algorithm is the passed
    assert concurrency_oracle.config == config
    # Concurrency between the activities that appear both one before the other
    assert concurrency_oracle.concurrency == {'A': set(), 'B': set(), 'C': {'D'}, 'D': {'C'}, 'E': set(),
                                              'F': set(), 'G': set(), 'H': set(), 'I': set()}
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[0], event_log[0][6]) == event_log[0][5][config.log_ids.end_timestamp]
    # Enabled since the previous event when there is no concurrency
    assert concurrency_oracle.enabled_since(event_log[2], event_log[2][5]) == event_log[2][4][config.log_ids.end_timestamp]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[1], event_log[1][3]) == event_log[1][1][config.log_ids.end_timestamp]
    # Enabled since its causal input for an event when the previous one is concurrent
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][3]) == event_log[3][1][config.log_ids.end_timestamp]
    # [non_estimated_time] as the enablement time of the first event in the trace
    assert concurrency_oracle.enabled_since(event_log[3], event_log[3][0]) == config.non_estimated_time


def test_heuristics_concurrency_oracle_multi_parallel():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('../assets/test_event_log_3.xes')
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
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = pm4py.read_xes('../assets/test_event_log_3_noise.xes')
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
