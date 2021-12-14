from datetime import datetime
from typing import Union

import pandas as pd
from pm4py.objects.log.obj import EventLog

from common import get_activities, zip_with_next
from config import EventLogIDs, Configuration


class ConcurrencyOracle:
    def __init__(self, concurrency: dict, config: Configuration):
        # Dict with the concurrency: self.concurrency[A] = set of activities concurrent with A
        self.concurrency = concurrency
        # Configuration parameters
        self.config = config
        # Set log IDs to ease access within class
        self.log_ids = config.log_ids

    def enabled_since(self, trace, event) -> datetime:
        if type(trace) is pd.DataFrame:
            # Calculate for pd.DataFrame
            previous_time = trace[self.log_ids.end_timestamp].where(  # End timestamps of the events
                (trace[self.log_ids.end_timestamp] < event[self.log_ids.end_timestamp]) &  # previous to the current one
                ((~self.config.consider_parallelism) or (trace[self.log_ids.end_timestamp] < event[self.log_ids.start_timestamp])) &  # not overlapping (if parallel check is up)
                (~trace[self.log_ids.activity].isin(self.concurrency[event[self.log_ids.activity]]))  # with no concurrency
            ).max()  # and keeping only the last (highest) one
            if pd.isnull(previous_time):
                # It is the first event of the trace, or all the previous events where concurrent to it
                previous_time = self.config.non_estimated_time
        else:
            # Calculate for pm4py.EventLog
            previous_time = next(
                (previous_event[self.log_ids.end_timestamp] for previous_event in reversed(trace) if (  # End timestamps of the events
                        previous_event[self.log_ids.end_timestamp] < event[self.log_ids.end_timestamp] and  # previous to the current one
                        ((not self.config.consider_parallelism) or previous_event[self.log_ids.end_timestamp] < event[self.log_ids.start_timestamp]) and  # not overlapping (if parallel check is up)
                        previous_event[self.log_ids.activity] not in self.concurrency[event[self.log_ids.activity]]  # with no concurrency
                )),
                self.config.non_estimated_time
            )
        # Return calculated value
        return previous_time


class DeactivatedConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, config: Configuration):
        # Super (with empty concurrency)
        super(DeactivatedConcurrencyOracle, self).__init__({}, config)

    def enabled_since(self, trace, event) -> datetime:
        return self.config.non_estimated_time


class NoConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log: Union[EventLog, pd.DataFrame], config):
        # Default with no concurrency (all directly-follows relations)
        activities = get_activities(event_log, config.log_ids)
        concurrency = {activity: set() for activity in activities}
        # Super
        super(NoConcurrencyOracle, self).__init__(concurrency, config)


class AlphaConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log: Union[EventLog, pd.DataFrame], config: Configuration):
        # Alpha concurrency
        # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
        df_relations = _get_df_relations(event_log, config.log_ids)
        # Create concurrency if there is a directly-follows relation in both directions
        concurrency = {}
        activities = get_activities(event_log, config.log_ids)
        for act_a in activities:
            concurrency[act_a] = set()
            for act_b in activities:
                if act_a != act_b and act_a in df_relations.get(act_b, []) and act_b in df_relations.get(act_a, []):
                    # Concurrency relation AB, add it to A
                    concurrency[act_a].add(act_b)
        # Super
        super(AlphaConcurrencyOracle, self).__init__(concurrency, config)


def _get_df_relations(event_log: Union[EventLog, pd.DataFrame], log_ids: EventLogIDs) -> dict:
    if type(event_log) is pd.DataFrame:
        df_relations = _get_df_relations_df(event_log, log_ids)
    else:
        df_relations = _get_df_relations_el(event_log, log_ids)
    return df_relations


def _get_df_relations_df(event_log: pd.DataFrame, log_ids: EventLogIDs) -> dict:
    # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
    df_relations = {activity: {} for activity in event_log[log_ids.activity].unique()}
    # Fill dictionary with directly-follows relations
    for (key, trace) in event_log.groupby([log_ids.case]):
        for (i, current_event), (j, future_event) in zip_with_next(trace.iterrows()):
            current_activity = current_event[log_ids.activity]
            future_activity = future_event[log_ids.activity]
            # Store df relation
            df_relations[current_activity][future_activity] = df_relations[current_activity].get(future_activity, 0) + 1
    return df_relations


def _get_df_relations_el(event_log: EventLog, log_ids: EventLogIDs) -> dict:
    # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
    df_relations = {}
    # Fill dictionary with directly-follows relations
    for trace in event_log:
        previous_activity = None
        for event in trace:
            activity = event[log_ids.activity]
            # Skip the first event of the trace
            if previous_activity is not None:
                # Create dict for previous activity if missing
                if previous_activity not in df_relations:
                    df_relations[previous_activity] = {}
                # Store df relation
                df_relations[previous_activity][activity] = df_relations[previous_activity].get(activity, 0) + 1
            previous_activity = activity
    return df_relations


class HeuristicsConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log: Union[EventLog, pd.DataFrame], config: Configuration):
        # Heuristics concurrency
        activities = get_activities(event_log, config.log_ids)
        # Get matrices for:
        # - Directly-follows relations: df_count[A][B] = number of times B following A
        # - Directly-follows dependency values: df_dependency[A][B] = value of certainty that there is a df-relation between A and B
        # - Length-2 loop values: l2l_dependency[A][B] = value of certainty that there is a l2l relation between A and B (A-B-A)
        (df_count, df_dependency, l2l_dependency) = _get_heuristics_matrices(event_log, activities, config)
        # Create concurrency if there is a directly-follows relation in both directions
        concurrency = {}
        for act_a in activities:
            concurrency[act_a] = set()
            for act_b in activities:
                if (act_a != act_b and  # They are not the same activity
                        df_count[act_a].get(act_b, 0) > 0 and  # 'B' follows 'A' at least once
                        df_count[act_b].get(act_a, 0) > 0 and  # 'A' follows 'B' at least once
                        l2l_dependency[act_a].get(act_b, 0) < config.heuristics_thresholds.l2l and  # 'A' and 'B' are not a length 2 loop
                        abs(df_dependency[act_a].get(act_b, 0)) < config.heuristics_thresholds.df):  # The df relations are weak
                    # Concurrency relation AB, add it to A
                    concurrency[act_a].add(act_b)
        # Super
        super(HeuristicsConcurrencyOracle, self).__init__(concurrency, config)


def _get_heuristics_matrices(event_log: Union[EventLog, pd.DataFrame], activities: list, config: Configuration) -> (dict, dict, dict):
    # Get directly-follows and length-2-loop count
    if type(event_log) is pd.DataFrame:
        (df_count, l2l_count) = _get_heuristics_matrices_df(event_log, activities, config.log_ids)
    else:
        (df_count, l2l_count) = _get_heuristics_matrices_el(event_log, activities, config.log_ids)
    # Initialize dependency matrices
    df_dependency = {activity: {} for activity in activities}
    l1l_dependency = {activity: 0 for activity in activities}
    l2l_dependency = {activity: {} for activity in activities}
    # Fill dependency matrices
    for act_a in activities:
        for act_b in activities:
            if act_a != act_b:
                # Process directly follows dependency value A -> B
                ab = df_count[act_a].get(act_b, 0)
                ba = df_count[act_b].get(act_a, 0)
                df_dependency[act_a][act_b] = (ab - ba) / (ab + ba + 1)
            else:
                # Process length 1 loop value
                aa = df_count[act_a].get(act_a, 0)
                l1l_dependency[act_a] = aa / (aa + 1)
    for act_a in activities:
        for act_b in activities:
            if act_a != act_b and \
                    l1l_dependency[act_a] < config.heuristics_thresholds.l1l and \
                    l1l_dependency[act_b] < config.heuristics_thresholds.l1l:
                # Process directly follows dependency value A -> B
                aba = l2l_count[act_a].get(act_b, 0)
                bab = l2l_count[act_b].get(act_a, 0)
                l2l_dependency[act_a][act_b] = (aba + bab) / (aba + bab + 1)
            else:
                l2l_dependency[act_a][act_b] = 0
    # Return matrices with dependency values
    return df_count, df_dependency, l2l_dependency


def _get_heuristics_matrices_df(event_log: pd.DataFrame, activities: list, log_ids: EventLogIDs) -> (dict, dict):
    # Initialize dictionary for directly-follows relations df_count[A][B] = number of times B following A
    df_count = {activity: {} for activity in activities}
    # Initialize dictionary for length 2 loops
    l2l_count = {activity: {} for activity in activities}
    # Count directly-follows and l2l relations
    for (key, trace) in event_log.groupby([log_ids.case]):
        previous_activity = None
        # Iterate the events of the trace in pairs: (e1, e2), (e2, e3), (e3, e4)...
        for (i, current_event), (j, future_event) in zip_with_next(trace.iterrows()):
            current_activity = current_event[log_ids.activity]
            future_activity = future_event[log_ids.activity]
            # Store df relation
            df_count[current_activity][future_activity] = df_count[current_activity].get(future_activity, 0) + 1
            # Process l2l
            if previous_activity:
                # Increase value if there is a length 2 loop (A-B-A)
                if previous_activity == future_activity:
                    l2l_count[previous_activity][current_activity] = l2l_count[previous_activity].get(current_activity, 0) + 1
            # Save previous activity
            previous_activity = current_activity
    # Return directly-follows count and length-2-loop count
    return df_count, l2l_count


def _get_heuristics_matrices_el(event_log: EventLog, activities: list, log_ids: EventLogIDs) -> (dict, dict):
    # Initialize dictionary for directly-follows relations df_count[A][B] = number of times B following A
    df_count = {activity: {} for activity in activities}
    # Initialize dictionary for length 2 loops
    l2l_count = {activity: {} for activity in activities}
    # Count directly-follows and l2l relations
    for trace in event_log:
        pre_previous_activity = None
        previous_activity = None
        for event in trace:
            current_activity = event[log_ids.activity]
            if previous_activity:
                # Store df relation
                df_count[previous_activity][current_activity] = df_count[previous_activity].get(current_activity, 0) + 1
                # Process l2l
                if pre_previous_activity:
                    # Increase value if there is a length 2 loop (A-B-A)
                    if pre_previous_activity == current_activity:
                        l2l_count[pre_previous_activity][previous_activity] = l2l_count[pre_previous_activity].get(previous_activity, 0) + 1
            # Save previous activities
            pre_previous_activity = previous_activity
            previous_activity = current_activity
    # Return directly-follows count and length-2-loop count
    return df_count, l2l_count
