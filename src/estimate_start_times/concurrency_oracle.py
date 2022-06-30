from datetime import datetime

import pandas as pd

from estimate_start_times.config import EventLogIDs, Configuration
from estimate_start_times.utils import zip_with_next


class ConcurrencyOracle:
    def __init__(self, concurrency: dict, config: Configuration):
        # Dict with the concurrency: self.concurrency[A] = set of activities concurrent with A
        self.concurrency = concurrency
        # Configuration parameters
        self.config = config
        # Set log IDs to ease access within class
        self.log_ids = config.log_ids

    def enabled_since(self, trace, event) -> datetime:
        # Calculate for pd.DataFrame
        previous_time = trace[self.log_ids.end_time].where(  # End timestamps of the events:
            (trace[self.log_ids.end_time] < event[self.log_ids.end_time]) &  # i) previous to the current one;
            ((not self.config.consider_start_times) or  # ii) if parallel check is activated,
             (trace[self.log_ids.end_time] <= event[self.log_ids.start_time])) &  # not overlapping;
            (~trace[self.log_ids.activity].isin(self.concurrency[event[self.log_ids.activity]]))  # iii) with no concurrency;
        ).max()  # keeping only the last (highest) one
        if pd.isnull(previous_time):
            # It is the first event of the trace, or all the previous events where concurrent to it
            previous_time = pd.NaT
        # Return calculated value
        return previous_time

    def add_enabled_times(self, event_log: pd.DataFrame, set_nat_to_first_event: bool = False):
        """
        Add the enabled time of each activity instance to the received event log based on the concurrency relations established in the
        class instance (extracted from the event log passed to the instantiation). For the first event on each trace, set the start of the
        trace as value.

        :param event_log:               event log to add the enabled time information to.
        :param set_nat_to_first_event:  if False, use the start of the trace as enabled time for the activity instances with no previous
                                        activity enabling them, otherwise use pd.NaT.
        """
        # For each trace in the log, estimate the enabled time of its events
        indexes = []
        enabled_times = []
        for (case_id, trace) in event_log.groupby([self.log_ids.case]):
            if self.log_ids.start_time in trace:
                # If the log has start times, take the first start/end as start of the trace
                trace_start_time = min(trace[self.log_ids.start_time].min(), trace[self.log_ids.end_time].min())
            else:
                # If not, take the first end
                trace_start_time = trace[self.log_ids.end_time].min()
            # Get the enabled times
            for index, event in trace.iterrows():
                indexes += [index]
                enabled_time = self.enabled_since(trace, event)
                if set_nat_to_first_event or not pd.isna(enabled_time):
                    # Use computed value
                    enabled_times += [enabled_time]
                else:
                    # Use the trace start for activity instances with no previous activity enabling them
                    enabled_times += [trace_start_time]
        # Set all trace enabled times at once
        event_log.loc[indexes, self.log_ids.enabled_time] = enabled_times
        event_log[self.log_ids.enabled_time] = pd.to_datetime(event_log[self.log_ids.enabled_time], utc=True)


class DeactivatedConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, config: Configuration):
        # Super (with empty concurrency)
        super(DeactivatedConcurrencyOracle, self).__init__({}, config)

    def enabled_since(self, trace, event) -> datetime:
        return pd.NaT


class NoConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log: pd.DataFrame, config):
        # Default with no concurrency (all directly-follows relations)
        activities = event_log[config.log_ids.activity].unique()
        concurrency = {activity: set() for activity in activities}
        # Super
        super(NoConcurrencyOracle, self).__init__(concurrency, config)


class AlphaConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Alpha concurrency
        # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
        df_relations = _get_df_relations(event_log, config.log_ids)
        # Create concurrency if there is a directly-follows relation in both directions
        concurrency = {}
        activities = event_log[config.log_ids.activity].unique()
        for act_a in activities:
            concurrency[act_a] = set()
            for act_b in activities:
                if act_a != act_b and act_a in df_relations.get(act_b, []) and act_b in df_relations.get(act_a, []):
                    # Concurrency relation AB, add it to A
                    concurrency[act_a].add(act_b)
        # Super
        super(AlphaConcurrencyOracle, self).__init__(concurrency, config)


def _get_df_relations(event_log: pd.DataFrame, log_ids: EventLogIDs) -> dict:
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


class HeuristicsConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Heuristics concurrency
        activities = event_log[config.log_ids.activity].unique()
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


def _get_heuristics_matrices(event_log: pd.DataFrame, activities: list, config: Configuration) -> (dict, dict, dict):
    # Initialize dictionary for directly-follows relations df_count[A][B] = number of times B following A
    df_count = {activity: {} for activity in activities}
    # Initialize dictionary for length 2 loops
    l2l_count = {activity: {} for activity in activities}
    # Count directly-follows and l2l relations
    for (key, trace) in event_log.groupby([config.log_ids.case]):
        previous_activity = None
        # Iterate the events of the trace in pairs: (e1, e2), (e2, e3), (e3, e4)...
        for (i, current_event), (j, future_event) in zip_with_next(trace.iterrows()):
            current_activity = current_event[config.log_ids.activity]
            future_activity = future_event[config.log_ids.activity]
            # Store df relation
            df_count[current_activity][future_activity] = df_count[current_activity].get(future_activity, 0) + 1
            # Process l2l
            if previous_activity:
                # Increase value if there is a length 2 loop (A-B-A)
                if previous_activity == future_activity:
                    l2l_count[previous_activity][current_activity] = l2l_count[previous_activity].get(current_activity, 0) + 1
            # Save previous activity
            previous_activity = current_activity
    # Fill df and l1l dependency matrices
    df_dependency = {activity: {} for activity in activities}
    l1l_dependency = {activity: 0 for activity in activities}
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
    # Fill l2l dependency matrix
    l2l_dependency = {activity: {} for activity in activities}
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
