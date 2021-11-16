from datetime import datetime

import pandas as pd

from common import zip_with_next


class ConcurrencyOracle:
    def __init__(self, concurrency, config):
        # Dict with the concurrency: self.concurrency[actA] = list of activities concurrent with A
        self.concurrency = concurrency
        # Configuration parameters
        self.config = config

    def enabled_since(self, trace, event) -> datetime:
        previous_time = trace[self.config.log_ids.end_timestamp].where(  # End timestamps of the events
            (trace[self.config.log_ids.end_timestamp] < event[self.config.log_ids.end_timestamp]) &  # which are previous to the current one
            (~trace[self.config.log_ids.activity].isin(self.concurrency[event[self.config.log_ids.activity]]))  # with no concurrency
        ).max()  # and keeping only the last (highest) one
        if pd.isnull(previous_time):
            # First event of the trace, or the previous events where all concurrent
            previous_time = self.config.non_estimated_time
        return previous_time


class NoConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log, config):
        # Default with no concurrency
        activities = event_log[config.log_ids.activity].unique()
        concurrency = {activity: [] for activity in activities}
        # Super
        super(NoConcurrencyOracle, self).__init__(concurrency, config)


class AlphaConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log, config):
        # Alpha concurrency
        # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
        df_relations = _get_df_relations(event_log, config)
        # Create concurrency if there is a directly-follows relation in both directions
        concurrency = {}
        activities = event_log[config.log_ids.activity].unique()
        for actA in activities:
            concurrency[actA] = []
            for actB in activities:
                if actA != actB and actA in df_relations.get(actB, []) and actB in df_relations.get(actA, []):
                    # Concurrency relation AB, add it to A
                    concurrency[actA] += [actB]
        # Super
        super(AlphaConcurrencyOracle, self).__init__(concurrency, config)


def _get_df_relations(event_log, config) -> dict:
    # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
    df_relations = {}
    # Fill dictionary with directly-follows relations
    for (key, trace) in event_log.groupby([config.log_ids.case]):
        for (i, current_event), (j, future_event) in zip_with_next(trace.iterrows()):
            current_activity = current_event[config.log_ids.activity]
            future_activity = future_event[config.log_ids.activity]
            # Create dict for previous activity if missing
            if current_activity not in df_relations:
                df_relations[current_activity] = {}
            # Store df relation
            df_relations[current_activity][future_activity] = df_relations[current_activity].get(future_activity, 0) + 1
    return df_relations
