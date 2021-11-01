import itertools
import pandas as pd

from datetime import datetime


class ConcurrencyOracle:
    def __init__(self, concurrency, initial_time):
        # Set initial time to return as default
        self.initial_time = initial_time
        # Dict with the concurrency: self.concurrency[actA] = list of activities concurrent with A
        self.concurrency = concurrency

    def enabled_since(self, trace, event) -> datetime:
        previous_time = trace['end_timestamp'].where(  # End timestamps of the events
            (trace['end_timestamp'] < event['end_timestamp']) &  # previous to the current one
            (~trace['activity'].isin(self.concurrency[event['activity']]))  # an with no concurrent relation
        ).max()  # keeping the last (highest) one
        if pd.isnull(previous_time):
            # First event of the trace, or the previous events where all concurrent
            previous_time = self.initial_time
        return previous_time


class NoConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log, initial_time):
        # Default with no concurrency
        activities = event_log['activity'].unique()
        concurrency = {activity: [] for activity in activities}
        # Super
        super(NoConcurrencyOracle, self).__init__(concurrency, initial_time)


class AlphaConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log, initial_time):
        # Alpha concurrency
        # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
        df_relations = get_df_relations(event_log)
        # Create concurrency if there is a directly-follows relation in both directions
        concurrency = {}
        activities = event_log['activity'].unique()
        for actA in activities:
            concurrency[actA] = []
            for actB in activities:
                if actA != actB and actA in df_relations.get(actB, []) and actB in df_relations.get(actA, []):
                    # Concurrency relation AB, add it to A
                    concurrency[actA] += actB
        # Super
        super(AlphaConcurrencyOracle, self).__init__(concurrency, initial_time)


def get_df_relations(event_log) -> dict:
    # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
    df_relations = {}
    # Fill dictionary with directly-follows relations
    for (key, trace) in event_log.groupby(['caseID']):
        for (i, current_event), (j, future_event) in zip_with_next(trace.iterrows()):
            current_activity = current_event['activity']
            future_activity = future_event['activity']
            # Create dict for previous activity if missing
            if current_activity not in df_relations:
                df_relations[current_activity] = {}
            # Store df relation
            df_relations[current_activity][future_activity] = df_relations[current_activity].get(future_activity, 0) + 1
    return df_relations


def zip_with_next(iterable):
    # s -> (s0,s1), (s1,s2), (s2, s3), ...
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)
