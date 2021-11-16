from datetime import datetime

from pm4py.algo.filtering.log.attributes import attributes_filter


class ConcurrencyOracle:
    def __init__(self, concurrency, config):
        # Dict with the concurrency: self.concurrency[actA] = list of activities concurrent with A
        self.concurrency = concurrency
        # Configuration parameters
        self.config = config

    def enabled_since(self, trace, event) -> datetime:
        return next(
            (previous_event[self.config.log_ids.end_timestamp] for previous_event in reversed(trace) if (
                    previous_event[self.config.log_ids.end_timestamp] < event[self.config.log_ids.end_timestamp] and
                    previous_event[self.config.log_ids.activity] not in self.concurrency[event[self.config.log_ids.activity]]
            )),
            self.config.non_estimated_time
        )


class NoConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log, config):
        # Default with no concurrency
        activities = attributes_filter.get_attribute_values(event_log, config.log_ids.activity)
        concurrency = {activity: [] for activity in activities}
        # Super
        super(NoConcurrencyOracle, self).__init__(concurrency, config)


class AlphaConcurrencyOracle(ConcurrencyOracle):
    def __init__(self, event_log, config):
        # Alpha concurrency
        # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
        df_relations = get_df_relations(event_log, config)
        # Create concurrency if there is a directly-follows relation in both directions
        concurrency = {}
        activities = attributes_filter.get_attribute_values(event_log, config.log_ids.activity).keys()
        for actA in activities:
            concurrency[actA] = []
            for actB in (activities - actA):
                if actA in df_relations.get(actB, []) and actB in df_relations.get(actA, []):
                    # Concurrency relation AB, add it to A
                    concurrency[actA] += [actB]
        # Super
        super(AlphaConcurrencyOracle, self).__init__(concurrency, config)


def get_df_relations(event_log, config) -> dict:
    # Initialize dictionary for directly-follows relations df_relations[A][B] = number of times B following A
    df_relations = {}
    # Fill dictionary with directly-follows relations
    for trace in event_log:
        previous_activity = None
        for event in trace:
            activity = event[config.log_ids.activity]
            # Skip the first event of the trace
            if previous_activity is not None:
                # Create dict for previous activity if missing
                if previous_activity not in df_relations:
                    df_relations[previous_activity] = {}
                # Store df relation
                df_relations[previous_activity][activity] = df_relations[previous_activity].get(activity, 0) + 1
            previous_activity = activity
    return df_relations
