from statistics import mode

import numpy as np
import pandas as pd

from common import FixMethod


def estimate_start_timestamps(event_log, concurrency_oracle, resource_availability, config) -> pd.DataFrame:
    # If there is not column for start timestamp, create it
    if config.log_ids.start_timestamp not in event_log.columns:
        event_log[config.log_ids.start_timestamp] = pd.NaT
    # Assign start timestamps
    for (key, trace) in event_log.groupby([config.log_ids.case]):
        for index, event in trace.iterrows():
            enabled_time = concurrency_oracle.enabled_since(trace, event)
            available_time = resource_availability.available_since(event[config.log_ids.resource], event[config.log_ids.end_timestamp])
            event_log.loc[index, config.log_ids.start_timestamp] = max(
                enabled_time,
                available_time
            )
    # Fix start times for those events being the first one of the trace and the resource (with non_estimated_time)
    if config.fix_method == FixMethod.SET_INSTANT:
        estimated_event_log = set_instant_non_estimated_start_times(event_log, config)
    elif config.fix_method == FixMethod.RE_ESTIMATE:
        estimated_event_log = re_estimate_non_estimated_start_times(event_log, config)
    else:
        print("Unselected fix method for events with no estimated start time! Setting them as instant by default.")
        estimated_event_log = set_instant_non_estimated_start_times(event_log, config)
    # Return modified event log
    return estimated_event_log


def set_instant_non_estimated_start_times(event_log, config) -> pd.DataFrame:
    # Identify events with non_estimated as start time
    # and set their processing time to instant
    event_log[config.log_ids.start_timestamp] = np.where(
        event_log[config.log_ids.start_timestamp] == config.non_estimated_time,
        event_log[config.log_ids.end_timestamp],
        event_log[config.log_ids.start_timestamp]
    )
    # Return modified event log
    return event_log


def re_estimate_non_estimated_start_times(event_log, config) -> pd.DataFrame:
    # Store the durations of the estimated ones
    activity_processing_times = event_log[event_log[config.log_ids.start_timestamp] != config.non_estimated_time] \
        .groupby([config.log_ids.activity]) \
        .apply(lambda row: row[config.log_ids.end_timestamp] - row[config.log_ids.start_timestamp])
    # Identify events with non_estimated as start time
    non_estimated_events = event_log[event_log[config.log_ids.start_timestamp] == config.non_estimated_time]
    for index, non_estimated_event in non_estimated_events.iterrows():
        activity = non_estimated_event[config.log_ids.activity]
        if activity in activity_processing_times:
            event_log.loc[index, config.log_ids.start_timestamp] = \
                non_estimated_event[config.log_ids.end_timestamp] - mode(activity_processing_times[activity])
        else:
            # If this activity has no estimated times set as instant activity
            event_log.loc[index, config.log_ids.start_timestamp] = event_log.loc[index, config.log_ids.end_timestamp]
    # Return modified event log
    return event_log
