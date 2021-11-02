#
# Author: David Chapela de la Campa
#
import pandas as pd
import numpy as np

from common import FixMethod

from statistics import mode


def estimate_start_timestamps(event_log,
                              concurrency_oracle,
                              resource_availability,
                              fix_method=FixMethod.SET_INSTANT) -> pd.DataFrame:
    # If there is not column for start timestamp, create it
    if 'start_timestamp' not in event_log.columns:
        event_log['start_timestamp'] = pd.NaT
    # Assign start timestamps
    for (key, trace) in event_log.groupby(['caseID']):
        for index, event in trace.iterrows():
            enabled_time = concurrency_oracle.enabled_since(trace, event)
            available_time = resource_availability.available_since(event['resource'], event['end_timestamp'])
            event_log.loc[index, 'start_timestamp'] = max(
                enabled_time,
                available_time
            )
    # Fix start times for those events being the first one of the trace and the resource (with initial_time)
    if fix_method == FixMethod.SET_INSTANT:
        estimated_event_log = set_instant_non_estimated_start_times(event_log, concurrency_oracle.initial_time)
    elif fix_method == FixMethod.RE_ESTIMATE:
        estimated_event_log = re_estimate_non_estimated_start_times(event_log, concurrency_oracle.initial_time)
    else:
        print("Unselected fix method for events with no estimated start time! Setting them as instant by default.")
        estimated_event_log = set_instant_non_estimated_start_times(event_log, concurrency_oracle.initial_time)
    # Return modified event log
    return estimated_event_log


def set_instant_non_estimated_start_times(event_log, non_estimated_time) -> pd.DataFrame:
    # Identify events with non_estimated as start time
    # and set their processing time to instant
    event_log['start_timestamp'] = np.where(
        event_log['start_timestamp'] == non_estimated_time,
        event_log['end_timestamp'],
        event_log['start_timestamp']
    )
    # Return modified event log
    return event_log


def re_estimate_non_estimated_start_times(event_log, non_estimated_time) -> pd.DataFrame:
    # Store the durations of the estimated ones
    activity_processing_times = event_log[event_log['start_timestamp'] != non_estimated_time] \
        .groupby(['activity']) \
        .apply(lambda row: row['end_timestamp'] - row['start_timestamp'])
    # Identify events with non_estimated as start time
    non_estimated_events = event_log[event_log['start_timestamp'] == non_estimated_time]
    for index, non_estimated_event in non_estimated_events.iterrows():
        activity = non_estimated_event['activity']
        if activity in activity_processing_times:
            event_log.loc[index, 'start_timestamp'] = \
                non_estimated_event['end_timestamp'] - mode(activity_processing_times[activity])
        else:
            # If this activity has no estimated times set as instant activity
            event_log.loc[index, 'start_timestamp'] = event_log.loc[index, 'end_timestamp']
    # Return modified event log
    return event_log
