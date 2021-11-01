#
# Author: David Chapela de la Campa
#
import enum
from pm4py.objects.log.obj import EventLog
from statistics import mode


class FixMethod(enum.Enum):
    SET_INSTANT = 1
    RE_ESTIMATE = 2


def estimate_start_timestamps(event_log,
                              concurrency_oracle,
                              resource_availability,
                              fix_method=FixMethod.SET_INSTANT) -> EventLog:
    # Assign start timestamps
    for trace in event_log:
        for event in trace:
            enabled_time = concurrency_oracle.enabled_since(trace, event)
            available_time = resource_availability.available_since(event['org:resource'], event['time:timestamp'])
            event['start:timestamp'] = max(
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


def set_instant_non_estimated_start_times(event_log, non_estimated_time) -> EventLog:
    # Identify events with non_estimated as start time
    # and set their processing time to instant
    for trace in event_log:
        for event in trace:
            if event['start:timestamp'] == non_estimated_time:
                # Non-estimated, save event to estimate based on statistics
                event['start:timestamp'] = event['time:timestamp']
    # Return modified event log
    return event_log


def re_estimate_non_estimated_start_times(event_log, non_estimated_time) -> EventLog:
    # Identify events with non_estimated as start time
    # and store the durations of the estimated ones
    non_estimated_events = []
    activity_times = {}
    for trace in event_log:
        for event in trace:
            if event['start:timestamp'] == non_estimated_time:
                # Non-estimated, save event to estimate based on statistics
                non_estimated_events += [event]
            else:
                # Estimated, store estimated time to calculate statistics
                activity = event['concept:name']
                processing_time = event['time:timestamp'] - event['start:timestamp']
                if activity not in activity_times:
                    activity_times[activity] = [processing_time]
                else:
                    activity_times[activity] += [processing_time]
    # Set as start time the end time - the mode of the processing times (most frequent processing time)
    for event in non_estimated_events:
        activity = event['concept:name']
        if activity in activity_times:
            event['start:timestamp'] = event['time:timestamp'] - mode(activity_times[activity])
        else:
            # If this activity has no estimated times set as instant activity
            event['start:timestamp'] = event['time:timestamp']
    # Return modified event log
    return event_log
