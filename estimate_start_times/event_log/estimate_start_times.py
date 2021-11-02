from statistics import mode

from pm4py.objects.log.obj import EventLog

from config import ReEstimationMethod


def estimate_start_timestamps(event_log, concurrency_oracle, resource_availability, config) -> EventLog:
    # Assign start timestamps
    for trace in event_log:
        for event in trace:
            enabled_time = concurrency_oracle.enabled_since(trace, event)
            available_time = resource_availability.available_since(event[config.log_ids.resource], event[config.log_ids.end_timestamp])
            event[config.log_ids.start_timestamp] = max(
                enabled_time,
                available_time
            )
    # Fix start times for those events being the first one of the trace and the resource (with non_estimated_time)
    if config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
        estimated_event_log = set_instant_non_estimated_start_times(event_log, config)
    elif config.re_estimation_method == ReEstimationMethod.MODE:
        estimated_event_log = re_estimate_non_estimated_start_times(event_log, config)
    else:
        print("Unselected fix method for events with no estimated start time! Setting them as instant by default.")
        estimated_event_log = set_instant_non_estimated_start_times(event_log, config)
    # Return modified event log
    return estimated_event_log


def set_instant_non_estimated_start_times(event_log, config) -> EventLog:
    # Identify events with non_estimated as start time
    # and set their processing time to instant
    for trace in event_log:
        for event in trace:
            if event[config.log_ids.start_timestamp] == config.non_estimated_time:
                # Non-estimated, save event to estimate based on statistics
                event[config.log_ids.start_timestamp] = event[config.log_ids.end_timestamp]
    # Return modified event log
    return event_log


def re_estimate_non_estimated_start_times(event_log, config) -> EventLog:
    # Identify events with non_estimated as start time
    # and store the durations of the estimated ones
    non_estimated_events = []
    activity_times = {}
    for trace in event_log:
        for event in trace:
            if event[config.log_ids.start_timestamp] == config.non_estimated_time:
                # Non-estimated, save event to estimate based on statistics
                non_estimated_events += [event]
            else:
                # Estimated, store estimated time to calculate statistics
                activity = event[config.log_ids.activity]
                processing_time = event[config.log_ids.end_timestamp] - event[config.log_ids.start_timestamp]
                if activity not in activity_times:
                    activity_times[activity] = [processing_time]
                else:
                    activity_times[activity] += [processing_time]
    # Set as start time the end time - the mode of the processing times (most frequent processing time)
    for event in non_estimated_events:
        activity = event[config.log_ids.activity]
        if activity in activity_times:
            event[config.log_ids.start_timestamp] = event[config.log_ids.end_timestamp] - mode(activity_times[activity])
        else:
            # If this activity has no estimated times set as instant activity
            event[config.log_ids.start_timestamp] = event[config.log_ids.end_timestamp]
    # Return modified event log
    return event_log
