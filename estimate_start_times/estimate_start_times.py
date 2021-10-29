#
# Author: David Chapela de la Campa
#
from pm4py.objects.log.obj import EventLog


def estimate_start_timestamps(event_log, concurrency_oracle, resource_availability) -> EventLog:
    # Assign start timestamps
    i = 0
    print("Starting inference")
    for trace in event_log:
        i += 1
        if i % 100 == 0:
            print(i)
        for event in trace:
            enabled_time = concurrency_oracle.enabled_since(trace, event)
            available_time = resource_availability.available_since(event['org:resource'], event['time:timestamp'])
            event['start:timestamp'] = max(
                enabled_time,
                available_time
            )
    # Fix start times for those events being the first one of the trace and the resource (with initial_time)
    return event_log
