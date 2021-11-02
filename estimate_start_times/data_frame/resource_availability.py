import pm4py
from datetime import datetime
from pm4py.algo.filtering.log.attributes import attributes_filter


class ResourceAvailability:
    def __init__(self, event_log, initial_time, missing_resource):
        # Set initial time to return as default
        self.initial_time = initial_time
        # Set ID for missing resource
        self.missing_resource = missing_resource
        # Create a dictionary with the resources as key and all its events as value
        resources = [str(i) for i in event_log['resource'].unique()]
        self.resources_calendar = {}
        for resource in resources:
            resource_events = event_log[event_log['resource'] == resource]
            self.resources_calendar[resource] = sorted(resource_events['end_timestamp'])

    def available_since(self, resource, timestamp) -> datetime:
        if resource == self.missing_resource:
            # If the resource is missing return [initial_time]
            timestamp_previous_event = self.initial_time
        else:
            # If not, take the first timestamp previous to [timestamp]
            resource_calendar = self.resources_calendar[resource]
            timestamp_previous_event = next(
                (t for t in reversed(resource_calendar) if t < timestamp),
                self.initial_time
            )
        return timestamp_previous_event
