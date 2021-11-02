from datetime import datetime


class ResourceAvailability:
    def __init__(self, event_log, config):
        # Configuration parameters
        self.config = config
        # Create a dictionary with the resources as key and all its events as value
        resources = [str(i) for i in event_log[self.config.log_ids.resource].unique()]
        self.resources_calendar = {}
        for resource in resources:
            resource_events = event_log[event_log[self.config.log_ids.resource] == resource]
            self.resources_calendar[resource] = sorted(resource_events[self.config.log_ids.end_timestamp])

    def available_since(self, resource, timestamp) -> datetime:
        if resource == self.config.missing_resource:
            # If the resource is missing return [non_estimated_time]
            timestamp_previous_event = self.config.non_estimated_time
        else:
            # If not, take the first timestamp previous to [timestamp]
            resource_calendar = self.resources_calendar[resource]
            timestamp_previous_event = next((t for t in reversed(resource_calendar) if t < timestamp), self.config.non_estimated_time)
        return timestamp_previous_event
