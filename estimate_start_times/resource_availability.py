from datetime import datetime


class ResourceAvailability:
    def __init__(self, resources_calendar, config):
        # Store dictionary with the resources as key and all its events as value
        self.resources_calendar = resources_calendar
        # Configuration parameters
        self.config = config

    def available_since(self, resource, timestamp) -> datetime:
        if resource == self.config.missing_resource:
            # If the resource is missing return [non_estimated_time]
            timestamp_previous_event = self.config.non_estimated_time
        elif resource in self.config.bot_resources:
            # If the resource has been marked as 'bot resource', return the same timestamp
            timestamp_previous_event = timestamp
        else:
            # If not, take the first timestamp previous to [timestamp]
            resource_calendar = self.resources_calendar[resource]
            timestamp_previous_event = next((t for t in reversed(resource_calendar) if t < timestamp), self.config.non_estimated_time)
        return timestamp_previous_event
