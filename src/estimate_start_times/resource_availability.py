from datetime import datetime

import pandas as pd

from estimate_start_times.config import Configuration


class ResourceAvailability:
    def __init__(self, resources_calendar: dict, config: Configuration):
        # Store dictionary with the resources as key and all its events as value
        self.resources_calendar = resources_calendar
        # Configuration parameters
        self.config = config

    def available_since(self, resource: str, event) -> datetime:
        if resource == self.config.missing_resource:
            # If the resource is missing return [non_estimated_time]
            timestamp_previous_event = self.config.non_estimated_time
        elif resource in self.config.bot_resources:
            # If the resource has been marked as 'bot resource', return the same timestamp
            timestamp_previous_event = event[self.config.log_ids.end_time]
        else:
            # If not, take the first timestamp previous to [timestamp]
            resource_calendar = self.resources_calendar[resource]
            timestamp_previous_event = resource_calendar.where(
                (resource_calendar < event[self.config.log_ids.end_time]) &
                ((not self.config.consider_start_times) or (resource_calendar <= event[self.config.log_ids.start_time]))
            ).max()
            if pd.isna(timestamp_previous_event):
                timestamp_previous_event = self.config.non_estimated_time
        return timestamp_previous_event


class SimpleResourceAvailability(ResourceAvailability):
    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Create a dictionary with the resources as key and all its events as value
        resources = {str(i) for i in event_log[config.log_ids.resource].unique()}
        resources_calendar = {}
        for resource in (resources - config.bot_resources):
            resources_calendar[resource] = event_log[event_log[config.log_ids.resource] == resource][config.log_ids.end_time]
        # Super
        super(SimpleResourceAvailability, self).__init__(resources_calendar, config)
