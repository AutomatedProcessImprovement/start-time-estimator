from datetime import datetime

import pm4py
from pm4py.algo.filtering.log.attributes import attributes_filter


class ResourceAvailability:
    def __init__(self, event_log, config):
        # Configuration parameters
        self.config = config
        # Create a dictionary with the resources as key and all its events as value
        resources = {str(i) for i in attributes_filter.get_attribute_values(event_log, config.log_ids.resource).keys()}
        self.resources_calendar = {}
        for resource in (resources - config.bot_resources):
            resource_events = pm4py.convert_to_event_stream(
                attributes_filter.apply_events(
                    event_log,
                    [resource],
                    {attributes_filter.Parameters.ATTRIBUTE_KEY: config.log_ids.resource,
                     attributes_filter.Parameters.POSITIVE: True}
                )
            )
            self.resources_calendar[resource] = sorted(event[config.log_ids.end_timestamp] for event in resource_events)

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
