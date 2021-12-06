from datetime import datetime
from typing import Union

import pandas as pd
import pm4py
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.objects.log.obj import EventLog

from config import Configuration


class ResourceAvailability:
    def __init__(self, resources_calendar: dict, config: Configuration):
        # Store dictionary with the resources as key and all its events as value
        self.resources_calendar = resources_calendar
        # Configuration parameters
        self.config = config

    def available_since(self, resource: str, timestamp: datetime) -> datetime:
        if resource == self.config.missing_resource:
            # If the resource is missing return [non_estimated_time]
            timestamp_previous_event = self.config.non_estimated_time
        elif resource in self.config.bot_resources:
            # If the resource has been marked as 'bot resource', return the same timestamp
            timestamp_previous_event = timestamp
        else:
            # If not, take the first timestamp previous to [timestamp]
            resource_calendar = self.resources_calendar[resource]
            if type(resource_calendar) is pd.Series:
                timestamp_previous_event = resource_calendar.where(resource_calendar < timestamp).max()
                if pd.isna(timestamp_previous_event):
                    timestamp_previous_event = self.config.non_estimated_time
            else:
                timestamp_previous_event = next((t for t in reversed(resource_calendar) if t < timestamp), self.config.non_estimated_time)
        return timestamp_previous_event


class SimpleResourceAvailability(ResourceAvailability):
    def __init__(self, event_log: Union[EventLog, pd.DataFrame], config: Configuration):
        # Create a dictionary with the resources as key and all its events as value
        resources_calendar = _get_simple_resources_calendar(event_log, config)
        # Super
        super(SimpleResourceAvailability, self).__init__(resources_calendar, config)


def _get_simple_resources_calendar(event_log: Union[EventLog, pd.DataFrame], config: Configuration):
    if type(event_log) is pd.DataFrame:
        resources_calendar = _get_simple_resources_calendar_df(event_log, config)
    else:
        resources_calendar = _get_simple_resources_calendar_el(event_log, config)
    return resources_calendar


def _get_simple_resources_calendar_df(event_log: pd.DataFrame, config: Configuration):
    # Create a dictionary with the resources as key and all its events as value
    resources = {str(i) for i in event_log[config.log_ids.resource].unique()}
    resources_calendar = {}
    for resource in (resources - config.bot_resources):
        resources_calendar[resource] = event_log[event_log[config.log_ids.resource] == resource][config.log_ids.end_timestamp]
    # Return resources calendar
    return resources_calendar


def _get_simple_resources_calendar_el(event_log: EventLog, config: Configuration):
    # Create a dictionary with the resources as key and all its events as value
    resources = {str(i) for i in attributes_filter.get_attribute_values(event_log, config.log_ids.resource).keys()}
    resources_calendar = {}
    for resource in (resources - config.bot_resources):
        resource_events = pm4py.convert_to_event_stream(
            attributes_filter.apply_events(
                event_log,
                [resource],
                {attributes_filter.Parameters.ATTRIBUTE_KEY: config.log_ids.resource,
                 attributes_filter.Parameters.POSITIVE: True}
            )
        )
        resources_calendar[resource] = sorted(event[config.log_ids.end_timestamp] for event in resource_events)
    # Super
    return resources_calendar
