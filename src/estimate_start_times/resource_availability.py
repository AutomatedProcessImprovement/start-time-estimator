from datetime import datetime

import pandas as pd

from estimate_start_times.config import Configuration
from pix_utils.calendar.resource_calendar import get_last_available_timestamp


class ResourceAvailability:
    def __init__(self, resources_calendar: dict, config: Configuration):
        # Store dictionary with the resources as key and all its events as value
        self.resources_calendar = resources_calendar
        # Configuration parameters
        self.config = config
        # Set log IDs to ease access within class
        self.log_ids = config.log_ids

    def available_since(self, resource: str, event) -> datetime:
        # Get the end timestamp of the previous activity instance executed by the same resource
        if resource == self.config.missing_resource:
            # If the resource is missing return pd.NaT
            timestamp_previous_event = pd.NaT
        elif resource in self.config.bot_resources:
            # If the resource has been marked as 'bot resource', return the same timestamp
            timestamp_previous_event = event[self.log_ids.end_time]
        else:
            # If existing resource, take the latest timestamp previous to [timestamp]
            resource_calendar = self.resources_calendar[resource]
            timestamp_previous_event = resource_calendar.where(
                (resource_calendar < event[self.log_ids.end_time]) &
                ((not self.config.consider_start_times) or (resource_calendar <= event[self.log_ids.start_time]))
            ).max()
            if pd.isna(timestamp_previous_event):
                timestamp_previous_event = pd.NaT
            # If there are non-working periods from the latest previous
            # end timestamp, take the end of the last non-working period
            if resource in self.config.working_schedules:
                activity_start = event[self.log_ids.start_time] if self.config.consider_start_times else event[self.log_ids.end_time]
                timestamp_previous_event = get_last_available_timestamp(
                    start=timestamp_previous_event,
                    end=activity_start,
                    schedule=self.config.working_schedules[resource]
                )
        # Return previous timestamp where the resource became available
        return timestamp_previous_event

    def add_resource_availability_times(self, event_log: pd.DataFrame):
        """
        Add the resource availability time of each activity instance to the received event log. For the first event of each resource, set
        pd.NaT.

        :param event_log: event log to add the resource availability time information to.
        """
        # For each trace in the log, estimate the enabled time of its events
        indexes = []
        resource_availability_times = []
        for (case_id, trace) in event_log.groupby([self.log_ids.case]):
            # Get the resource availability times
            for index, event in trace.iterrows():
                indexes += [index]
                resource_availability_time = self.available_since(event[self.log_ids.resource], event)
                resource_availability_times += [resource_availability_time]
        # Set all enabled times at once
        event_log.loc[indexes, self.log_ids.available_time] = resource_availability_times
        event_log[self.log_ids.available_time] = pd.to_datetime(event_log[self.log_ids.available_time], utc=True)


class SimpleResourceAvailability(ResourceAvailability):
    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Create a dictionary with the resources as key and all its end events as value
        resources = {str(i) for i in event_log[config.log_ids.resource].unique()}
        resources_calendar = {}
        for resource in (resources - config.bot_resources):
            resources_calendar[resource] = event_log[event_log[config.log_ids.resource] == resource][config.log_ids.end_time]
        # Super
        super(SimpleResourceAvailability, self).__init__(resources_calendar, config)


class CalendarResourceAvailability(ResourceAvailability):
    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Create a dictionary with the resources as key and all its end events as value
        resources = {str(i) for i in event_log[config.log_ids.resource].unique()}
        resources_calendar = {}
        for resource in (resources - config.bot_resources):
            resources_calendar[resource] = event_log[event_log[config.log_ids.resource] == resource][config.log_ids.end_time]
        # Super
        super(CalendarResourceAvailability, self).__init__(resources_calendar, config)
