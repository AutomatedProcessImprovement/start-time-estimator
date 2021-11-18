import pm4py
from pm4py.algo.filtering.log.attributes import attributes_filter

from resource_availability import ResourceAvailability


class SimpleResourceAvailability(ResourceAvailability):
    def __init__(self, event_log, config):
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
        super(SimpleResourceAvailability, self).__init__(resources_calendar, config)
