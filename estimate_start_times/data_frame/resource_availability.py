from resource_availability import ResourceAvailability


class SimpleResourceAvailability(ResourceAvailability):
    def __init__(self, event_log, config):
        # Create a dictionary with the resources as key and all its events as value
        resources = {str(i) for i in event_log[config.log_ids.resource].unique()}
        resources_calendar = {}
        for resource in (resources - config.bot_resources):
            resource_events = event_log[event_log[config.log_ids.resource] == resource]
            resources_calendar[resource] = sorted(resource_events[config.log_ids.end_timestamp])
        # Super
        super(SimpleResourceAvailability, self).__init__(resources_calendar, config)
