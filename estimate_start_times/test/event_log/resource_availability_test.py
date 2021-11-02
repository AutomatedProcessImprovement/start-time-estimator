from datetime import datetime

from config import Configuration, DEFAULT_XES_IDS
from event_log.resource_availability import ResourceAvailability
from event_log_readers import read_xes_log


def test_resource_availability():
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    event_log = read_xes_log('../assets/test_event_log_1.xes', config)
    resource_availability = ResourceAvailability(event_log, config)
    # The configuration for the algorithm is the passed
    assert resource_availability.config == config
    # All the resources have been loaded
    assert set(resource_availability.resources_calendar.keys()) == {'Marcus', 'Dominic', 'Anya'}
    # The availability of the resource is the timestamp of its previous executed event
    assert resource_availability.available_since(
        'Marcus',
        event_log[2][4][config.log_ids.end_timestamp]
    ) == event_log[0][4][config.log_ids.end_timestamp]
    # The availability of the resource is the timestamp of its previous executed event
    assert resource_availability.available_since(
        'Dominic',
        datetime.fromisoformat('2006-11-07T17:00:00.000+02:00')
    ) == event_log[0][2][config.log_ids.end_timestamp]
    # The availability of the resource is the [non_estimated_time] for the first event of the resource
    assert resource_availability.available_since(
        'Anya',
        event_log[3][0][config.log_ids.end_timestamp]
    ) == config.non_estimated_time
    # The missing resource is always available since [non_estimated_time]
    assert resource_availability.available_since(
        config.missing_resource,
        datetime.fromisoformat('2006-11-07T10:00:00.000+02:00')
    ) == config.non_estimated_time
    # The missing resource is always available since [non_estimated_time]
    assert resource_availability.available_since(
        config.missing_resource,
        datetime.fromisoformat('2006-11-09T10:00:00.000+02:00')
    ) == config.non_estimated_time
