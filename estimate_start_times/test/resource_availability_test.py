import pm4py
import pytz
from datetime import datetime
from estimate_start_times.resource_availability import ResourceAvailability


def test_resource_availability():
    initial_time = datetime.min.replace(tzinfo=pytz.UTC)
    missing_resource = 'missing_resource'
    event_log = pm4py.read_xes('.\\event_logs\\test_event_log_1.xes')
    resource_availability = ResourceAvailability(event_log, initial_time, missing_resource)
    # The initial time to use as default is the passed
    assert resource_availability.initial_time == initial_time
    # All the resources have been loaded
    assert set(resource_availability.resources_calendar.keys()) == {'Marcus', 'Dominic', 'Anya'}
    # The availability of the resource is the timestamp of its previous executed event
    assert resource_availability.available_since(
        'Marcus',
        event_log[2][4]['time:timestamp']
    ) == event_log[0][4]['time:timestamp']
    # The availability of the resource is the timestamp of its previous executed event
    assert resource_availability.available_since(
        'Dominic',
        datetime.fromisoformat('2006-11-07T17:00:00.000+02:00')
    ) == event_log[0][2]['time:timestamp']
    # The availability of the resource is the initial_time for the first event of the resource
    assert resource_availability.available_since(
        'Anya',
        event_log[3][0]['time:timestamp']
    ) == initial_time
    # The missing resource is always available since 'initial_time'
    assert resource_availability.available_since(
        missing_resource,
        datetime.fromisoformat('2006-11-07T10:00:00.000+02:00')
    ) == initial_time
    # The missing resource is always available since 'initial_time'
    assert resource_availability.available_since(
        missing_resource,
        datetime.fromisoformat('2006-11-09T10:00:00.000+02:00')
    ) == initial_time
