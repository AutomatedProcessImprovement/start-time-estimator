import pytz
from datetime import datetime
from data_frame.resource_availability import ResourceAvailability
from event_log_readers import read_csv_log


def test_resource_availability():
    initial_time = datetime.min.replace(tzinfo=pytz.UTC)
    missing_resource = 'missing_resource'
    event_log = read_csv_log('../assets/test_event_log_1.csv')
    resource_availability = ResourceAvailability(event_log, initial_time, missing_resource)
    # The initial time to use as default is the passed
    assert resource_availability.initial_time == initial_time
    # All the resources have been loaded
    assert set(resource_availability.resources_calendar.keys()) == {'Marcus', 'Dominic', 'Anya'}
    # The availability of the resource is the timestamp of its previous executed event
    third_trace = event_log[event_log['caseID'] == 'trace-03']
    first_trace = event_log[event_log['caseID'] == 'trace-01']
    assert resource_availability.available_since(
        'Marcus',
        third_trace.iloc[4]['end_timestamp']
    ) == first_trace.iloc[4]['end_timestamp']
    # The availability of the resource is the timestamp of its previous executed event
    assert resource_availability.available_since(
        'Dominic',
        datetime.fromisoformat('2006-11-07T17:00:00.000+02:00')
    ) == first_trace.iloc[2]['end_timestamp']
    # The availability of the resource is the initial_time for the first event of the resource
    fourth_trace = event_log[event_log['caseID'] == 'trace-04']
    assert resource_availability.available_since(
        'Anya',
        fourth_trace.iloc[0]['end_timestamp']
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
