from datetime import datetime

from estimate_start_times.config import Configuration
from estimate_start_times.resource_availability import SimpleResourceAvailability
from estimate_start_times.utils import read_csv_log


def test_resource_availability():
    config = Configuration()
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    resource_availability = SimpleResourceAvailability(event_log, config)
    # The configuration for the algorithm is the passed
    assert resource_availability.config == config
    # All the resources have been loaded
    assert set(resource_availability.resources_calendar.keys()) == {'Marcus', 'Dominic', 'Anya'}
    # The availability of the resource is the timestamp of its previous executed event
    third_trace = event_log[event_log[config.log_ids.case] == 'trace-03']
    first_trace = event_log[event_log[config.log_ids.case] == 'trace-01']
    assert resource_availability.available_since(
        'Marcus',
        third_trace.iloc[4][config.log_ids.end_time]
    ) == first_trace.iloc[4][config.log_ids.end_time]
    # The availability of the resource is the timestamp of its previous executed event
    assert resource_availability.available_since(
        'Dominic',
        datetime.fromisoformat('2006-11-07T17:00:00.000+02:00')
    ) == first_trace.iloc[2][config.log_ids.end_time]
    # The availability of the resource is the [non_estimated_time] for the first event of the resource
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert resource_availability.available_since(
        'Anya',
        fourth_trace.iloc[0][config.log_ids.end_time]
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


def test_resource_availability_bot_resources():
    config = Configuration(bot_resources={'Marcus', 'Dominic'})
    event_log = read_csv_log('./tests/assets/test_event_log_1.csv', config)
    resource_availability = SimpleResourceAvailability(event_log, config)
    # The configuration for the algorithm is the passed
    assert resource_availability.config == config
    # All the resources have been loaded
    assert set(resource_availability.resources_calendar.keys()) == {'Anya'}
    # The availability of a bot resource is the same timestamp as checked
    first_trace = event_log[event_log[config.log_ids.case] == 'trace-01']
    assert resource_availability.available_since(
        'Marcus',
        first_trace.iloc[4][config.log_ids.end_time]
    ) == first_trace.iloc[4][config.log_ids.end_time]
    # The availability of a bot resource is the same timestamp as checked
    assert resource_availability.available_since(
        'Dominic',
        datetime.fromisoformat('2006-11-07T17:00:00.000+02:00')
    ) == datetime.fromisoformat('2006-11-07T17:00:00.000+02:00')
    # The availability of the resource is the [non_estimated_time] for the first event of the resource
    fourth_trace = event_log[event_log[config.log_ids.case] == 'trace-04']
    assert resource_availability.available_since(
        'Anya',
        fourth_trace.iloc[0][config.log_ids.end_time]
    ) == config.non_estimated_time