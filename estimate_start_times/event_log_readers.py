from typing import Union

import pandas as pd
import pm4py
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.objects.log.obj import EventLog


def read_event_log(log_path, config) -> Union[EventLog, pd.DataFrame]:
    if log_path.endswith(".xes") or log_path.endswith(".xes.gz"):
        event_log = read_xes_log(log_path, config)
    elif log_path.endswith(".csv") or log_path.endswith(".csv.gz"):
        event_log = read_csv_log(log_path, config)
    else:
        raise ValueError("Unknown event log file extension! Only 'xes', 'xes.gz', 'csv', and 'csv.gz' supported.")
    # Return read event log
    return event_log


def read_xes_log(log_path, config) -> EventLog:
    # Read log
    event_log = pm4py.read_xes(log_path)
    # Retain only 'complete' events
    event_log = attributes_filter.apply_events(
        event_log,
        ["complete"],
        parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: config.log_ids.lifecycle, attributes_filter.Parameters.POSITIVE: True}
    )
    # Fix missing resources
    for trace in event_log:
        for event in trace:
            if config.log_ids.resource not in event:
                event[config.log_ids.resource] = config.missing_resource
    return event_log


def read_csv_log(log_path, config, reset_start_times=True) -> pd.DataFrame:
    # Read log
    event_log = pd.read_csv(log_path)
    # If the events have a lifecycle, retain only 'complete'
    if config.log_ids.lifecycle in event_log.columns:
        event_log = event_log[event_log[config.log_ids.lifecycle] == 'complete']
    # Set case id as object
    event_log = event_log.astype({config.log_ids.case: object})
    # Fix missing resources
    if config.log_ids.resource not in event_log.columns:
        event_log[config.log_ids.resource] = config.missing_resource
    else:
        event_log[config.log_ids.resource].fillna(config.missing_resource, inplace=True)
    # Convert timestamp value to datetime
    event_log[config.log_ids.end_timestamp] = pd.to_datetime(event_log[config.log_ids.end_timestamp], utc=True)
    # Set as NaT the start time
    if reset_start_times:
        event_log[config.log_ids.start_timestamp] = pd.NaT
    else:
        event_log[config.log_ids.start_timestamp] = pd.to_datetime(event_log[config.log_ids.start_timestamp], utc=True)
    # Sort by end time
    event_log = event_log.sort_values(config.log_ids.end_timestamp)
    # Return parsed event log
    return event_log
