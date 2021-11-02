import pandas as pd
import pm4py

from pm4py.objects.log.obj import EventLog
from pm4py.objects.log.util import dataframe_utils


def read_xes_log(log_path, config) -> EventLog:
    # Read log
    event_log = pm4py.read_xes(log_path)
    # Fix missing resources
    for trace in event_log:
        for event in trace:
            if config.log_ids.resource not in event:
                event[config.log_ids.resource] = config.missing_resource
    return event_log


def read_csv_log(log_path, config) -> pd.DataFrame:
    # Read log
    event_log = pd.read_csv(log_path)
    # Set case id as object
    event_log = event_log.astype({config.log_ids.case: object})
    # Fix missing resources
    event_log[config.log_ids.resource].fillna(config.missing_resource, inplace=True)
    # Convert timestamp values to datetime and sort by end time
    event_log = dataframe_utils.convert_timestamp_columns_in_df(event_log)
    event_log = event_log.sort_values(config.log_ids.end_timestamp)
    # Return parsed event log
    return event_log
