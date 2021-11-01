import pandas as pd
import pm4py

from pm4py.objects.log.obj import EventLog
from pm4py.objects.log.util import dataframe_utils


def read_xes_log(log_path, missing_resource="missing_resource") -> EventLog:
    # Read log
    event_log = pm4py.read_xes(log_path)
    # Fix missing resources
    for trace in event_log:
        for event in trace:
            if 'org:resource' not in event:
                event['org:resource'] = missing_resource
    return event_log


def read_csv_log(log_path, missing_resource="missing_resource") -> pd.DataFrame:
    # Read log
    event_log = pd.read_csv(log_path, sep=',')
    # Default column names
    event_log = event_log[['caseID', 'activity', 'start_timestamp', 'end_timestamp', 'resource']]
    # Set case id as object
    event_log = event_log.astype({'caseID': object})
    # Fix missing resources
    event_log['resource'].fillna(missing_resource, inplace=True)
    # Convert timestamp values to datetime and sort by end time
    event_log = dataframe_utils.convert_timestamp_columns_in_df(event_log)
    event_log = event_log.sort_values('end_timestamp')
    # Return parsed event log
    return event_log
