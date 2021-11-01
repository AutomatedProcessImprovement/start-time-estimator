#
# Author: David Chapela de la Campa
#

import pytz
import pm4py
from datetime import datetime
import pandas as pd

from pm4py.objects.log.obj import EventLog
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

from concurrency_oracle import AlphaConcurrencyOracle
from estimate_start_times import estimate_start_timestamps
from resource_availability import ResourceAvailability

missing_resource = "missing_resource"
initial_time = datetime.min.replace(tzinfo=pytz.UTC)


def read_xes_log(log_path) -> EventLog:
    # Read log
    event_log = pm4py.read_xes(log_path)
    # Fix missing resources
    for trace in event_log:
        for event in trace:
            if 'org:resource' not in event:
                event['org:resource'] = missing_resource
    return event_log


def read_csv_log(log_path) -> pd.DataFrame:
    # Read log
    col_names = ['caseID', 'activity', 'start_timestamp', 'end_timestamp', 'resource']  # Default column names
    event_log = pd.read_csv(log_path, sep=',')
    event_log = event_log[col_names]
    event_log = event_log.astype({'caseID': object})
    event_log = dataframe_utils.convert_timestamp_columns_in_df(event_log)
    event_log = event_log.sort_values('end_timestamp')
    event_log.resource.fillna('missing_resource', inplace=True)  # Fix missing resources
    return event_log


def main(event_log_path) -> None:
    # Read event log
    event_log = read_xes_log(event_log_path)
    # Build concurrency oracle
    concurrency_oracle = AlphaConcurrencyOracle(event_log, initial_time)
    # Build resource schedule
    resource_availability = ResourceAvailability(event_log, initial_time, missing_resource)
    # Infer start timestamps
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle, resource_availability)
    # Export event log
    xes_exporter.apply(extended_event_log, './extended_event_log.xes')


if __name__ == '__main__':
    main('../event_logs/BPI Challenge 2017.xes.gz')
