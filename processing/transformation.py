import os

import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.obj import EventLog

from config import EventLogIDs


def export_event_log_only_millis(event_log: EventLog, export_path: str) -> None:
    """
    Export [event_log] following SIMOD requirements (date times only with millis, not microseconds).

    :param event_log: event log to export.
    :param export_path: path to write the event log file.
    """
    # Convert datetime to str with desired format
    for trace in event_log:
        for event in trace:
            event['time:timestamp'] = (event['time:timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +
                                       event['time:timestamp'].strftime('%z')[:-2] +
                                       ":" +
                                       event['time:timestamp'].strftime('%z')[-2:])
    if 'event' in event_log.omni_present and 'time:timestamp' in event_log.omni_present['event']:
        timestamp = event_log.omni_present['event']['time:timestamp']
        event_log.omni_present['event']['time:timestamp'] = (timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +
                                                             timestamp.strftime('%z')[:-2] +
                                                             ":" +
                                                             timestamp.strftime('%z')[-2:])
    # Write to file
    xes_exporter.apply(event_log, export_path + ".str")
    # Edit timestamp fields to "date" (chapuza)
    with open(export_path + ".str", 'r') as input_file, open(export_path, 'w') as output_file:
        for line in input_file.readlines():
            if "<string key=\"time:timestamp\"" in line:
                output_file.write(line.replace("<string key=\"time:timestamp\"", "<date key=\"time:timestamp\""))
            else:
                output_file.write(line)
    os.remove(export_path + ".str")


def from_csv_to_simod_xes(event_log_path: str, log_ids: EventLogIDs) -> None:
    """
    Read a CSV event log with [log_ids] having start and end times per activity instance, and export
    it as XES supported by SIMOD (two events per activity instance: 'start' and 'complete').

    :param event_log_path: path to the XES event log file.
    :param log_ids: IDs correspondence in the CSV log.
    """
    # Read event log
    event_log = pd.read_csv(event_log_path)
    # Parse timestamps
    event_log[log_ids.start_timestamp] = pd.to_datetime(event_log[log_ids.start_timestamp], utc=True)
    event_log[log_ids.end_timestamp] = pd.to_datetime(event_log[log_ids.end_timestamp], utc=True)
    # Rename the columns to fit SIMOD format
    event_log = event_log.rename(
        columns={
            log_ids.case: 'case:concept:name',
            log_ids.activity: 'concept:name',
            log_ids.start_timestamp: 'time:start',
            log_ids.end_timestamp: 'time:timestamp',
            log_ids.resource: 'org:resource'
        }
    )[
        ['case:concept:name', 'concept:name', 'time:start', 'time:timestamp', 'org:resource']
    ]
    # Create 'start' events
    event_log_start = event_log.copy()
    event_log_start['lifecycle:transition'] = 'start'
    event_log_start['time:timestamp'] = event_log_start['time:start']
    event_log_end = event_log.copy()
    event_log_end['lifecycle:transition'] = 'complete'
    event_log = pd.concat([event_log_start, event_log_end]) \
        .drop(['time:start'], axis=1) \
        .rename_axis('index') \
        .sort_values(by=['time:timestamp', 'index', 'lifecycle:transition'], ascending=[True, True, False])
    # Transform to event log
    event_log = log_converter.apply(event_log, variant=log_converter.Variants.TO_EVENT_LOG)
    # Export event log
    export_event_log_only_millis(event_log, event_log_path.replace(".csv.gz", ".xes"))


def unify_csv_date_format(event_log_path: str, log_ids: EventLogIDs, output_path: str) -> None:
    """
        Read a CSV event log with [log_ids] having start and end times per activity instance, and export
        it with a unified date format 'yyyy-mm-ddThh:mm:ss.sss+zz:zz'.

        :param event_log_path: path to the CSV event log file.
        :param log_ids: IDs correspondence in the CSV log.
        :param output_path: path to the file where to write the formatted log.
    """
    # Read event log
    event_log = pd.read_csv(event_log_path)
    # Set case id as object
    event_log = event_log.astype({log_ids.case: object})
    # Convert timestamp value to datetime
    event_log[log_ids.end_timestamp] = pd.to_datetime(event_log[log_ids.end_timestamp], utc=True)
    event_log[log_ids.start_timestamp] = pd.to_datetime(event_log[log_ids.start_timestamp], utc=True)
    # Convert back to string fulfilling format
    event_log[log_ids.end_timestamp] = \
        event_log[log_ids.end_timestamp].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x: x[:-3]) + \
        event_log[log_ids.end_timestamp].apply(lambda x: x.strftime("%z")).apply(lambda x: x[:-2]) + \
        ":" + \
        event_log[log_ids.end_timestamp].apply(lambda x: x.strftime("%z")).apply(lambda x: x[-2:])
    event_log[log_ids.start_timestamp] = \
        event_log[log_ids.start_timestamp].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x: x[:-3]) + \
        event_log[log_ids.start_timestamp].apply(lambda x: x.strftime("%z")).apply(lambda x: x[:-2]) + \
        ":" + \
        event_log[log_ids.start_timestamp].apply(lambda x: x.strftime("%z")).apply(lambda x: x[-2:])
    # Export event log
    event_log.to_csv(output_path, encoding='utf-8', index=False)


if __name__ == '__main__':
    pass
