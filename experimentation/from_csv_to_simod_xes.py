import os

from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

from config import Configuration, DEFAULT_CSV_IDS
from event_log_readers import read_csv_log


def main(event_log_path) -> None:
    # Configuration
    config = Configuration(log_ids=DEFAULT_CSV_IDS)
    # Read event log
    complete_event_log = read_csv_log(event_log_path, config)
    # Process it
    start_event_log = complete_event_log.copy()
    start_event_log['lifecycle'] = 'start'
    start_event_log['end_timestamp'] = start_event_log['start_timestamp']
    complete_event_log['lifecycle'] = 'complete'
    event_log = complete_event_log \
        .append(start_event_log) \
        .drop(['start_timestamp'], axis=1) \
        .sort_values(by=['case', 'end_timestamp'])
    # Rename columns
    event_log = event_log.rename(columns={
        'case': 'case:concept:name',
        'activity': 'concept:name',
        'end_timestamp': 'time:timestamp',
        'lifecycle': 'lifecycle:transition',
        'resource': 'org:resource'
    })
    # Unify date format to 'yyyy-mm-ddThh:mm:ss.sss+zz:zz' (chapuza)
    event_log['time:timestamp'] = \
        event_log['time:timestamp'].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3] + \
        event_log['time:timestamp'].dt.strftime("%z").str[:-2] + \
        ":" + \
        event_log['time:timestamp'].dt.strftime("%z").str[-2:]
    # Transform to event log
    event_log = log_converter.apply(event_log, variant=log_converter.Variants.TO_EVENT_LOG)
    # Export event log
    output_path = event_log_path.replace('.csv', '_str.xes')
    xes_exporter.apply(event_log, output_path)
    # Change 'time:timestamp' from 'string' to 'date' (chapuza)
    change_string_by_date(output_path)
    os.remove(output_path)


def change_string_by_date(file_path):
    with open(file_path, 'r') as input_file, open(file_path.replace('_str.xes', '.xes'), 'w') as output_file:
        for line in input_file.readlines():
            if "<string key=\"time:timestamp\"" in line:
                output_file.write(line.replace("<string key=\"time:timestamp\"", "<date key=\"time:timestamp\""))
            else:
                output_file.write(line)


if __name__ == '__main__':
    main("../event_logs/PurchasingExample_estimated.csv")
