import os
import time

import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.obj import EventLog

from config import Configuration, DEFAULT_XES_IDS, ReEstimationMethod, ConcurrencyOracleType, ResourceAvailabilityType, DEFAULT_CSV_IDS
from estimate_start_times import StartTimeEstimator
from event_log_readers import read_event_log


def main(event_log_path) -> None:
    # Configuration
    config = Configuration(
        # log_ids=DEFAULT_CSV_IDS,
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.ALPHA,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        missing_resource="NOT_SET"
    )
    # Read event log
    event_log = read_event_log(event_log_path, config)

    print("Starting start time estimation.")
    start_time = time.process_time()
    # Create start time estimator
    start_time_estimator = StartTimeEstimator(event_log, config)
    # Estimate start times
    extended_event_log = start_time_estimator.estimate()
    end_time = time.process_time()
    print("Estimation finished ({}s).".format(end_time - start_time))

    # Export event log
    dataframe = log_converter \
        .apply(extended_event_log, variant=log_converter.Variants.TO_DATA_FRAME) \
        .rename(
        columns={
            'case:concept:name': 'case',
            'concept:name': 'activity',
            'start:timestamp': 'start_timestamp',
            'time:timestamp': 'end_timestamp',
            'org:resource': 'resource'
        }
    )
    dataframe = dataframe[['case', 'activity', 'start_timestamp', 'end_timestamp', 'resource']]
    dataframe.to_csv(event_log_path.replace('.xes', '_estimated.csv'), index=False)
    # xes_exporter.apply(event_log, event_log_path.replace('.xes', '_estimated.xes'))


def experimentation(dir_path):
    for filename in os.listdir(dir_path):
        event_log_path = "{}{}".format(dir_path, filename)
        if event_log_path.endswith(".xes"):
            log_ids = DEFAULT_XES_IDS
            extension = ".xes"
        elif event_log_path.endswith(".xes.gz"):
            log_ids = DEFAULT_XES_IDS
            extension = ".xes.gz"
        elif event_log_path.endswith(".csv"):
            log_ids = DEFAULT_CSV_IDS
            extension = ".csv"
        elif event_log_path.endswith(".csv.gz"):
            log_ids = DEFAULT_CSV_IDS
            extension = ".csv.gz"
        else:
            continue  # Jump to te next file
        # Configuration
        print("\nProcessing event log {}".format(event_log_path))
        config = Configuration(
            # log_ids=DEFAULT_CSV_IDS,
            log_ids=log_ids,
            re_estimation_method=ReEstimationMethod.MODE,
            concurrency_oracle_type=ConcurrencyOracleType.ALPHA,
            resource_availability_type=ResourceAvailabilityType.SIMPLE,
            missing_resource="NOT_SET"
        )
        # Read event log
        event_log = read_event_log(event_log_path, config)
        # Process event log
        print("Starting start time estimation.")
        start_time = time.process_time()
        # Create start time estimator
        start_time_estimator = StartTimeEstimator(event_log, config)
        # Estimate start times
        extended_event_log = start_time_estimator.estimate()
        end_time = time.process_time()
        print("Estimation finished ({}s).".format(end_time - start_time))

        if type(extended_event_log) is EventLog:
            # Translate to pd.DataFrame
            extended_event_log = log_converter \
                .apply(extended_event_log, variant=log_converter.Variants.TO_DATA_FRAME) \
                .rename(
                    columns={
                        'case:{}'.format(config.log_ids.case): 'case:concept:name',
                        config.log_ids.activity: 'concept:name',
                        config.log_ids.start_timestamp: 'start:timestamp',
                        config.log_ids.end_timestamp: 'time:timestamp',
                        config.log_ids.resource: 'org:resource'
                    }
                )
        elif type(extended_event_log) is pd.DataFrame:
            # Rename the columns to fit SIMOD format
            extended_event_log = extended_event_log.rename(
                columns={
                    config.log_ids.case: 'case:concept:name',
                    config.log_ids.activity: 'concept:name',
                    config.log_ids.start_timestamp: 'start:timestamp',
                    config.log_ids.end_timestamp: 'time:timestamp',
                    config.log_ids.resource: 'org:resource'
                }
            )

        # Transform to SIMOD input format: two events with lifecycles
        extended_event_log = extended_event_log[
            ['case:concept:name', 'concept:name', 'start:timestamp', 'time:timestamp', 'org:resource']
        ].sort_values(by=['case:concept:name', 'time:timestamp'])
        extended_event_log_start = extended_event_log.copy()
        extended_event_log_start['lifecycle:transition'] = 'start'
        extended_event_log_start['time:timestamp'] = extended_event_log_start['start:timestamp']
        extended_event_log_end = extended_event_log.copy()
        extended_event_log_end['lifecycle:transition'] = 'complete'
        extended_event_log = pd.concat([extended_event_log_start, extended_event_log_end]) \
            .drop(['start:timestamp'], axis=1) \
            .rename_axis('index') \
            .sort_values(by=['time:timestamp', 'index', 'lifecycle:transition'], ascending=[True, True, False])
        # Unify date format to 'yyyy-mm-ddThh:mm:ss.sss+zz:zz' (chapuza)
        extended_event_log['time:timestamp'] = \
            extended_event_log['time:timestamp'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x: x[:-3]) + \
            extended_event_log['time:timestamp'].apply(lambda x: x.strftime("%z")).apply(lambda x: x[:-2]) + \
            ":" + \
            extended_event_log['time:timestamp'].apply(lambda x: x.strftime("%z")).apply(lambda x: x[-2:])
        # Transform to event log
        extended_event_log = log_converter.apply(extended_event_log, variant=log_converter.Variants.TO_EVENT_LOG)
        # Export event log
        output_path = event_log_path.replace(extension, '_estimated_str.xes')
        xes_exporter.apply(extended_event_log, output_path)
        # Change 'time:timestamp' from 'string' to 'date' (chapuza)
        change_string_by_date(output_path)


def change_string_by_date(file_path):
    with open(file_path, 'r') as input_file, open(file_path.replace('_str.xes', '.xes'), 'w') as output_file:
        for line in input_file.readlines():
            if "<string key=\"time:timestamp\"" in line:
                output_file.write(line.replace("<string key=\"time:timestamp\"", "<date key=\"time:timestamp\""))
            else:
                output_file.write(line)
    os.remove(file_path)


if __name__ == '__main__':
    # main("../event_logs/Production.xes.gz")
    experimentation("../event_logs/test/")
