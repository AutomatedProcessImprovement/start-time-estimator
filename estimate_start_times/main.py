import os
import time

import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.obj import EventLog

from config import Configuration, DEFAULT_XES_IDS, ReEstimationMethod, ConcurrencyOracleType, ResourceAvailabilityType, \
    HeuristicsThresholds
from estimate_start_times import StartTimeEstimator
from event_log_readers import read_event_log


def run_estimation(event_log_path, configuration, extension):
    print("\nProcessing event log {}".format(event_log_path))
    # Read event log
    event_log = read_event_log(event_log_path, configuration)
    # Process event log
    print("Starting start time estimation.")
    start_time = time.process_time()
    # Create start time estimator
    start_time_estimator = StartTimeEstimator(event_log, configuration)
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
                'case:{}'.format(configuration.log_ids.case): 'case:concept:name',
                configuration.log_ids.activity: 'concept:name',
                configuration.log_ids.start_timestamp: 'time:start',
                configuration.log_ids.end_timestamp: 'time:timestamp',
                configuration.log_ids.resource: 'org:resource'
            }
        )
    elif type(extended_event_log) is pd.DataFrame:
        # Rename the columns to fit SIMOD format
        extended_event_log = extended_event_log.rename(
            columns={
                configuration.log_ids.case: 'case:concept:name',
                configuration.log_ids.activity: 'concept:name',
                configuration.log_ids.start_timestamp: 'time:start',
                configuration.log_ids.end_timestamp: 'time:timestamp',
                configuration.log_ids.resource: 'org:resource'
            }
        )

    # Transform to SIMOD input format: two events with lifecycles
    extended_event_log = extended_event_log[
        ['case:concept:name', 'concept:name', 'time:start', 'time:timestamp', 'org:resource']
    ].sort_values(by=['case:concept:name', 'time:timestamp'])
    extended_event_log_start = extended_event_log.copy()
    extended_event_log_start['lifecycle:transition'] = 'start'
    extended_event_log_start['time:timestamp'] = extended_event_log_start['time:start']
    extended_event_log_end = extended_event_log.copy()
    extended_event_log_end['lifecycle:transition'] = 'complete'
    extended_event_log = pd.concat([extended_event_log_start, extended_event_log_end]) \
        .drop(['time:start'], axis=1) \
        .rename_axis('index') \
        .sort_values(by=['time:timestamp', 'index', 'lifecycle:transition'], ascending=[True, True, False])
    # Unify date format to 'yyyy-mm-ddThh:mm:ss.sss+zz:zz' (chapuza)
    extended_event_log['time:timestamp'] = \
        extended_event_log['time:timestamp'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')).apply(
            lambda x: x[:-3]) + \
        extended_event_log['time:timestamp'].apply(lambda x: x.strftime("%z")).apply(lambda x: x[:-2]) + \
        ":" + \
        extended_event_log['time:timestamp'].apply(lambda x: x.strftime("%z")).apply(lambda x: x[-2:])
    # Transform to event log
    extended_event_log = log_converter.apply(extended_event_log, variant=log_converter.Variants.TO_EVENT_LOG)
    # Export event log
    output_path = event_log_path.replace(extension, "_estimated_str.xes")
    xes_exporter.apply(extended_event_log, output_path)
    # Change 'time:timestamp' from 'string' to 'date' (chapuza)
    change_string_by_date(output_path)


def change_string_by_date(file_path):
    with open(file_path, 'r') as input_file, open(file_path.replace("_str.xes", ".xes"), 'w') as output_file:
        for line in input_file.readlines():
            if "<string key=\"time:timestamp\"" in line:
                output_file.write(line.replace("<string key=\"time:timestamp\"", "<date key=\"time:timestamp\""))
            else:
                output_file.write(line)
    os.remove(file_path)


def main():
    # Consulta Data Mining 2016 - 2018
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={"Start", "End"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
    )
    run_estimation("../event_logs/ConsultaDataMining201618.xes.gz", config, ".xes.gz")
    # Production
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={"Start", "End"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
    )
    run_estimation("../event_logs/Production.xes.gz", config, ".xes.gz")
    # CVS Pharmacy
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={"Pharmacy System-000001"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
    )
    run_estimation("../event_logs/cvs_pharmacy.xes.gz", config, ".xes.gz")


if __name__ == '__main__':
    main()
