from datetime import timedelta
from typing import Union

import pandas as pd
from numpy import mean, median
from pm4py.objects.log.obj import EventLog
from sklearn.metrics import mean_absolute_error

from config import DEFAULT_CSV_IDS, EventLogIDs

logs = [
    "Application_to_Approval_Government_Agency",
    "BPI_Challenge_2012_W_Two_TS",
    "BPI_Challenge_2017_W_Two_TS",
    "callcentre",
    "confidential",
    "ConsultaDataMining201618",
    "cvs_pharmacy",
    "insurance",
    "Loan_Application",
    "poc_processmining",
    "Procure_to_Pay",
    "Production",
]
raw_path = "../event_logs/{}.csv.gz"


def get_cycle_times(event_log: Union[EventLog, pd.DataFrame], config: EventLogIDs) -> list:
    # Get cycle times
    cycle_times = []
    if type(event_log) is EventLog:
        for trace in event_log:
            cycle_times += [
                max([event[config.end_timestamp] for event in trace]) -
                min([event[config.start_timestamp] for event in trace])
            ]
    else:
        for (key, trace) in event_log.groupby([config.case]):
            cycle_times += [
                max([event[config.end_timestamp] for (_, event) in trace.iterrows()]) -
                min([event[config.start_timestamp] for (_, event) in trace.iterrows()])
            ]
    # Return measured cycle times
    return [time.total_seconds() for time in cycle_times]


def get_processing_times(event_log: Union[EventLog, pd.DataFrame], config: EventLogIDs) -> list:
    # Get processing times
    processing_times = []
    if type(event_log) is EventLog:
        for trace in event_log:
            processing_times += [
                sum(
                    [event[config.end_timestamp] - event[config.start_timestamp] for event in trace],
                    timedelta(0)
                )
            ]
    else:
        for (key, trace) in event_log.groupby([config.case]):
            processing_times += [
                sum(
                    [event[config.end_timestamp] - event[config.start_timestamp] for (_, event) in trace.iterrows()],
                    timedelta(0)
                )
            ]
    # Return measured processing times
    return [time.total_seconds() for time in processing_times]


def measure():
    print("log,"
          "mae_proc_time (s),min_proc_time,max_proc_time,avg_proc_time,median_proc_time,"
          "mae_cycle_time (s),min_cycle_time,max_cycle_time,avg_cycle_time,median_cycle_time")
    for log in logs:
        # Stats for the raw log
        raw_event_log = pd.read_csv(raw_path.format(log))
        raw_event_log[DEFAULT_CSV_IDS.end_timestamp] = pd.to_datetime(raw_event_log[DEFAULT_CSV_IDS.end_timestamp], utc=True)
        raw_event_log[DEFAULT_CSV_IDS.start_timestamp] = pd.to_datetime(raw_event_log[DEFAULT_CSV_IDS.start_timestamp], utc=True)
        raw_processing_times = get_processing_times(raw_event_log, DEFAULT_CSV_IDS)
        raw_cycle_times = get_cycle_times(raw_event_log, DEFAULT_CSV_IDS)
        print("{},,{},{},{},{},,{},{},{},{}".format(
            log,
            min(raw_processing_times),
            max(raw_processing_times),
            mean(raw_processing_times),
            median(raw_processing_times),
            min(raw_cycle_times),
            max(raw_cycle_times),
            mean(raw_cycle_times),
            median(raw_cycle_times)
        ))
        # Stats for the Heuristics Median no outlier control
        measure_estimated_stats(log, "heur_median", raw_processing_times, raw_cycle_times)
        # Stats for the Heuristics Median Threshold=200%
        measure_estimated_stats(log, "heur_median_2", raw_processing_times, raw_cycle_times)
        # Stats for the Heuristics Median Threshold=500%
        measure_estimated_stats(log, "heur_median_5", raw_processing_times, raw_cycle_times)
        # Stats for the Heuristics Mode no outlier control
        measure_estimated_stats(log, "heur_mode", raw_processing_times, raw_cycle_times)
        # Stats for the Heuristics Mode Threshold=200%
        measure_estimated_stats(log, "heur_mode_2", raw_processing_times, raw_cycle_times)
        # Stats for the Heuristics Mode Threshold=500%
        measure_estimated_stats(log, "heur_mode_5", raw_processing_times, raw_cycle_times)


def measure_estimated_stats(log: str, method: str, raw_processing_times: list, raw_cycle_times: list):
    # Measure stats for estimated log
    estimated_event_log = pd.read_csv(raw_path.format(method + "/" + log + "_estimated"))
    estimated_event_log[DEFAULT_CSV_IDS.end_timestamp] = pd.to_datetime(estimated_event_log[DEFAULT_CSV_IDS.end_timestamp], utc=True)
    estimated_event_log[DEFAULT_CSV_IDS.start_timestamp] = pd.to_datetime(estimated_event_log[DEFAULT_CSV_IDS.start_timestamp], utc=True)
    estimated_cycle_times = get_cycle_times(estimated_event_log, DEFAULT_CSV_IDS)
    estimated_processing_times = get_processing_times(estimated_event_log, DEFAULT_CSV_IDS)
    print("{}_{},{},{},{},{},{},{},{},{},{},{}".format(
        log,
        method,
        mean_absolute_error(raw_processing_times, estimated_processing_times),
        min(estimated_processing_times) - min(raw_processing_times),
        max(estimated_processing_times) - max(raw_processing_times),
        mean(estimated_processing_times) - mean(raw_processing_times),
        median(estimated_processing_times) - median(raw_processing_times),
        mean_absolute_error(raw_cycle_times, estimated_cycle_times),
        min(estimated_cycle_times) - min(raw_cycle_times),
        max(estimated_cycle_times) - max(raw_cycle_times),
        mean(estimated_cycle_times) - mean(raw_cycle_times),
        median(estimated_cycle_times) - median(raw_cycle_times)
    ))


if __name__ == '__main__':
    measure()
