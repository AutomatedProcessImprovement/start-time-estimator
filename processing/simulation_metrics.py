import datetime
import enum
import math
import os
from collections import Counter

import pandas as pd
from scipy.stats import wasserstein_distance

from estimate_start_times.config import DEFAULT_CSV_IDS, EventLogIDs
from start_time_metrics import read_and_preprocess_log

logs = [
    "insurance",
    "BPI_Challenge_2012_W_Two_TS",
    "BPI_Challenge_2017_W_Two_TS",
    # "Application_to_Approval_Government_Agency",
    "callcentre",
    "ConsultaDataMining201618",
    # "poc_processmining",
    "Production"
]
raw_path = "../event_logs/{}.csv.gz"
simulated_logs_path = "../event_logs/simulated/{}/{}/{}_simulated_1.csv"
simulated_log_IDs = EventLogIDs(
    case="caseid",
    activity="task",
    start_time="start_timestamp",
    end_time="end_timestamp",
    resource="resource"
)


class _EmdType(enum.Enum):
    BOTH = 0
    START = 1
    END = 2


def measure_simulation():
    techniques = ["raw", "heur_median", "heur_median_2", "heur_median_5", "heur_mode", "heur_mode_2", "heur_mode_5"]
    print("dataset,simulated_from,start_time_hour_emd,end_time_hour_emd,absolute_hour_emd,"
          "start_time_day_emd,end_time_day_emd,absolute_day_emd,trace_duration_emd")
    for log_name in logs:
        raw_event_log = read_and_preprocess_log(raw_path.format(log_name), DEFAULT_CSV_IDS)
        for technique in techniques:
            calculate_simulation_stats(log_name, technique, raw_event_log)


def calculate_simulation_stats(log_name: str, method: str, raw_event_log: pd.DataFrame):
    # Measure stats for estimated log
    simulated_log_path = simulated_logs_path.format(log_name, method, log_name)
    if os.path.exists(simulated_log_path):
        simulated_event_log = read_and_preprocess_log(simulated_log_path, simulated_log_IDs)
        simulated_event_log = simulated_event_log[~simulated_event_log[simulated_log_IDs.activity].isin(['Start', 'End'])]
        # Calculate cycle time bin size w.r.t. the original log
        bin_size = max(
            [events[DEFAULT_CSV_IDS.end_time].max() - events[DEFAULT_CSV_IDS.start_time].min()
             for case, events in raw_event_log.groupby([DEFAULT_CSV_IDS.case])]
        ) / 100
        print("{},{},{},{},{},{},{},{},{}".format(
            log_name,
            method,
            absolute_hour_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, _EmdType.START, discretize_to_hour),
            absolute_hour_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, _EmdType.END, discretize_to_hour),
            absolute_hour_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, _EmdType.BOTH, discretize_to_hour),
            absolute_hour_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, _EmdType.START, discretize_to_day),
            absolute_hour_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, _EmdType.END, discretize_to_day),
            absolute_hour_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, _EmdType.BOTH, discretize_to_day),
            trace_duration_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, bin_size)
        ))


def discretize_to_hour(seconds: int):
    return math.floor(seconds / 3600)


def discretize_to_day(seconds: int):
    return math.floor(seconds / 3600 / 24)


def absolute_hour_emd(
        event_log_1: pd.DataFrame,
        log_1_ids: EventLogIDs,
        event_log_2: pd.DataFrame,
        log_2_ids: EventLogIDs,
        emd_type: _EmdType = _EmdType.BOTH,
        discretize=discretize_to_hour  # function to discretize a total amount of seconds
) -> float:
    # Get the first and last dates of the log
    if emd_type == _EmdType.BOTH:
        interval_start = min(event_log_1[log_1_ids.start_time].min(), event_log_2[log_2_ids.start_time].min())
        interval_end = max(event_log_1[log_1_ids.end_time].max(), event_log_2[log_2_ids.end_time].max())
    elif emd_type == _EmdType.START:
        interval_start = min(event_log_1[log_1_ids.start_time].min(), event_log_2[log_2_ids.start_time].min())
        interval_end = max(event_log_1[log_1_ids.start_time].max(), event_log_2[log_2_ids.start_time].max())
    else:
        interval_start = min(event_log_1[log_1_ids.end_time].min(), event_log_2[log_2_ids.end_time].min())
        interval_end = max(event_log_1[log_1_ids.end_time].max(), event_log_2[log_2_ids.end_time].max())
    # Create empty histograms
    histogram_1 = [0] * (discretize((interval_end - interval_start).total_seconds()) + 1)
    histogram_2 = histogram_1.copy()
    # Increase, for each time, the value in its corresponding bin
    absolute_hours = []
    if emd_type != _EmdType.END:
        absolute_hours += [
            discretize(difference.total_seconds()) for difference in (event_log_1[log_1_ids.start_time] - interval_start)
        ]
    if emd_type != _EmdType.START:
        absolute_hours += [
            discretize(difference.total_seconds()) for difference in (event_log_1[log_1_ids.end_time] - interval_start)
        ]
    for absolute_hour, frequency in Counter(absolute_hours).items():
        histogram_1[absolute_hour] = frequency
    # Increase, for each time, the value in its corresponding bin
    absolute_hours = []
    if emd_type != _EmdType.END:
        absolute_hours += [
            discretize(difference.total_seconds()) for difference in (event_log_2[log_2_ids.start_time] - interval_start)
        ]
    if emd_type != _EmdType.START:
        absolute_hours += [
            discretize(difference.total_seconds()) for difference in (event_log_2[log_2_ids.end_time] - interval_start)
        ]
    for absolute_hour, frequency in Counter(absolute_hours).items():
        histogram_2[absolute_hour] = frequency
    # Return EMD metric
    return wasserstein_distance(histogram_1, histogram_2)


def trace_duration_emd(
        event_log_1: pd.DataFrame,
        log_1_ids: EventLogIDs,
        event_log_2: pd.DataFrame,
        log_2_ids: EventLogIDs,
        bin_size: datetime.timedelta
) -> float:
    # Get trace durations for both logs
    trace_durations_1 = []
    for case, events in event_log_1.groupby([log_1_ids.case]):
        trace_durations_1 += [events[log_1_ids.end_time].max() - events[log_1_ids.start_time].min()]
    trace_durations_2 = []
    for case, events in event_log_2.groupby([log_2_ids.case]):
        trace_durations_2 += [events[log_2_ids.end_time].max() - events[log_2_ids.start_time].min()]
    # Get size of the bin
    min_duration = min(trace_durations_1 + trace_durations_2)
    max_duration = max(trace_durations_1 + trace_durations_2)
    num_bins = math.floor((max_duration - min_duration) / bin_size) + 1
    # Create empty histograms
    histogram_1 = [0] * num_bins
    histogram_2 = histogram_1.copy()
    # Increase, for each duration, the value in its corresponding bin
    for trace_duration in trace_durations_1:
        bin_index = math.floor((trace_duration - min_duration) / bin_size)
        histogram_1[bin_index] += 1
    for trace_duration in trace_durations_2:
        bin_index = math.floor((trace_duration - min_duration) / bin_size)
        histogram_2[bin_index] += 1
    # Return EMD metric
    return wasserstein_distance(histogram_1, histogram_2)


if __name__ == '__main__':
    measure_simulation()
