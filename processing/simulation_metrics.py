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
simulated_path = "../event_logs/simulated/{}"
simulated_log_IDs = EventLogIDs(
    case="caseid",
    activity="task",
    start_time="start_timestamp",
    end_time="end_timestamp",
    resource="resource"
)


def measure_simulation():
    techniques = ["raw", "heur_median", "heur_median_2", "heur_median_5", "heur_mode", "heur_mode_2", "heur_mode_5"]
    print("dataset,simulated_from,absolute_hour_emd,trace_duration_emd,#traces_original,#traces_simulated")
    for log_name in logs:
        raw_event_log = read_and_preprocess_log(raw_path.format(log_name), DEFAULT_CSV_IDS)
        for technique in techniques:
            calculate_simulation_stats(log_name, technique, raw_event_log)


def calculate_simulation_stats(log_name: str, method: str, raw_event_log: pd.DataFrame):
    # Measure stats for estimated log
    simulated_logs_path = simulated_path.format(log_name + "/" + method + "/sim_data/")
    simulated_event_log = None
    if os.path.exists(simulated_logs_path):
        for file_name in os.listdir(simulated_logs_path):
            tmp_event_log = read_and_preprocess_log(simulated_logs_path + file_name, simulated_log_IDs)
            tmp_event_log = tmp_event_log[~tmp_event_log[simulated_log_IDs.activity].isin(['Start', 'End'])]
            if simulated_event_log is None:
                simulated_event_log = tmp_event_log
            else:
                max_case_id = simulated_event_log[simulated_log_IDs.case].max()
                tmp_event_log[simulated_log_IDs.case] = tmp_event_log[simulated_log_IDs.case] + max_case_id + 1
                simulated_event_log = simulated_event_log.append(tmp_event_log)
        hour_emd = absolute_hour_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs)
        duration_emd = trace_duration_emd(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs, 100)
        print("{},{},{},{},{},{}".format(
            log_name,
            method,
            hour_emd,
            duration_emd,
            len(raw_event_log[DEFAULT_CSV_IDS.case].unique()),
            len(simulated_event_log[simulated_log_IDs.case].unique())
        ))


def absolute_hour_emd(event_log_1: pd.DataFrame, log_1_ids: EventLogIDs, event_log_2: pd.DataFrame, log_2_ids: EventLogIDs) -> float:
    # Get the first date of the log
    interval_start = min(event_log_1[log_1_ids.start_time].min(), event_log_2[log_2_ids.start_time].min())
    interval_end = max(event_log_1[log_1_ids.end_time].max(), event_log_2[log_2_ids.end_time].max())
    # Create empty histograms
    histogram_1 = [0] * math.floor(((interval_end - interval_start).total_seconds() / 3600) + 1)
    histogram_2 = histogram_1.copy()
    # Increase, for each time, the value in its corresponding bin
    absolute_hours = [math.floor(difference.total_seconds() / 3600) for difference in (event_log_1[log_1_ids.start_time] - interval_start)]
    absolute_hours += [math.floor(difference.total_seconds() / 3600) for difference in (event_log_1[log_1_ids.end_time] - interval_start)]
    for absolute_hour, frequency in Counter(absolute_hours).items():
        histogram_1[absolute_hour] = frequency
    absolute_hours = [math.floor(difference.total_seconds() / 3600) for difference in (event_log_2[log_2_ids.start_time] - interval_start)]
    absolute_hours += [math.floor(difference.total_seconds() / 3600) for difference in (event_log_2[log_2_ids.end_time] - interval_start)]
    for absolute_hour, frequency in Counter(absolute_hours).items():
        histogram_2[absolute_hour] = frequency
    # Return EMD metric
    return wasserstein_distance(histogram_1, histogram_2)


def trace_duration_emd(
        event_log_1: pd.DataFrame,
        log_1_ids: EventLogIDs,
        event_log_2: pd.DataFrame,
        log_2_ids: EventLogIDs,
        num_bins: int
) -> float:
    # Get trace durations for both logs
    trace_durations_1 = []
    for case, events in event_log_1.groupby([log_1_ids.case]):
        trace_durations_1 += [events[log_1_ids.end_time].max() - events[log_1_ids.start_time].min()]
    trace_durations_2 = []
    for case, events in event_log_2.groupby([log_2_ids.case]):
        trace_durations_2 += [events[log_2_ids.end_time].max() - events[log_2_ids.start_time].min()]
    # Get size of the bin
    min_duration = max(trace_durations_1 + trace_durations_2)
    max_duration = min(trace_durations_1 + trace_durations_2)
    bin_size = (max_duration - min_duration) / (num_bins - 1)
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
