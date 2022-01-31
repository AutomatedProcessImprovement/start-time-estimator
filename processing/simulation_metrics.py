import math
import os
from collections import Counter

import numpy as np
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
    print("dataset,simulated_from,median_emd,mean_emd, ")
    for log_name in logs:
        raw_event_log = read_and_preprocess_log(raw_path.format(log_name), DEFAULT_CSV_IDS)
        for technique in techniques:
            calculate_simulation_stats(log_name, technique, raw_event_log)


def calculate_simulation_stats(log_name: str, method: str, raw_event_log: pd.DataFrame):
    # Measure stats for estimated log
    simulated_logs_path = simulated_path.format(log_name + "/" + method + "/sim_data/")
    emds, maes = [], []
    for file_name in os.listdir(simulated_logs_path):
        simulated_event_log = read_and_preprocess_log(simulated_logs_path + file_name, simulated_log_IDs)
        simulated_event_log = simulated_event_log[~simulated_event_log[simulated_log_IDs.activity].isin(['Start', 'End'])]
        emds += [earth_movers_distance(raw_event_log, DEFAULT_CSV_IDS, simulated_event_log, simulated_log_IDs)]
    if len(emds) != 0:
        print("{},{},{},{}".format(
            log_name,
            method,
            np.median(emds),
            np.mean(emds)
        ))


def earth_movers_distance(event_log_1: pd.DataFrame, log_1_ids: EventLogIDs, event_log_2: pd.DataFrame, log_2_ids: EventLogIDs) -> float:
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

    return wasserstein_distance(histogram_1, histogram_2)


if __name__ == '__main__':
    measure_simulation()
