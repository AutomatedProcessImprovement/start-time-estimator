import math
from collections import Counter
from datetime import timedelta

import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance
from sklearn.metrics import mean_absolute_error

from estimate_start_times.config import DEFAULT_CSV_IDS

logs = [
    "insurance",
    "BPI_Challenge_2012_W_Two_TS",
    "BPI_Challenge_2017_W_Two_TS",
    "Application_to_Approval_Government_Agency",
    "callcentre",
    "ConsultaDataMining201618",
    "poc_processmining",
    "Production",
    "confidential",
    "Loan_Application",
    "cvs_pharmacy",
    "Procure_to_Pay",
]
raw_path = "../event_logs/{}.csv.gz"


def read_and_preprocess_log(event_log_path: str) -> pd.DataFrame:
    # Read from CSV
    event_log = pd.read_csv(event_log_path)
    # Transform to Timestamp bot start and end columns
    event_log[DEFAULT_CSV_IDS.start_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.start_time], utc=True)
    event_log[DEFAULT_CSV_IDS.end_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.end_time], utc=True)
    if DEFAULT_CSV_IDS.enabled_time in event_log:
        event_log[DEFAULT_CSV_IDS.enabled_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.enabled_time], utc=True)
    if DEFAULT_CSV_IDS.available_time in event_log:
        event_log[DEFAULT_CSV_IDS.available_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.available_time], utc=True)
    # Sort by end timestamp, then by start timestamp, and then by activity name
    event_log = event_log.sort_values(
        [DEFAULT_CSV_IDS.end_time, DEFAULT_CSV_IDS.activity, DEFAULT_CSV_IDS.case, DEFAULT_CSV_IDS.resource]
    )
    # Reset the index
    event_log.reset_index(drop=True, inplace=True)
    return event_log


def measure_estimation():
    techniques = ["heur_median", "heur_median_2", "heur_median_5",
                  "heur_mode", "heur_mode_2", "heur_mode_5",
                  "df_median", "df_median_2", "df_median_5",
                  "df_mode", "df_mode_2", "df_mode_5",
                  "only_resource_median", "only_resource_median_2", "only_resource_median_5",
                  "only_resource_mode", "only_resource_mode_2", "only_resource_mode_5"]
    print("log_technique,"
          "smape_proc_times,"
          "mape_proc_times,"
          "mae_proc_times (s),"
          "total_activity_instances,"
          "num_selected_enabled_time,"
          "num_selected_available_time,"
          "num_re_estimated,"
          "num_estimated_after_real,"
          "num_estimated_before_real,"
          "num_exact_estimation")
    for log_name in logs:
        raw_event_log = read_and_preprocess_log(raw_path.format(log_name))
        for technique in techniques:
            calculate_estimation_stats(log_name, technique, raw_event_log)


def calculate_estimation_stats(log_name: str, method: str, raw_event_log: pd.DataFrame):
    # Measure stats for estimated log
    estimated_event_log = read_and_preprocess_log(raw_path.format(method + "/" + log_name + "_estimated"))
    # Check sorting similarity
    if not raw_event_log[DEFAULT_CSV_IDS.end_time].equals(estimated_event_log[DEFAULT_CSV_IDS.end_time]):
        print("Different 'end_timestamp' order!!")
    if not raw_event_log[DEFAULT_CSV_IDS.activity].equals(estimated_event_log[DEFAULT_CSV_IDS.activity]):
        print("Different 'activity' order!!")
    if not raw_event_log[DEFAULT_CSV_IDS.case].equals(estimated_event_log[DEFAULT_CSV_IDS.case]):
        print("Different 'case' order!!")
    # Print stats
    raw_processing_times = (
                                   raw_event_log[DEFAULT_CSV_IDS.end_time] - raw_event_log[DEFAULT_CSV_IDS.start_time]
                           ).astype(np.int64) / 1000000000
    estimated_processing_times = (
                                         estimated_event_log[DEFAULT_CSV_IDS.end_time] - estimated_event_log[DEFAULT_CSV_IDS.start_time]
                                 ).astype(np.int64) / 1000000000
    raw_minus_estimated = raw_processing_times - estimated_processing_times
    print("{}_{},{},{},{},{},{},{},{},{},{},{}".format(
        log_name,
        method,
        symmetric_mean_absolute_percentage_error(raw_processing_times, estimated_processing_times),
        mean_absolute_percentage_error(raw_processing_times, estimated_processing_times),
        mean_absolute_error(raw_processing_times, estimated_processing_times),
        len(estimated_event_log),
        ((estimated_event_log[DEFAULT_CSV_IDS.start_time] == estimated_event_log[DEFAULT_CSV_IDS.enabled_time]) &
         (estimated_event_log[DEFAULT_CSV_IDS.start_time] != estimated_event_log[DEFAULT_CSV_IDS.available_time])).sum(),
        ((estimated_event_log[DEFAULT_CSV_IDS.start_time] == estimated_event_log[DEFAULT_CSV_IDS.available_time]) &
         (estimated_event_log[DEFAULT_CSV_IDS.start_time] != estimated_event_log[DEFAULT_CSV_IDS.enabled_time])).sum(),
        ((estimated_event_log[DEFAULT_CSV_IDS.start_time] != estimated_event_log[DEFAULT_CSV_IDS.available_time]) &
         (estimated_event_log[DEFAULT_CSV_IDS.start_time] != estimated_event_log[DEFAULT_CSV_IDS.enabled_time])).sum(),
        (raw_minus_estimated > 0).sum(),
        (raw_minus_estimated < 0).sum(),
        (raw_minus_estimated == 0).sum()
    ))


def symmetric_mean_absolute_percentage_error(actual, forecast) -> float:
    return np.sum(2 * np.abs(forecast - actual) / (np.abs(actual) + np.abs(forecast))) / len(actual)


def mean_absolute_percentage_error(actual, forecast) -> float:
    return np.sum(np.abs(forecast - actual) / np.abs(actual)) / len(actual)


def mean_idle_multitasking_times(event_log: pd.DataFrame) -> (float, float):
    abs_idle_times = []
    abs_multi_times = []
    total_times = []
    for (resource, events) in event_log.groupby([DEFAULT_CSV_IDS.resource]):
        start_times = events[DEFAULT_CSV_IDS.start_time].to_frame().rename(columns={DEFAULT_CSV_IDS.start_time: 'time'})
        start_times['lifecycle'] = 'start'
        end_times = events[DEFAULT_CSV_IDS.end_time].to_frame().rename(columns={DEFAULT_CSV_IDS.end_time: 'time'})
        end_times['lifecycle'] = 'end'
        times = start_times.append(end_times).sort_values(['time', 'lifecycle'], ascending=[True, False])
        counter = 0
        idle_time = timedelta(0)
        multi_time = timedelta(0)
        start_idle_time = times['time'].min()
        total_time = times['time'].max() - times['time'].min()
        for time, lifecycle in times.itertuples(index=False):
            if lifecycle == 'start':
                counter += 1
                if counter == 1:
                    # Idle time has finished
                    idle_time += time - start_idle_time
                elif counter == 2:
                    # Multitasking time starting
                    start_multi_time = time
            else:
                counter -= 1
                if counter < 0:
                    print("Error, wrong sorting, ending an activity without having another one started.")
                elif counter == 0:
                    # Idle time starts again
                    start_idle_time = time
                elif counter == 1:
                    # Ends multitasking time
                    multi_time += time - start_multi_time
        abs_idle_times += [idle_time]
        abs_multi_times += [multi_time]
        total_times += [total_time]

    return (sum(abs_idle_times, timedelta(0)) / sum(total_times, timedelta(0)),
            sum(abs_multi_times, timedelta(0)) / sum(total_times, timedelta(0)))


def measure_simulation():
    techniques = ["heur_median", "heur_median_2", "heur_median_5",
                  "heur_mode", "heur_mode_2", "heur_mode_5"]
    print("emd")
    for log_name in logs:
        raw_event_log = read_and_preprocess_log(raw_path.format(log_name))
        for technique in techniques:
            calculate_simulation_stats(log_name, technique, raw_event_log)


def calculate_simulation_stats(log_name: str, method: str, raw_event_log: pd.DataFrame):
    # Measure stats for estimated log
    simulated_event_log = read_and_preprocess_log(raw_path.format(method + "/" + log_name + "_estimated"))
    emd = earth_movers_distance(raw_event_log, simulated_event_log)
    print(emd)


def earth_movers_distance(event_log_1: pd.DataFrame, event_log_2: pd.DataFrame) -> float:
    # Get the first date of the log
    interval_start = min(event_log_1['start_time'].min(), event_log_2['start_time'].min())
    interval_end = max(event_log_1['end_time'].max(), event_log_2['end_time'].max())
    # Create empty histograms
    histogram_1 = [0] * math.floor((interval_end - interval_start).total_seconds() / 3600)
    histogram_2 = histogram_1.copy()
    # Increase, for each time, the value in its corresponding bin
    absolute_hours = [difference.total_seconds() / 3600 for difference in (event_log_1['start_time'] - interval_start)]
    absolute_hours += [difference.total_seconds() / 3600 for difference in (event_log_1['end_time'] - interval_start)]
    for absolute_hour, frequency in Counter(absolute_hours).items():
        histogram_1[absolute_hour] = frequency
    absolute_hours = [difference.total_seconds() / 3600 for difference in (event_log_2['start_time'] - interval_start)]
    absolute_hours += [difference.total_seconds() / 3600 for difference in (event_log_2['end_time'] - interval_start)]
    for absolute_hour, frequency in Counter(absolute_hours).items():
        histogram_2[absolute_hour] = frequency

    return wasserstein_distance(histogram_1, histogram_2)


if __name__ == '__main__':
    measure_estimation()
