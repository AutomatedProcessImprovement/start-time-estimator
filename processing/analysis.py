from datetime import timedelta

import pandas as pd
import pytz
from numpy import mean

from estimate_start_times.config import DEFAULT_CSV_IDS, EventLogIDs
from start_time_metrics import read_and_preprocess_log

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


def analyze_raw_logs():
    for log_name in logs:
        raw_event_log = read_and_preprocess_log(raw_path.format(log_name), DEFAULT_CSV_IDS)

        parallelism = percentage_of_parallelism(raw_event_log, DEFAULT_CSV_IDS)
        idle, multi, rel_multi = mean_idle_multitasking_times(raw_event_log, DEFAULT_CSV_IDS)
        print("{}\t{:.2f}\t{:.2f}\t{:.2f}\t{:.2f}".format(
            log_name, round(idle, 2), round(multi, 2), round(rel_multi, 2), round(parallelism, 2))
        )


def percentage_of_parallelism(event_log: pd.DataFrame, log_ids: EventLogIDs) -> float:
    parallel_count = 0
    for resource, events in event_log.groupby([log_ids.resource]):
        for index, event in events.iterrows():
            parallel_activities = events[(events[log_ids.start_time] < event[log_ids.end_time]) &
                                         (events[log_ids.end_time] > event[log_ids.start_time])]
            if len(parallel_activities) > 1:
                parallel_count += 1
    return parallel_count / len(event_log)


def mean_idle_multitasking_times(event_log: pd.DataFrame, log_ids: EventLogIDs) -> (float, float, float):
    abs_idle_times = []
    abs_multi_times = []
    processing_times = []
    total_times = []
    for (resource, events) in event_log.groupby([log_ids.resource]):
        start_times = events[log_ids.start_time].to_frame().rename(columns={log_ids.start_time: 'time'})
        start_times['lifecycle'] = 'start'
        end_times = events[log_ids.end_time].to_frame().rename(columns={log_ids.end_time: 'time'})
        end_times['lifecycle'] = 'end'
        times = start_times.append(end_times).sort_values(['time', 'lifecycle'], ascending=[True, False])
        counter = 0
        idle_time = timedelta(0)
        multi_time = timedelta(0)
        processing_time = timedelta(0)
        start_idle_time = times['time'].min()
        total_time = times['time'].max() - times['time'].min()
        for time, lifecycle in times.itertuples(index=False):
            if lifecycle == 'start':
                counter += 1
                if counter == 1:
                    # Idle time has finished
                    idle_time += time - start_idle_time
                    # Processing time starts
                    start_processing_time = time
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
                    # Processing time ends
                    processing_time += time - start_processing_time
                elif counter == 1:
                    # Ends multitasking time
                    multi_time += time - start_multi_time
        abs_idle_times += [idle_time]
        abs_multi_times += [multi_time]
        processing_times += [processing_time]
        total_times += [total_time]

    # Return i) the idle time w.r.t. the time since the start until the end of the process
    return (sum(abs_idle_times, timedelta(0)) / sum(total_times, timedelta(0)),
            # ii) The multitasking time w.r.t. the time since the start until the end of the process
            sum(abs_multi_times, timedelta(0)) / sum(total_times, timedelta(0)),
            # iii) The multitasking time w.r.t. the working time (time in which something has been processed in the process)
            sum(abs_multi_times, timedelta(0)) / sum(processing_times, timedelta(0)))


def analyze_results():
    techniques = ["heur_median", "heur_median_2", "heur_median_5",
                  "heur_mode", "heur_mode_2", "heur_mode_5",
                  "df_median", "df_median_2", "df_median_5",
                  "df_mode", "df_mode_2", "df_mode_5"]
    for log_name in logs:
        print("\n\n" + log_name)
        raw_event_log = read_and_preprocess_log(raw_path.format(log_name), DEFAULT_CSV_IDS)
        for technique in techniques:
            print("\t" + technique)
            analyze_estimated_log(log_name, technique, DEFAULT_CSV_IDS, raw_event_log)


def analyze_estimated_log(log_name: str, method: str, log_ids: EventLogIDs, original_log: pd.DataFrame):
    # Measure stats for estimated log
    estimated_event_log = read_and_preprocess_log(raw_path.format(method + "/" + log_name + "_estimated"), log_ids)
    enabled_chosen = (
            (estimated_event_log[log_ids.enabled_time] == estimated_event_log[log_ids.start_time]) &
            (estimated_event_log[log_ids.available_time] != estimated_event_log[log_ids.start_time]) &
            (estimated_event_log[log_ids.available_time] != (pd.Timestamp.min.tz_localize(tz=pytz.UTC) + timedelta(seconds=1)).floor(
                freq='ms'))
    )
    print("\t\tNumber of enabled chosen: {} from {}".format(sum(enabled_chosen), len(estimated_event_log)))
    differences_available = abs(
        (estimated_event_log[enabled_chosen][log_ids.available_time] - original_log[enabled_chosen][log_ids.start_time])
    )
    differences_enabled = abs(
        (estimated_event_log[enabled_chosen][log_ids.enabled_time] - original_log[enabled_chosen][log_ids.start_time])
    )
    print("\t\tMean difference availab: {}".format(mean(differences_available)))
    print("\t\tMean difference enabled: {}".format(mean(differences_enabled)))


if __name__ == '__main__':
    analyze_raw_logs()
