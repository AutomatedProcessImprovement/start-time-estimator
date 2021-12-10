import numpy as np
import pandas as pd
import pm4py
from pm4py.algo.filtering.log.attributes import attributes_filter

from transformation import export_event_log_only_millis


def preprocess_consulta_data_mining():
    # Read event log
    event_log = pm4py.read_xes("../event_logs/ConsultaDataMining201618.xes.gz")
    # Remove artificial "Start" and "End" activities
    event_log = attributes_filter.apply_events(
        event_log,
        ["Start", "End"],
        parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: 'concept:name', attributes_filter.Parameters.POSITIVE: False}
    )
    # Export ensuring the timestamps has only milliseconds, not microseconds
    export_event_log_only_millis(event_log, "../event_logs/ConsultaDataMining201618_preproc.xes")


def preprocess_production():
    # Read event log
    event_log = pm4py.read_xes("../event_logs/Production.xes.gz")
    # Remove artificial "Start" and "End" activities
    event_log = attributes_filter.apply_events(
        event_log,
        ["Start", "End"],
        parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: 'concept:name', attributes_filter.Parameters.POSITIVE: False}
    )
    # Export ensuring the timestamps has only milliseconds, not microseconds
    export_event_log_only_millis(event_log, "../event_logs/Production_preproc.xes")


def preprocess_pharmacy():
    # Read event log
    event_log = pm4py.read_xes("../event_logs/cvs_pharmacy.xes.gz")
    # Remove events with lifecycle != 'start' or 'complete'
    event_log = attributes_filter.apply_events(
        event_log,
        ["start", "complete"],
        parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: 'lifecycle:transition', attributes_filter.Parameters.POSITIVE: True}
    )
    # Export ensuring the timestamps has only milliseconds, not microseconds
    export_event_log_only_millis(event_log, "../event_logs/cvs_pharmacy_preproc.xes")


def preprocess_confidential():
    # Read event log
    event_log = pd.read_csv("../event_logs/confidential_old.csv.gz")
    # Make instant the 'message' events
    instant_events = ["M1", "M2", "M3", "M4", "M5", "M6"]
    event_log['start_time'] = np.where(
        event_log['Activity'].isin(instant_events),
        event_log['end_time'],
        event_log['start_time']
    )
    # Export ensuring the timestamps has only milliseconds, not microseconds
    event_log.to_csv("../event_logs/confidential.csv.gz", encoding='utf-8', index=False, compression='gzip')


if __name__ == '__main__':
    pass
