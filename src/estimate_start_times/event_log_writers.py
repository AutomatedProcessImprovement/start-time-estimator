from typing import Union

import pandas as pd
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.obj import EventLog


def write_event_log(event_log: Union[EventLog, pd.DataFrame], log_path: str) -> None:
    if type(event_log) is EventLog:
        write_xes_log(event_log, log_path)
    elif type(event_log) is pd.DataFrame:
        write_csv_log(event_log, log_path)
    else:
        raise ValueError("Unknown event log file type! Only [PM4PY.EventLog] and [pandas.DataFrame] supported.")


def write_xes_log(event_log: EventLog, log_path: str) -> None:
    xes_exporter.apply(event_log, log_path)


def write_csv_log(event_log: pd.DataFrame, log_path: str) -> None:
    if log_path.endswith(".gz"):
        event_log.to_csv(log_path, encoding='utf-8', index=False, compression='gzip')
    else:
        event_log.to_csv(log_path, encoding='utf-8', index=False)
