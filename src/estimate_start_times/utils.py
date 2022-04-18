import itertools

import pandas as pd


def zip_with_next(iterable):
    # s -> (s0,s1), (s1,s2), (s2, s3), ...
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def read_csv_log(log_path, config, sort_by_end_time=True) -> pd.DataFrame:
    # Read log
    event_log = pd.read_csv(log_path)
    # If the events have a lifecycle, retain only 'complete'
    if config.log_ids.lifecycle in event_log.columns:
        event_log = event_log[event_log[config.log_ids.lifecycle] == 'complete']
    # Set case id as object
    event_log = event_log.astype({config.log_ids.case: object})
    # Fix missing resources
    if config.log_ids.resource not in event_log.columns:
        event_log[config.log_ids.resource] = config.missing_resource
    else:
        event_log[config.log_ids.resource].fillna(config.missing_resource, inplace=True)
    # Convert timestamp value to datetime
    event_log[config.log_ids.end_time] = pd.to_datetime(event_log[config.log_ids.end_time], utc=True)
    if config.log_ids.start_time in event_log.columns:
        event_log[config.log_ids.start_time] = pd.to_datetime(event_log[config.log_ids.start_time], utc=True)
    # Sort by end time
    if sort_by_end_time:
        event_log = event_log.sort_values(config.log_ids.end_time)
    # Return parsed event log
    return event_log


def write_csv_log(event_log: pd.DataFrame, log_path: str) -> None:
    if log_path.endswith(".gz"):
        event_log.to_csv(log_path, encoding='utf-8', index=False, compression='gzip')
    else:
        event_log.to_csv(log_path, encoding='utf-8', index=False)
