import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter

from config import Configuration, DEFAULT_XES_IDS
from event_log_readers import read_xes_log


def main(event_log_path) -> None:
    # Configuration
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    # Read event log
    event_log = read_xes_log(event_log_path, config)
    # Process it
    processed_event_log = process_bpic_2017(
        log_converter.apply(event_log, variant=log_converter.Variants.TO_DATA_FRAME)
    )
    # Export event log
    processed_event_log.to_csv(event_log_path.replace('.xes.gz', '_filtered.csv'), index=False)


def process_bpic_2017(event_log: pd.DataFrame) -> pd.DataFrame:
    # Rename columns
    event_log = event_log.rename(columns={
        'case:concept:name': 'case',
        'concept:name': 'activity',
        'time:timestamp': 'end_timestamp',
        'lifecycle:transition': 'lifecycle',
        'org:resource': 'resource'
    })
    # Sort first by case, and then by timestamp
    event_log = event_log.sort_values(by=['case', 'end_timestamp'])
    # Retain only the W_* events, those which have 'start' and 'complete' lifecycles
    event_log = event_log[event_log['activity'].astype(str).str[0] == 'W']
    # Rename the 'ate_abort' activities to distinguish them from the correct 'complete' ending
    event_log.loc[event_log['lifecycle'] == 'ate_abort', 'activity'] = event_log['activity'] + ' (aborted)'
    # Transform the 'ate_abort' lifecycle into 'complete', as they have finished, with abort status, but finished
    event_log.loc[event_log['lifecycle'] == 'ate_abort', 'lifecycle'] = 'complete'
    # Retain only events with 'start' or 'complete' lifecycles
    event_log = event_log[(event_log['lifecycle'] == 'start') | (event_log['lifecycle'] == 'complete')]
    # Add as 'start_lifecycle' to each event the 'end_lifecycle' of the previous one
    event_log['start_timestamp'] = event_log['end_timestamp'].shift(1)
    # Retain only the 'complete' ones (they have the start added)
    event_log = event_log[event_log['lifecycle'] == 'complete']
    # Retain only needed columns
    event_log = event_log[['case', 'activity', 'start_timestamp', 'end_timestamp', 'resource']]
    # Sort again by end timestamp
    event_log = event_log.sort_values(by=['end_timestamp'])
    # Return processed event log
    return event_log


if __name__ == '__main__':
    main("../event_logs/BPI_Challenge_2017.xes.gz")
