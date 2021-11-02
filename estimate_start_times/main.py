from config import Configuration, DEFAULT_CSV_IDS, ReEstimationMethod
from estimate_start_times import StartTimeEstimator
from event_log_readers import read_csv_log


def main(event_log_path) -> None:
    # Configuration
    config = Configuration(
        log_ids=DEFAULT_CSV_IDS,
        re_estimation_method=ReEstimationMethod.MODE
    )
    # Read event log
    event_log = read_csv_log(event_log_path, config)
    # Create start time estimator
    start_time_estimator = StartTimeEstimator(event_log, config)
    extended_event_log = start_time_estimator.estimate()
    # Export event log
    extended_event_log.to_csv("./assets/test.csv", index=False)
    # xes_exporter.apply(extended_event_log, "../assets/extended_event_log.xes")


if __name__ == '__main__':
    main("test/assets/test_event_log_1.csv")
