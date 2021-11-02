from config import Configuration, DEFAULT_CSV_IDS, ReEstimationMethod
from data_frame.concurrency_oracle import AlphaConcurrencyOracle
from data_frame.estimate_start_times import estimate_start_timestamps
from data_frame.resource_availability import ResourceAvailability
from event_log_readers import read_csv_log


def main(event_log_path) -> None:
    # Configuration
    config = Configuration(
        log_ids=DEFAULT_CSV_IDS,
        re_estimation_method=ReEstimationMethod.MODE
    )
    # Read event log
    event_log = read_csv_log(event_log_path, config)
    # Build concurrency oracle
    concurrency_oracle = AlphaConcurrencyOracle(event_log, config)
    # Build resource schedule
    resource_availability = ResourceAvailability(event_log, config)
    # Infer start timestamps
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle, resource_availability, config)
    # Export event log
    extended_event_log.to_csv("./assets/test.csv", index=False)
    # xes_exporter.apply(extended_event_log, "../assets/extended_event_log.xes")


if __name__ == '__main__':
    main("test/assets/test_event_log_1.csv")
