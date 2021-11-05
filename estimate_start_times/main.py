import time

from config import Configuration, DEFAULT_CSV_IDS, ReEstimationMethod, ConcurrencyOracleType, ResourceAvailabilityType
from estimate_start_times import StartTimeEstimator
from event_log_readers import read_csv_log


def main(event_log_path) -> None:
    # Configuration
    config = Configuration(
        log_ids=DEFAULT_CSV_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.ALPHA,
        resource_availability_type=ResourceAvailabilityType.SIMPLE
    )
    # Read event log
    event_log = read_csv_log(event_log_path, config)
    print("Starting start time estimation.")
    start_time = time.process_time()
    # Create start time estimator
    start_time_estimator = StartTimeEstimator(event_log, config)
    # Estimate start times
    extended_event_log = start_time_estimator.estimate()
    end_time = time.process_time()
    print("Estimation finished ({}s).".format(end_time - start_time))
    # Export event log
    extended_event_log.to_csv(event_log_path.replace('.csv', '_estimated.csv'), index=False)


if __name__ == '__main__':
    main("../event_logs/BPI_Challenge_2017_filtered.csv")
