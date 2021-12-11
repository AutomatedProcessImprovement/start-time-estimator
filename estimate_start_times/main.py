import time

import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter

from config import Configuration, DEFAULT_XES_IDS, ReEstimationMethod, ConcurrencyOracleType, ResourceAvailabilityType, \
    HeuristicsThresholds, OutlierStatistic, DEFAULT_CSV_IDS
from estimate_start_times import StartTimeEstimator
from event_log_readers import read_event_log
from event_log_writers import write_event_log


def run_estimation(event_log_path, configuration, output_log_path):
    print("\nProcessing event log {}".format(event_log_path))
    # Read event log
    event_log = read_event_log(event_log_path, configuration)
    # Process event log
    print("Starting start time estimation.")
    start_time = time.process_time()
    # Create start time estimator
    start_time_estimator = StartTimeEstimator(event_log, configuration)
    # Estimate start times
    extended_event_log = start_time_estimator.estimate()
    end_time = time.process_time()
    print("Estimation finished ({}s).".format(end_time - start_time))
    # Export as csv
    extended_event_log = log_converter.apply(extended_event_log, variant=log_converter.Variants.TO_DATA_FRAME)
    extended_event_log = extended_event_log[[
        'case:{}'.format(configuration.log_ids.case),
        configuration.log_ids.activity,
        configuration.log_ids.start_timestamp,
        configuration.log_ids.end_timestamp,
        configuration.log_ids.enabled_time,
        configuration.log_ids.available_time,
        configuration.log_ids.resource
    ]]
    extended_event_log = extended_event_log.rename(columns={'case:{}'.format(configuration.log_ids.case): DEFAULT_CSV_IDS.case,
                                                            configuration.log_ids.activity: DEFAULT_CSV_IDS.activity,
                                                            configuration.log_ids.start_timestamp: DEFAULT_CSV_IDS.start_timestamp,
                                                            configuration.log_ids.end_timestamp: DEFAULT_CSV_IDS.end_timestamp,
                                                            configuration.log_ids.enabled_time: DEFAULT_CSV_IDS.enabled_time,
                                                            configuration.log_ids.available_time: DEFAULT_CSV_IDS.available_time,
                                                            configuration.log_ids.resource: DEFAULT_CSV_IDS.resource
                                                            })
    # Convert timestamp value to datetime
    extended_event_log[DEFAULT_CSV_IDS.start_timestamp] = pd.to_datetime(extended_event_log[DEFAULT_CSV_IDS.start_timestamp], utc=True)
    extended_event_log[DEFAULT_CSV_IDS.start_timestamp] = timestamp_to_string(extended_event_log[DEFAULT_CSV_IDS.start_timestamp])
    extended_event_log[DEFAULT_CSV_IDS.end_timestamp] = pd.to_datetime(extended_event_log[DEFAULT_CSV_IDS.end_timestamp], utc=True)
    extended_event_log[DEFAULT_CSV_IDS.end_timestamp] = timestamp_to_string(extended_event_log[DEFAULT_CSV_IDS.end_timestamp])
    extended_event_log[DEFAULT_CSV_IDS.enabled_time] = pd.to_datetime(extended_event_log[DEFAULT_CSV_IDS.enabled_time], utc=True)
    extended_event_log[DEFAULT_CSV_IDS.enabled_time] = timestamp_to_string(extended_event_log[DEFAULT_CSV_IDS.enabled_time])
    extended_event_log[DEFAULT_CSV_IDS.available_time] = pd.to_datetime(extended_event_log[DEFAULT_CSV_IDS.available_time], utc=True)
    extended_event_log[DEFAULT_CSV_IDS.available_time] = timestamp_to_string(extended_event_log[DEFAULT_CSV_IDS.available_time])
    write_event_log(extended_event_log, output_log_path)


def timestamp_to_string(dates: pd.Series) -> pd.Series:
    return (dates.apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x: x[:-3]) +
            dates.apply(lambda x: x.strftime("%z")).apply(lambda x: x[:-2]) +
            ":" +
            dates.apply(lambda x: x.strftime("%z")).apply(lambda x: x[-2:]))


def main():
    # General options
    outlier_threshold = 2.0
    folder = "heur-median-2"

    # ------------------------------------ #
    # ---------- SYNTHETIC LOGS ---------- #
    # ------------------------------------ #

    # Confidential
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        instant_activities={"M1", "M2", "M3", "M4", "M5", "M6"},
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/confidential.xes.gz", config,
                   "../event_logs/{}/confidential_estimated.csv.gz".format(folder))

    # CVS Pharmacy
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={"Pharmacy System-000001"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/cvs_pharmacy.xes.gz", config,
                   "../event_logs/{}/cvs_pharmacy_estimated.csv.gz".format(folder))

    # Loan Application
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        instant_activities={"loan application returned"},
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/Loan_Application.xes.gz", config,
                   "../event_logs/{}/Loan_Application_estimated.csv.gz".format(folder))

    # Procure to Pay
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/Procure_to_Pay.xes.gz", config,
                   "../event_logs/{}/Procure_to_Pay_estimated.csv.gz".format(folder))

    # ------------------------------- #
    # ---------- REAL LOGS ---------- #
    # ------------------------------- #

    # Application to Approval Government Agency
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/Application_to_Approval_Government_Agency.xes.gz", config,
                   "../event_logs/{}/Application_to_Approval_Government_Agency_estimated.csv.gz".format(folder))

    # BPIC 2012
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/BPI_Challenge_2012_W_Two_TS.xes.gz", config,
                   "../event_logs/{}/BPI_Challenge_2012_W_Two_TS_estimated.csv.gz".format(folder))

    # BPIC 2017
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/BPI_Challenge_2017_W_Two_TS.xes.gz", config,
                   "../event_logs/{}/BPI_Challenge_2017_W_Two_TS_estimated.csv.gz".format(folder))

    # Call centre
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/callcentre.xes.gz", config,
                   "../event_logs/{}/callcentre_estimated.csv.gz".format(folder))

    # Consulta Data Mining 2016 - 2018
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        instant_activities={"Notificacion estudiante cancelacion soli", "Traer informacion estudiante - banner"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/ConsultaDataMining201618.xes.gz", config,
                   "../event_logs/{}/ConsultaDataMining201618_estimated.csv.gz".format(folder))

    # Insurance
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/insurance.xes.gz", config,
                   "../event_logs/{}/insurance_estimated.csv.gz".format(folder))

    # POC Process Mining
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/poc_processmining.xes.gz", config,
                   "../event_logs/{}/poc_processmining_estimated.csv.gz".format(folder))

    # Production
    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9),
        outlier_statistic=OutlierStatistic.MEDIAN,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/Production.xes.gz", config,
                   "../event_logs/{}/Production_estimated.csv.gz".format(folder))


if __name__ == '__main__':
    main()
