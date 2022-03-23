# Start Time Estimator

Python implementation of the start time estimation technique presented in the paper "Estimating Activity Start Times for Business Process Simulation", by David Chapela-Campa and Marlon Dumas.

The technique takes as input an event log (pd.DataFrame) recording the execution of the activities of a process (including resource information), and produces a version of that event log with estimated start times for each activity instance.

## Requirements

- **Python v3.9.5+**
- **PIP v21.1.2+**
- Python dependencies: The packages listed in `requirements.txt`.

## Basic Usage

Check [main file](https://github.com/AutomatedProcessImprovement/start-time-estimator/blob/main/processing/main.py) for an example of a simple execution, and [config file](https://github.com/AutomatedProcessImprovement/start-time-estimator/blob/main/src/estimate_start_times/config.py) for an explanation of the configuration parameters.

### Examples

Here we provide a simple example of use with the default configuration, followed by different custom configurations to run all the versions of the technique.

```python
# Set up default configuration
configuration = Configuration()
# Read event log
event_log = read_csv_log(
    log_path="path/to/event/log.csv.gz",
    config=configuration,
    reset_start_times=True,  # Reset all start times to estimate them all
    sort_by_end_time=True  # Sort log by end time (warning this might alter the order of the events sharing end time)
)
# Estimate start times
extended_event_log = StartTimeEstimator(event_log, configuration).estimate()
```

The column IDs for the CSV file can be customized so the implementation works correctly with them:

```python
# Set up custom configuration
configuration = Configuration(
    log_ids=EventLogIDs(
        case="case",
        activity="task",
        start_time="start",
        end_time="end",
        resource="resource"
    )
)
```

#### Configuration of the proposed approach

With no outlier threshold and using the Median as a statistic to calculate the most typical duration:

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    re_estimation_method=ReEstimationMethod.MEDIAN,
    resource_availability_type=ResourceAvailabilityType.SIMPLE
)
```

With no outlier threshold and using the Mode as a statistic to calculate the most typical duration:

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    re_estimation_method=ReEstimationMethod.MODE,
    resource_availability_type=ResourceAvailabilityType.SIMPLE
)
```

Customize the thresholds for the concurrency detection:

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    heuristics_thresholds=HeuristicsThresholds(df=0.6, l2l=0.6),
    re_estimation_method=ReEstimationMethod.MODE,
    resource_availability_type=ResourceAvailabilityType.SIMPLE
)
```

Add an outlier threshold of 200% and set the Mode to re-estimate outlier estimations too:

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    re_estimation_method=ReEstimationMethod.MODE,
    resource_availability_type=ResourceAvailabilityType.SIMPLE,
    outlier_statistic=OutlierStatistic.MODE,
    outlier_threshold=2.0
)
```

Specify *bot resources* (perform the activities instantly) and *instant activities*:

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    re_estimation_method=ReEstimationMethod.MODE,
    resource_availability_type=ResourceAvailabilityType.SIMPLE,
    bot_resources={"SYSTEM", "BOT_001"},
    instant_activities=={"Automatic Validation", "Send Notification"}
)
```

#### Configuration with a simpler concurrency oracle (Alpha Miner's) for the Enablement Time calculation

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.ALPHA,
    re_estimation_method=ReEstimationMethod.MODE,
    resource_availability_type=ResourceAvailabilityType.SIMPLE
)
```

#### Configuration with no concurrency oracle for the Enablement Time calculation (i.e. assuming directly-follows relations) 

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.NONE,
    re_estimation_method=ReEstimationMethod.MODE,
    resource_availability_type=ResourceAvailabilityType.SIMPLE
)
```

#### Configuration only taking into account the Resource Availability Time 

```python
# Set up custom configuration
configuration = Configuration(
    concurrency_oracle_type=ConcurrencyOracleType.DEACTIVATED,
    re_estimation_method=ReEstimationMethod.MODE,
    resource_availability_type=ResourceAvailabilityType.SIMPLE
)
```
