import enum
from dataclasses import dataclass, field
from datetime import datetime

import pytz


class ReEstimationMethod(enum.Enum):
    SET_INSTANT = 1
    MODE = 2
    MEDIAN = 3
    MEAN = 4


class OutlierStatistic(enum.Enum):
    MODE = 1
    MEDIAN = 2
    MEAN = 3


class ConcurrencyOracleType(enum.Enum):
    DEACTIVATED = 1
    NONE = 2
    ALPHA = 3
    HEURISTICS = 4


class ResourceAvailabilityType(enum.Enum):
    SIMPLE = 1  # Consider all the events that each resource performs
    WITH_CALENDAR = 2  # Future possibility considering also the resource calendars and non-working days


@dataclass
class EventLogIDs:
    case: str = 'case'
    activity: str = 'activity'
    start_timestamp: str = 'start_timestamp'
    end_timestamp: str = 'end_timestamp'
    resource: str = 'resource'
    lifecycle: str = 'lifecycle'


DEFAULT_CSV_IDS = EventLogIDs(case='case_id',
                              activity='Activity',
                              start_timestamp='start_time',
                              end_timestamp='end_time',
                              resource='Resource',
                              lifecycle='Lifecycle')
DEFAULT_XES_IDS = EventLogIDs(case='concept:name',
                              activity='concept:name',
                              start_timestamp='time:start',
                              end_timestamp='time:timestamp',
                              resource='org:resource',
                              lifecycle='lifecycle:transition')


@dataclass
class HeuristicsThresholds:
    df: float = 0.9
    l2l: float = 0.9
    l1l: float = 0.9


@dataclass
class Configuration:
    """Class storing the configuration parameters for the start time estimation.

    Attributes:
        log_ids                     Identifiers for each key element (e.g. executed activity or resource).
        concurrency_oracle_type     Concurrency oracle to use (e.g. heuristics miner's concurrency oracle).
        resource_availability_type  Resource availability engine to use (e.g. using resource calendars).
        missing_resource            String to identify the events with missing resource (it is avoided in the resource availability
                                    calculation).
        non_estimated_time          Time to use as value when the start time cannot be estimated (later re-estimated with
                                    [re_estimation_method].
        re_estimation_method        Method (e.g. median) to re-estimate the start times that couldn't be estimated due to lack of resource
                                    availability and causal predecessors.
        bot_resources               Set of resource IDs corresponding bots, in order to set their events as instant.
        instant_activities          Set of instantaneous activities, in order to set their events as instant.
        heuristics_thresholds       Thresholds for the heuristics concurrency oracle (only used is this oracle is selected as
                                    [concurrency_oracle_type].
        outlier_statistic           Statistic (e.g. median) to calculate from the distribution of each activity durations to consider and
                                    re-estimate the outlier events which estimated duration is higher.
        outlier_threshold           Threshold to control outliers, those events with estimated durations over
    """
    log_ids: EventLogIDs = DEFAULT_CSV_IDS
    concurrency_oracle_type: ConcurrencyOracleType = ConcurrencyOracleType.ALPHA
    resource_availability_type: ResourceAvailabilityType = ResourceAvailabilityType.SIMPLE
    missing_resource: str = "NOT_SET"
    non_estimated_time: datetime = datetime.min.replace(tzinfo=pytz.UTC)
    re_estimation_method: ReEstimationMethod = ReEstimationMethod.MEDIAN
    bot_resources: set = field(default_factory=set)
    instant_activities: set = field(default_factory=set)
    heuristics_thresholds: HeuristicsThresholds = HeuristicsThresholds()
    outlier_statistic: OutlierStatistic = OutlierStatistic.MEDIAN
    outlier_threshold: float = float('nan')
