from statistics import mode

import numpy as np
import pandas as pd

from config import ConcurrencyOracleType, ReEstimationMethod, ResourceAvailabilityType
from data_frame.concurrency_oracle import NoConcurrencyOracle, AlphaConcurrencyOracle
from data_frame.resource_availability import ResourceAvailability


class StartTimeEstimator:
    def __init__(self, event_log, config):
        # Set event log
        self.event_log = event_log
        # Set configuration
        self.config = config
        # Set concurrency oracle
        if config.concurrency_oracle_type == ConcurrencyOracleType.NONE:
            self.concurrency_oracle = NoConcurrencyOracle(event_log, config)
        elif config.concurrency_oracle_type == ConcurrencyOracleType.ALPHA:
            self.concurrency_oracle = AlphaConcurrencyOracle(event_log, config)
        else:
            print("No concurrency oracle defined! Setting Alpha as default.")
            self.concurrency_oracle = AlphaConcurrencyOracle(event_log, config)
        # Set resource availability
        if config.resource_availability_type == ResourceAvailabilityType.SIMPLE:
            self.resource_availability = ResourceAvailability(event_log, config)
        else:
            print("No resource availability defined! Setting Simple as default.")
            self.resource_availability = ResourceAvailability(event_log, config)

    def estimate(self) -> pd.DataFrame:
        # If there is not column for start timestamp, create it
        if self.config.log_ids.start_timestamp not in self.event_log.columns:
            self.event_log[self.config.log_ids.start_timestamp] = pd.NaT
        # Assign start timestamps
        for (key, trace) in self.event_log.groupby([self.config.log_ids.case]):
            for index, event in trace.iterrows():
                enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                available_time = self.resource_availability.available_since(
                    event[self.config.log_ids.resource],
                    event[self.config.log_ids.end_timestamp]
                )
                self.event_log.loc[index, self.config.log_ids.start_timestamp] = max(enabled_time, available_time)
        # Fix start times for those events being the first one of the trace and the resource (with non_estimated_time)
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            estimated_event_log = self._set_instant_non_estimated_start_times()
        elif self.config.re_estimation_method == ReEstimationMethod.MODE:
            estimated_event_log = self._re_estimate_non_estimated_start_times()
        else:
            print("Unselected re-estimation method for events with no estimated start time! Setting them as instant by default.")
            estimated_event_log = self._set_instant_non_estimated_start_times()
        # Return modified event log
        return estimated_event_log

    def _set_instant_non_estimated_start_times(self) -> pd.DataFrame:
        # Identify events with non_estimated as start time
        # and set their processing time to instant
        self.event_log[self.config.log_ids.start_timestamp] = np.where(
            self.event_log[self.config.log_ids.start_timestamp] == self.config.non_estimated_time,
            self.event_log[self.config.log_ids.end_timestamp],
            self.event_log[self.config.log_ids.start_timestamp]
        )
        # Return modified event log
        return self.event_log

    def _re_estimate_non_estimated_start_times(self) -> pd.DataFrame:
        # Store the durations of the estimated ones
        activity_processing_times = self.event_log[self.event_log[self.config.log_ids.start_timestamp] != self.config.non_estimated_time] \
            .groupby([self.config.log_ids.activity]) \
            .apply(lambda row: row[self.config.log_ids.end_timestamp] - row[self.config.log_ids.start_timestamp])
        # Identify events with non_estimated as start time
        non_estimated_events = self.event_log[self.event_log[self.config.log_ids.start_timestamp] == self.config.non_estimated_time]
        for index, non_estimated_event in non_estimated_events.iterrows():
            activity = non_estimated_event[self.config.log_ids.activity]
            if activity in activity_processing_times:
                self.event_log.loc[index, self.config.log_ids.start_timestamp] = \
                    non_estimated_event[self.config.log_ids.end_timestamp] - mode(activity_processing_times[activity])
            else:
                # If this activity has no estimated times set as instant activity
                self.event_log.loc[index, self.config.log_ids.start_timestamp] = self.event_log.loc[
                    index, self.config.log_ids.end_timestamp]
        # Return modified event log
        return self.event_log
