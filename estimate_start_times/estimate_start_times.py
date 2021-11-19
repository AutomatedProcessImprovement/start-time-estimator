from datetime import timedelta
from statistics import mode
from typing import Union

import numpy as np
import pandas as pd
from pm4py.objects.log.obj import EventLog

from common import EventLogType
from config import ConcurrencyOracleType, ReEstimationMethod, ResourceAvailabilityType
from data_frame.concurrency_oracle import AlphaConcurrencyOracle as DFAlphaConcurrencyOracle
from data_frame.concurrency_oracle import HeuristicsConcurrencyOracle as DFHeuristicsConcurrencyOracle
from data_frame.concurrency_oracle import NoConcurrencyOracle as DFNoConcurrencyOracle
from data_frame.resource_availability import SimpleResourceAvailability as DFSimpleResourceAvailability
from event_log.concurrency_oracle import AlphaConcurrencyOracle as ELAlphaConcurrencyOracle
from event_log.concurrency_oracle import HeuristicsConcurrencyOracle as ELHeuristicsConcurrencyOracle
from event_log.concurrency_oracle import NoConcurrencyOracle as ELNoConcurrencyOracle
from event_log.resource_availability import SimpleResourceAvailability as ELSimpleResourceAvailability


class StartTimeEstimator:
    def __init__(self, event_log, config):
        # Set event log
        self.event_log = event_log
        # Set configuration
        self.config = config
        # Set parameters
        if type(self.event_log) is EventLog:
            # Set type of event log
            self.event_log_type = EventLogType.EVENT_LOG
            # Set concurrency oracle
            if self.config.concurrency_oracle_type == ConcurrencyOracleType.NONE:
                self.concurrency_oracle = ELNoConcurrencyOracle(self.event_log, self.config)
            elif self.config.concurrency_oracle_type == ConcurrencyOracleType.ALPHA:
                self.concurrency_oracle = ELAlphaConcurrencyOracle(self.event_log, self.config)
            elif self.config.concurrency_oracle_type == ConcurrencyOracleType.HEURISTICS:
                self.concurrency_oracle = ELHeuristicsConcurrencyOracle(self.event_log, self.config)
            else:
                raise ValueError("No concurrency oracle defined!")
            # Set resource availability
            if self.config.resource_availability_type == ResourceAvailabilityType.SIMPLE:
                self.resource_availability = ELSimpleResourceAvailability(self.event_log, self.config)
            else:
                raise ValueError("No resource availability defined!")
        elif type(self.event_log) is pd.DataFrame:
            self.event_log_type = EventLogType.DATA_FRAME
            # Set concurrency oracle
            if self.config.concurrency_oracle_type == ConcurrencyOracleType.NONE:
                self.concurrency_oracle = DFNoConcurrencyOracle(self.event_log, self.config)
            elif self.config.concurrency_oracle_type == ConcurrencyOracleType.ALPHA:
                self.concurrency_oracle = DFAlphaConcurrencyOracle(self.event_log, self.config)
            elif self.config.concurrency_oracle_type == ConcurrencyOracleType.HEURISTICS:
                self.concurrency_oracle = DFHeuristicsConcurrencyOracle(self.event_log, self.config)
            else:
                raise ValueError("No concurrency oracle defined!")
            # Set resource availability
            if self.config.resource_availability_type == ResourceAvailabilityType.SIMPLE:
                self.resource_availability = DFSimpleResourceAvailability(self.event_log, self.config)
            else:
                raise ValueError("No resource availability defined!")
        else:
            raise ValueError("Unrecognizable event log instance!! Only Pandas-DataFrame and PM4PY-EventLog are supported.")

    def estimate(self) -> Union[EventLog, pd.DataFrame]:
        if self.event_log_type == EventLogType.DATA_FRAME:
            self._estimate_data_frame()
        else:
            self._estimate_event_log()
        return self.event_log

    def _estimate_data_frame(self):
        # If there is no column for start timestamp, create it
        if self.config.log_ids.start_timestamp not in self.event_log.columns:
            self.event_log[self.config.log_ids.start_timestamp] = pd.NaT
        # Process instant activities
        self.event_log[self.config.log_ids.start_timestamp] = np.where(
            self.event_log[self.config.log_ids.activity].isin(self.config.instant_activities),
            self.event_log[self.config.log_ids.end_timestamp],
            self.event_log[self.config.log_ids.start_timestamp]
        )
        # Assign start timestamps
        for (key, trace) in self.event_log.groupby([self.config.log_ids.case]):
            for index, event in trace.iterrows():
                if pd.isnull(event[self.config.log_ids.start_timestamp]):
                    enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                    available_time = self.resource_availability.available_since(
                        event[self.config.log_ids.resource],
                        event[self.config.log_ids.end_timestamp]
                    )
                    self.event_log.loc[index, self.config.log_ids.start_timestamp] = max(enabled_time, available_time)
        # Fix start time of those events for which it could not be estimated (with [config.non_estimated_time])
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            self._set_instant_non_estimated_start_times()
        else:
            self._re_estimate_non_estimated_start_times()

    def _estimate_event_log(self):
        # Assign start timestamps
        for trace in self.event_log:
            for event in trace:
                if event[self.config.log_ids.activity] in self.config.instant_activities:
                    # Process events of instant activities
                    event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp]
                elif self.config.log_ids.start_timestamp not in event:
                    # Estimate start time for non-instant events without start time
                    enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                    available_time = self.resource_availability.available_since(
                        event[self.config.log_ids.resource],
                        event[self.config.log_ids.end_timestamp]
                    )
                    event[self.config.log_ids.start_timestamp] = max(enabled_time, available_time)
        # Fix start time of those events for which it could not be estimated (with [config.non_estimated_time])
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            self._set_instant_non_estimated_start_times()
        else:
            self._re_estimate_non_estimated_start_times()

    def _set_instant_non_estimated_start_times(self):
        # Identify events with non_estimated as start time
        # and set their processing time to instant
        if self.event_log_type == EventLogType.DATA_FRAME:
            self.event_log[self.config.log_ids.start_timestamp] = np.where(
                self.event_log[self.config.log_ids.start_timestamp] == self.config.non_estimated_time,
                self.event_log[self.config.log_ids.end_timestamp],
                self.event_log[self.config.log_ids.start_timestamp]
            )
        elif self.event_log_type == EventLogType.EVENT_LOG:
            for trace in self.event_log:
                for event in trace:
                    if event[self.config.log_ids.start_timestamp] == self.config.non_estimated_time:
                        event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp]

    def _re_estimate_non_estimated_start_times(self):
        if self.event_log_type == EventLogType.DATA_FRAME:
            # Store the durations of the estimated ones
            activity_processing_times = self.event_log[
                self.event_log[self.config.log_ids.start_timestamp] != self.config.non_estimated_time] \
                .groupby([self.config.log_ids.activity]) \
                .apply(lambda row: row[self.config.log_ids.end_timestamp] - row[self.config.log_ids.start_timestamp])
            # Identify events with non_estimated as start time
            non_estimated_events = self.event_log[self.event_log[self.config.log_ids.start_timestamp] == self.config.non_estimated_time]
            for index, non_estimated_event in non_estimated_events.iterrows():
                activity = non_estimated_event[self.config.log_ids.activity]
                if activity in activity_processing_times:
                    processing_time = self._get_processing_time(activity_processing_times, activity)
                    self.event_log.loc[index, self.config.log_ids.start_timestamp] = \
                        non_estimated_event[self.config.log_ids.end_timestamp] - processing_time
        elif self.event_log_type == EventLogType.EVENT_LOG:
            # Store the durations of the estimated ones
            non_estimated_events = []
            activity_processing_times = {}
            for trace in self.event_log:
                for event in trace:
                    if event[self.config.log_ids.start_timestamp] == self.config.non_estimated_time:
                        # Non-estimated, save event to estimate based on statistics
                        non_estimated_events += [event]
                    else:
                        # Estimated, store estimated time to calculate statistics
                        activity = event[self.config.log_ids.activity]
                        processing_time = event[self.config.log_ids.end_timestamp] - event[self.config.log_ids.start_timestamp]
                        if activity not in activity_processing_times:
                            activity_processing_times[activity] = [processing_time]
                        else:
                            activity_processing_times[activity] += [processing_time]
            # Set as start time the end time minus a statistic of the processing times (mean/mode/median)
            for event in non_estimated_events:
                activity = event[self.config.log_ids.activity]
                if activity in activity_processing_times:
                    processing_time = self._get_processing_time(activity_processing_times, activity)
                    event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp] - processing_time

    def _get_processing_time(self, activity_processing_times, activity):
        if activity in activity_processing_times:
            # There have been measured other processing times for the activity, take specified statistic
            if self.config.re_estimation_method == ReEstimationMethod.MODE:
                return mode(activity_processing_times[activity])
            elif self.config.re_estimation_method == ReEstimationMethod.MEDIAN:
                return np.median(activity_processing_times[activity])
            elif self.config.re_estimation_method == ReEstimationMethod.MEAN:
                return np.mean(activity_processing_times[activity])
            else:
                raise ValueError("Unselected re-estimation method for events with non-estimated start time!")
        else:
            # There are not other measures for the processing times of the activity, set instant (processing time = 0)
            return timedelta(0)
