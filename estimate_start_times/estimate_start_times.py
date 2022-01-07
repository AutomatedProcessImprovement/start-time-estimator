import math
from datetime import timedelta
from statistics import mode
from typing import Union

import numpy as np
import pandas as pd
from pm4py.objects.log.obj import EventLog

from common import EventLogType
from concurrency_oracle import NoConcurrencyOracle, AlphaConcurrencyOracle, HeuristicsConcurrencyOracle, DeactivatedConcurrencyOracle
from config import ConcurrencyOracleType, ReEstimationMethod, ResourceAvailabilityType, OutlierStatistic
from resource_availability import SimpleResourceAvailability


class StartTimeEstimator:
    def __init__(self, event_log, config):
        # Set event log
        self.event_log = event_log
        # Set configuration
        self.config = config
        # Set log IDs to ease access within class
        self.log_ids = config.log_ids
        # Set event log type
        if type(self.event_log) is EventLog:
            self.event_log_type = EventLogType.EVENT_LOG
        elif type(self.event_log) is pd.DataFrame:
            self.event_log_type = EventLogType.DATA_FRAME
        else:
            raise ValueError("Unrecognizable event log instance!! Only Pandas-DataFrame and PM4PY-EventLog are supported.")
        # Set concurrency oracle
        if self.config.concurrency_oracle_type == ConcurrencyOracleType.DEACTIVATED:
            self.concurrency_oracle = DeactivatedConcurrencyOracle(self.config)
        elif self.config.concurrency_oracle_type == ConcurrencyOracleType.NONE:
            self.concurrency_oracle = NoConcurrencyOracle(self.event_log, self.config)
        elif self.config.concurrency_oracle_type == ConcurrencyOracleType.ALPHA:
            self.concurrency_oracle = AlphaConcurrencyOracle(self.event_log, self.config)
        elif self.config.concurrency_oracle_type == ConcurrencyOracleType.HEURISTICS:
            self.concurrency_oracle = HeuristicsConcurrencyOracle(self.event_log, self.config)
        else:
            raise ValueError("No concurrency oracle defined!")
        # Set resource availability
        if self.config.resource_availability_type == ResourceAvailabilityType.SIMPLE:
            self.resource_availability = SimpleResourceAvailability(self.event_log, self.config)
        else:
            raise ValueError("No resource availability defined!")

    def estimate(self) -> Union[EventLog, pd.DataFrame]:
        if self.event_log_type == EventLogType.DATA_FRAME:
            self._estimate_data_frame()
        else:
            self._estimate_event_log()
        return self.event_log

    def _estimate_data_frame(self):
        # If there is no column for start timestamp, create it
        if self.log_ids.start_time not in self.event_log.columns:
            self.event_log[self.log_ids.start_time] = pd.NaT
        # Process instant activities
        self.event_log[self.log_ids.enabled_time] = np.where(
            self.event_log[self.log_ids.activity].isin(self.config.instant_activities),
            self.event_log[self.log_ids.end_time],
            self.event_log[self.log_ids.start_time]
        )
        self.event_log[self.log_ids.available_time] = np.where(
            self.event_log[self.log_ids.activity].isin(self.config.instant_activities),
            self.event_log[self.log_ids.end_time],
            self.event_log[self.log_ids.start_time]
        )
        self.event_log[self.log_ids.start_time] = np.where(
            self.event_log[self.log_ids.activity].isin(self.config.instant_activities),
            self.event_log[self.log_ids.end_time],
            self.event_log[self.log_ids.start_time]
        )
        # Assign start timestamps
        for (key, trace) in self.event_log.groupby([self.log_ids.case]):
            for index, event in trace.iterrows():
                if pd.isnull(event[self.log_ids.start_time]):
                    enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                    available_time = self.resource_availability.available_since(event[self.log_ids.resource], event[self.log_ids.end_time])
                    self.event_log.loc[index, self.log_ids.enabled_time] = enabled_time
                    self.event_log.loc[index, self.log_ids.available_time] = available_time
                    self.event_log.loc[index, self.log_ids.start_time] = max(enabled_time, available_time)
        # Re-estimate start time of those events with an estimated duration over the threshold
        if not math.isnan(self.config.outlier_threshold):
            self._re_estimate_durations_over_threshold()
        # Fix start time of those events for which it could not be estimated (with [config.non_estimated_time])
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            self._set_instant_non_estimated_start_times()
        else:
            self._re_estimate_non_estimated_start_times()

    def _estimate_event_log(self):
        # Assign start timestamps
        for trace in self.event_log:
            for event in trace:
                if event[self.log_ids.activity] in self.config.instant_activities:
                    # Process events of instant activities
                    event[self.log_ids.enabled_time] = event[self.log_ids.end_time]
                    event[self.log_ids.available_time] = event[self.log_ids.end_time]
                    event[self.log_ids.start_time] = event[self.log_ids.end_time]
                elif self.log_ids.start_time not in event:
                    # Estimate start time for non-instant events without start time
                    enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                    available_time = self.resource_availability.available_since(event[self.log_ids.resource], event[self.log_ids.end_time])
                    event[self.log_ids.enabled_time] = enabled_time
                    event[self.log_ids.available_time] = available_time
                    event[self.log_ids.start_time] = max(enabled_time, available_time)
        # Re-estimate start time of those events with an estimated duration over the threshold
        if not math.isnan(self.config.outlier_threshold):
            self._re_estimate_durations_over_threshold()
        # Fix start time of those events for which it could not be estimated (with [config.non_estimated_time])
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            self._set_instant_non_estimated_start_times()
        else:
            self._re_estimate_non_estimated_start_times()

    def _re_estimate_durations_over_threshold(self):
        if self.event_log_type == EventLogType.DATA_FRAME:
            # Take all the estimated durations of each activity and store the specified statistic of each distribution
            statistic_durations = \
                self.event_log[self.event_log[self.log_ids.start_time] != self.config.non_estimated_time] \
                    .groupby([self.log_ids.activity]) \
                    .apply(lambda row: row[self.log_ids.end_time] - row[self.log_ids.start_time]) \
                    .groupby(level=0) \
                    .apply(lambda row: self._apply_statistic(row))
            # For each event, if the duration is over the threshold, set the defined statistic
            for index, event in self.event_log.iterrows():
                duration_limit = self.config.outlier_threshold * statistic_durations[event[self.log_ids.activity]]
                if (event[self.log_ids.start_time] != self.config.non_estimated_time and
                        (event[self.log_ids.end_time] - event[self.log_ids.start_time]) > duration_limit):
                    self.event_log.loc[index, self.log_ids.start_time] = \
                        event[self.log_ids.end_time] - duration_limit
        elif self.event_log_type == EventLogType.EVENT_LOG:
            # Take all the estimated durations of each activity
            statistic_durations = {}
            for trace in self.event_log:
                for event in trace:
                    if event[self.log_ids.start_time] != self.config.non_estimated_time:
                        # Store estimated time to calculate statistics
                        activity = event[self.log_ids.activity]
                        duration = event[self.log_ids.end_time] - event[self.log_ids.start_time]
                        statistic_durations[activity] = statistic_durations.get(activity, []) + [duration]
            # Store, for each activity, the mean/median/mode of its estimated durations
            for activity in statistic_durations:
                statistic_durations[activity] = self._apply_statistic(statistic_durations[activity])
            # For each event, if the duration is over the threshold, set the defined statistic
            for trace in self.event_log:
                for event in trace:
                    if event[self.log_ids.start_time] != self.config.non_estimated_time:
                        activity = event[self.log_ids.activity]
                        duration = event[self.log_ids.end_time] - event[self.log_ids.start_time]
                        duration_limit = self.config.outlier_threshold * statistic_durations[activity]
                        if duration > duration_limit:
                            # Set statistic
                            event[self.log_ids.start_time] = event[self.log_ids.end_time] - duration_limit

    def _set_instant_non_estimated_start_times(self):
        # Identify events with non_estimated as start time
        # and set their duration to instant
        if self.event_log_type == EventLogType.DATA_FRAME:
            self.event_log[self.log_ids.start_time] = np.where(
                self.event_log[self.log_ids.start_time] == self.config.non_estimated_time,
                self.event_log[self.log_ids.end_time],
                self.event_log[self.log_ids.start_time]
            )
        elif self.event_log_type == EventLogType.EVENT_LOG:
            for trace in self.event_log:
                for event in trace:
                    if event[self.log_ids.start_time] == self.config.non_estimated_time:
                        event[self.log_ids.start_time] = event[self.log_ids.end_time]

    def _re_estimate_non_estimated_start_times(self):
        if self.event_log_type == EventLogType.DATA_FRAME:
            # Store the durations of the estimated ones
            activity_durations = (self.event_log[self.event_log[self.log_ids.start_time] != self.config.non_estimated_time]
                                  .groupby([self.log_ids.activity])
                                  .apply(lambda row: row[self.log_ids.end_time] - row[self.log_ids.start_time]))
            # Identify events with non_estimated as start time
            non_estimated_events = self.event_log[self.event_log[self.log_ids.start_time] == self.config.non_estimated_time]
            for index, non_estimated_event in non_estimated_events.iterrows():
                activity = non_estimated_event[self.log_ids.activity]
                # Re-estimate
                duration = self._get_activity_duration(activity_durations, activity)
                self.event_log.loc[index, self.log_ids.start_time] = non_estimated_event[self.log_ids.end_time] - duration
        elif self.event_log_type == EventLogType.EVENT_LOG:
            # Store the durations of the estimated ones
            non_estimated_events = []
            activity_durations = {}
            for trace in self.event_log:
                for event in trace:
                    if event[self.log_ids.start_time] == self.config.non_estimated_time:
                        # Non-estimated, save event to estimate based on statistics
                        non_estimated_events += [event]
                    else:
                        # Estimated, store estimated time to calculate statistics
                        activity = event[self.log_ids.activity]
                        duration = event[self.log_ids.end_time] - event[self.log_ids.start_time]
                        if activity not in activity_durations:
                            activity_durations[activity] = [duration]
                        else:
                            activity_durations[activity] += [duration]
            # Set as start time the end time minus a statistic of the durations (mean/mode/median)
            for event in non_estimated_events:
                activity = event[self.log_ids.activity]
                # Re-estimate
                duration = self._get_activity_duration(activity_durations, activity)
                event[self.log_ids.start_time] = event[self.log_ids.end_time] - duration

    def _get_activity_duration(self, activity_durations, activity):
        if activity in activity_durations:
            # There have been measured other durations for the activity, take specified statistic
            if self.config.re_estimation_method == ReEstimationMethod.MODE:
                return mode(activity_durations[activity])
            elif self.config.re_estimation_method == ReEstimationMethod.MEDIAN:
                return np.median(activity_durations[activity])
            elif self.config.re_estimation_method == ReEstimationMethod.MEAN:
                return np.mean(activity_durations[activity])
            else:
                raise ValueError("Unselected re-estimation method for events with non-estimated start time!")
        else:
            # There are not other measures for the durations of the activity, set instant (duration = 0)
            return timedelta(0)

    def _apply_statistic(self, durations):
        if self.config.outlier_statistic == OutlierStatistic.MODE:
            return mode(durations)
        elif self.config.outlier_statistic == OutlierStatistic.MEDIAN:
            return np.median(durations)
        elif self.config.outlier_statistic == OutlierStatistic.MEAN:
            return np.mean(durations)
        else:
            raise ValueError("Unselected outlier statistic for events with estimated duration over the established!")
