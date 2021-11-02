from statistics import mode

from pm4py.objects.log.obj import EventLog

from config import ConcurrencyOracleType, ReEstimationMethod, ResourceAvailabilityType
from event_log.concurrency_oracle import NoConcurrencyOracle, AlphaConcurrencyOracle
from event_log.resource_availability import ResourceAvailability


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

    def estimate(self) -> EventLog:
        # Assign start timestamps
        for trace in self.event_log:
            for event in trace:
                enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                available_time = self.resource_availability.available_since(
                    event[self.config.log_ids.resource],
                    event[self.config.log_ids.end_timestamp]
                )
                event[self.config.log_ids.start_timestamp] = max(
                    enabled_time,
                    available_time
                )
        # Fix start times for those events being the first one of the trace and the resource (with non_estimated_time)
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            estimated_event_log = self._set_instant_non_estimated_start_times()
        elif self.config.re_estimation_method == ReEstimationMethod.MODE:
            estimated_event_log = self._re_estimate_non_estimated_start_times()
        else:
            print("Unselected fix method for events with no estimated start time! Setting them as instant by default.")
            estimated_event_log = self._set_instant_non_estimated_start_times()
        # Return modified event log
        return estimated_event_log

    def _set_instant_non_estimated_start_times(self) -> EventLog:
        # Identify events with non_estimated as start time
        # and set their processing time to instant
        for trace in self.event_log:
            for event in trace:
                if event[self.config.log_ids.start_timestamp] == self.config.non_estimated_time:
                    # Non-estimated, save event to estimate based on statistics
                    event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp]
        # Return modified event log
        return self.event_log

    def _re_estimate_non_estimated_start_times(self) -> EventLog:
        # Identify events with non_estimated as start time
        # and store the durations of the estimated ones
        non_estimated_events = []
        activity_times = {}
        for trace in self.event_log:
            for event in trace:
                if event[self.config.log_ids.start_timestamp] == self.config.non_estimated_time:
                    # Non-estimated, save event to estimate based on statistics
                    non_estimated_events += [event]
                else:
                    # Estimated, store estimated time to calculate statistics
                    activity = event[self.config.log_ids.activity]
                    processing_time = event[self.config.log_ids.end_timestamp] - event[self.config.log_ids.start_timestamp]
                    if activity not in activity_times:
                        activity_times[activity] = [processing_time]
                    else:
                        activity_times[activity] += [processing_time]
        # Set as start time the end time - the mode of the processing times (most frequent processing time)
        for event in non_estimated_events:
            activity = event[self.config.log_ids.activity]
            if activity in activity_times:
                event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp] - mode(activity_times[activity])
            else:
                # If this activity has no estimated times set as instant activity
                event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp]
        # Return modified event log
        return self.event_log
