#
# Author: David Chapela de la Campa
#

import pytz
from datetime import datetime
from event_log_readers import read_xes_log
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

from event_log.concurrency_oracle import AlphaConcurrencyOracle
from event_log.estimate_start_times import estimate_start_timestamps
from event_log.resource_availability import ResourceAvailability

missing_resource = "missing_resource"
initial_time = datetime.min.replace(tzinfo=pytz.UTC)


def main(event_log_path) -> None:
    # Read event log
    event_log = read_xes_log(event_log_path, missing_resource)
    # Build concurrency oracle
    concurrency_oracle = AlphaConcurrencyOracle(event_log, initial_time)
    # Build resource schedule
    resource_availability = ResourceAvailability(event_log, initial_time, missing_resource)
    # Infer start timestamps
    extended_event_log = estimate_start_timestamps(event_log, concurrency_oracle, resource_availability)
    # Export event log
    xes_exporter.apply(extended_event_log, './extended_event_log.xes')


if __name__ == '__main__':
    main('../event_logs/BPI Challenge 2017.xes.gz')
