from statistics import median, mean

import pm4py
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.objects.log.obj import EventLog


def analyze_stats(event_log):
    print("num_traces,num_events,num_variants,num_activities,min_trace_length,median_trace_length,avg_trace_length,max_trace_length")
    num_traces = len(event_log)
    trace_lengths = [len(trace) for trace in event_log]
    num_events = sum(trace_lengths)
    num_variants = len(variants_filter.get_variants(event_log))
    num_activities = len(attributes_filter.get_attribute_values(event_log, 'concept:name'))
    min_trace_length = min(trace_lengths)
    median_trace_length = median(trace_lengths)
    avg_trace_length = mean(trace_lengths)
    max_trace_length = max(trace_lengths)

    print("{},{},{},{},{},{},{},{}".format(
        num_traces,
        num_events,
        num_variants,
        num_activities,
        min_trace_length,
        median_trace_length,
        avg_trace_length,
        max_trace_length
    ))


def event_log_stats():
    print("BPI_Challenge_2012_W_Two_TS")
    event_log = pm4py.read_xes("../event_logs/BPI_Challenge_2012_W_Two_TS.xes.gz")
    analyze_stats(event_log)
    print("BPI_Challenge_2017_W_Two_TS")
    event_log = pm4py.read_xes("../event_logs/BPI_Challenge_2017_W_Two_TS.xes.gz")
    analyze_stats(event_log)
    print("callcentre")
    event_log = pm4py.read_xes("../event_logs/callcentre.xes.gz")
    analyze_stats(event_log)
    print("ConsultaDataMining201618")
    event_log = pm4py.read_xes("../event_logs/ConsultaDataMining201618.xes.gz")
    analyze_stats(event_log)
    print("cvs_pharmacy")
    event_log = pm4py.read_xes("../event_logs/cvs_pharmacy.xes.gz")
    analyze_stats(event_log)
    print("insurance")
    event_log = pm4py.read_xes("../event_logs/insurance.xes.gz")
    analyze_stats(event_log)
    print("poc_processmining")
    event_log = pm4py.read_xes("../event_logs/poc_processmining.xes.gz")
    analyze_stats(event_log)
    print("Production")
    event_log = pm4py.read_xes("../event_logs/Production.xes.gz")
    analyze_stats(event_log)
    print("PurchasingExample")
    event_log = pm4py.read_xes("../event_logs/PurchasingExample.xes.gz")
    analyze_stats(event_log)


def check_event_lifecycles(event_log: EventLog):
    diff_lifecycle_flag = False
    for trace in event_log:
        starts = []
        for event in trace:
            if event['lifecycle:transition'] == 'start':
                starts += [event['concept:name']]
            elif event['lifecycle:transition'] == 'complete':
                if event['concept:name'] not in starts:
                    print("'complete' event of '{}' not preceded by 'start' event in trace '{}'.".format(event['concept:name'],
                                                                                                         trace.attributes['concept:name']))
                else:
                    starts.remove(event['concept:name'])
            elif not diff_lifecycle_flag:
                print("Different lifecycle ({}) in trace '{}'. Turning down 'different lifecycle' warnings".format(
                    event['lifecycle:transition'],
                    trace.attributes['concept:name']
                ))
                diff_lifecycle_flag = True
        if len(starts) > 0:
            print("Missing 'complete' events ({}) in trace '{}'".format(", ".join(starts), trace.attributes['concept:name']))


if __name__ == '__main__':
    event_log_stats()