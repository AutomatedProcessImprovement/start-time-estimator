from statistics import median, mean

import pandas as pd
import pm4py
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.objects.log.obj import EventLog
from pm4py.statistics.traces.generic.pandas import case_statistics


def analyze_stats(event_log):
    print("num_traces,num_events,num_variants,num_activities,min_trace_length,"
          "median_trace_length,avg_trace_length,max_trace_length,num_resources")
    if type(event_log) is pd.DataFrame:
        num_traces = len(event_log['case_id'].unique())
        trace_lengths = [len(trace) for (key, trace) in event_log.groupby(['case_id'])]
        num_events = len(event_log)
        num_variants = len(
            case_statistics.get_variants_df(
                event_log,
                parameters={case_statistics.Parameters.CASE_ID_KEY: "case_id",
                            case_statistics.Parameters.ACTIVITY_KEY: "Activity"}
            )['variant'].unique()
        )
        num_activities = len(event_log['Activity'].unique())
        min_trace_length = min(trace_lengths)
        median_trace_length = median(trace_lengths)
        avg_trace_length = mean(trace_lengths)
        max_trace_length = max(trace_lengths)
        num_resources = len(event_log['Resource'].unique())
    else:
        num_traces = len(event_log)
        trace_lengths = [len(trace) for trace in event_log]
        num_events = sum(trace_lengths)
        num_variants = len(variants_filter.get_variants(event_log))
        num_activities = len(attributes_filter.get_attribute_values(event_log, 'concept:name'))
        min_trace_length = min(trace_lengths)
        median_trace_length = median(trace_lengths)
        avg_trace_length = mean(trace_lengths)
        max_trace_length = max(trace_lengths)
        num_resources = len(attributes_filter.get_attribute_values(event_log, 'org:resource'))

    print("{},{},{},{},{},{},{},{},{}".format(
        num_traces,
        num_events,
        num_variants,
        num_activities,
        min_trace_length,
        median_trace_length,
        avg_trace_length,
        max_trace_length,
        num_resources
    ))


def event_log_stats():
    print("\nApplication_to_Approval_Government_Agency")
    event_log = pm4py.read_xes("../event_logs/Application_to_Approval_Government_Agency.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\nBPI_Challenge_2012_W_Two_TS")
    event_log = pm4py.read_xes("../event_logs/BPI_Challenge_2012_W_Two_TS.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\nBPI_Challenge_2017_W_Two_TS")
    event_log = pm4py.read_xes("../event_logs/BPI_Challenge_2017_W_Two_TS.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\ncallcentre")
    event_log = pm4py.read_xes("../event_logs/callcentre.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\nConfidential")
    event_log = pm4py.read_xes("../event_logs/confidential.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\nConsultaDataMining201618")
    event_log = pm4py.read_xes("../event_logs/ConsultaDataMining201618.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\ncvs_pharmacy")
    event_log = pm4py.read_xes("../event_logs/cvs_pharmacy.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\ninsurance")
    event_log = pm4py.read_xes("../event_logs/insurance.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\nLoan_Application")
    event_log = pm4py.read_xes("../event_logs/Loan_Application.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\npoc_processmining")
    event_log = pm4py.read_xes("../event_logs/poc_processmining.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\nProcure_to_Pay")
    event_log = pm4py.read_xes("../event_logs/Procure_to_Pay.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)
    print("\nProduction")
    event_log = pm4py.read_xes("../event_logs/Production.xes.gz")
    check_event_lifecycles(event_log)
    analyze_stats(event_log)


def check_event_lifecycles(event_log: EventLog):
    diff_lifecycle_flag = False
    for trace in event_log:
        starts = []
        for event in trace:
            if event['lifecycle:transition'] == 'start':
                # if event['concept:name'] in starts:
                #    print("'start' of an activity ('{}') before the completion of its previous activity instance in trace '{}'.".format(
                #        event['concept:name'],
                #        trace.attributes['concept:name']
                #    ))
                starts += [event['concept:name']]
            elif event['lifecycle:transition'] == 'complete':
                if event['concept:name'] not in starts:
                    print("'complete' event of '{}' not preceded by 'start' event in trace '{}'.".format(
                        event['concept:name'],
                        trace.attributes['concept:name']
                    ))
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
