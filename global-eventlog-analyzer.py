#!/usr/bin/env python3

from sys import argv
from htcondor2 import JobEventLog
from os import uname

only_event_codes = [
    12, # Hold Event
    #5, # Termination Event
]

including_filters = {
#    'HoldReason': [['Aborted due to lack of progress']],
    'HoldReason': [['osdf'],['Pelican']],
#    'HoldReason': [['server returned 404 Not Found','osdf']],  # AND
#    'EventTime': [['2024-08-21'], ['2024-08-22']],  # OR
#    'EventTime': [['2024-08-22']],
}

excluding_filters = {
#   'HoldReason': [['stash']]
}

if len(argv) == 1:
    hostname = uname()[1]
    if hostname == 'ospool-ap2140':
        eventlogpath = '/var/log/condor/GlobalEventLog'
    elif hostname in ['ap2001', 'ap2002']:
        eventlogpath = '/var/log/condor/EventLog'
    globaleventlog = True
elif len(argv) == 2:
    eventlogpath = str(argv[1])
    globaleventlog = False
else:
    raise ValueError('Error: Too many arguments. Either no arguments (defaults to global event log) or'
                     '1 argument that is the name of the log file to analyze.')

eventlog = JobEventLog(eventlogpath)

def event_filters_check(event, filters_dict, require_all_match=False):
    if len(filters_dict) == 0:
        raise ValueError('filters_dict must not be empty')

    event_matches = []
    for key, value in filters_dict.items():
        or_matches = []
        for or_item in value:
            and_matches = []
            for and_item in or_item:
                try:
                    if and_item in event[key]:
                        and_matches.append(True)
                    else:
                        and_matches.append(False)
                        break
                except KeyError:
                    and_matches.append(False)
                    break
            if all(and_matches):
                or_matches.append(True)
            else:
                or_matches.append(False)
        if any(or_matches):
            event_matches.append(True)
        else:
            event_matches.append(False)
    if require_all_match is True:
        return all(event_matches)
    else:
        return any(event_matches)

#hold_events = [event for event in eventlog.events(stop_after=0) if event['EventTypeNumber'] == 12]

filtered_events = []

if excluding_filters and including_filters:
    for event in eventlog.events(stop_after=0):
        if event['EventTypeNumber'] not in only_event_codes:
            continue
        if event_filters_check(event, excluding_filters):
            continue
        elif event_filters_check(event, including_filters, require_all_match=True):
            filtered_events.append(event)
        else:
            continue
elif excluding_filters and not including_filters:
    for event in eventlog.events(stop_after=0):
        if event['EventTypeNumber'] not in only_event_codes:
            continue
        if event_filters_check(event, excluding_filters):
            continue
        else:
            filtered_events.append(event)
elif not excluding_filters and including_filters:
    for event in eventlog.events(stop_after=0):
        if event['EventTypeNumber'] not in only_event_codes:
            continue
        if event_filters_check(event, including_filters, require_all_match=True):
            filtered_events.append(event)
        else:
            continue
else:
    raise ValueError('No filters were provided - aborting analysis.')


#for event in filtered_events:
#    print(event)

len_filtered_events = len(filtered_events)
print('\n\tFound {} matching events.'.format(len_filtered_events))

if globaleventlog:
    print('Writing to "filtered-events.log".\n')
    with open('filtered-events.log','w') as myout:
        myout.write('{}...\n'.format('...\n'.join([str(i) for i in filtered_events])))
else:
    print('{}...\n'.format('...\n'.join([str(i) for i in filtered_events])))