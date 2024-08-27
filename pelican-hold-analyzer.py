#!/usr/bin/env python3

from htcondor2 import JobEventLog
from sys import argv

if len(argv) != 2:
    raise ValueError('Error: Requires exactly 1 argument - the name of the log file with Pelican hold messages.')
else:
    eventlogpath = str(argv[1])

# Assuming that log file only contains events that are hold messages involving the Pelican client.
# Can create by using ./global-eventlog-analyzer.py with including filter "'HoldReason': [['Pelican']]".

eventlog = JobEventLog(eventlogpath)

hold_dicts = []
for event in eventlog.events(stop_after=0):
    hold_entry = {'FullEvent': event}
    try:
        header, details, empty = event['HoldReason'].split('|')
        hold_entry['Header'] = header
        hold_entry['Details'] = details
        try:
            hold_type = header.split('.')[0]
            hold_entry['HoldType'] = hold_type
        except:
            pass

        try:
            hold_entry['Version'] = details.split('Version: ')[1].split(';')[0]
            hold_entry['Site'] = details.split('Site: ')[1].split(')')[0]
            hold_entry['File'] = details.split('URL file = ')[1].split(' )')[0]
        except:
            pass

        try:
            hold_entry['Attempt #3'] = details.split('Attempt #3: ')[1].split(';')[0]
            hold_entry['Attempt #2'] = details.split('Attempt #2: ')[1].split(';')[0]
            hold_entry['Attempt #1'] = details.split('Attempt #1: ')[1].split(' (Version:')[0]
        except:
            pass

    except:
        pass
    hold_dicts.append(hold_entry)

attempts_dicts = []
for hold_dict in hold_dicts:
    has_attempts = False
    if 'Attempt #3' in hold_dict.keys():
        try:
            attempt_entry = {**hold_dict}
            attempt_entry['Cache'] = hold_dict['Attempt #3'].split('from ')[1].split(':')[0]
            attempt_entry['Cache Fail Reason'] = hold_dict['Attempt #3'].split(': ')[1].split(' (')[0]
            attempt_entry['Cache Fail Details'] = hold_dict['Attempt #3'].split(' (')[1].split(')')[0]
            attempts_dicts.append(attempt_entry)
            has_attempts = True
        except:
            pass
    if 'Attempt #2' in hold_dict.keys():
        try:
            attempt_entry = {**hold_dict}
            attempt_entry['Cache'] = hold_dict['Attempt #2'].split('from ')[1].split(':')[0]
            attempt_entry['Cache Fail Reason'] = hold_dict['Attempt #2'].split(': ')[1].split(' (')[0]
            attempt_entry['Cache Fail Details'] = hold_dict['Attempt #2'].split(' (')[1].split(')')[0]
            attempts_dicts.append(attempt_entry)
            has_attempts = True
        except:
            pass
    if 'Attempt #1' in hold_dict.keys():
        try:
            attempt_entry = {**hold_dict}
            attempt_entry['Cache'] = hold_dict['Attempt #1'].split('from ')[1].split(':')[0]
            attempt_entry['Cache Fail Reason'] = hold_dict['Attempt #1'].split(': ')[1].split(' (')[0]
            attempt_entry['Cache Fail Details'] = hold_dict['Attempt #1'].split(' (')[1].split(')')[0]
            attempts_dicts.append(attempt_entry)
            has_attempts = True
        except:
            pass
    if has_attempts is False:
        attempts_dicts.append(hold_dict)

failed_caches = []
failed_reasons = []
for attempt in attempts_dicts:
    if 'Cache' in attempt.keys():
        failed_caches.append(attempt['Cache'])
    if 'Cache Fail Reason' in attempt.keys():
        failed_reasons.append(attempt['Cache Fail Reason'])

n_failed_caches = len(failed_caches)
padding_cache = len(str(n_failed_caches))
set_failed_caches = set(failed_caches)
results_caches = []
for cache in set_failed_caches:
    results_caches.append([cache, failed_caches.count(cache)])


n_failed_reasons = len(failed_reasons)
padding_reasons = len(str(n_failed_reasons))
set_failed_reasons = set(failed_reasons)
results_failed_reasons = []
for failed_reason in set_failed_reasons:
    results_failed_reasons.append([failed_reason, failed_reasons.count(failed_reason)])

sites = []
for hold in hold_dicts:
    if 'Site' in hold.keys():
        sites.append(hold['Site'])


n_sites = len(sites)
padding_sites = len(str(n_sites))
set_sites = set(sites)
results_sites = []
for site in set_sites:
    results_sites.append([site, sites.count(site)])

sorted_results_caches = sorted(results_caches, key=lambda x: x[1], reverse=True)
formatted_caches = ['{count:{padding}d}: {value}'.format(value=cache, count=cache_count, padding=padding_cache) for cache, cache_count in sorted_results_caches]
string_caches = '\n'.join(formatted_caches)

sorted_results_reasons = sorted(results_failed_reasons, key=lambda x: x[1], reverse=True)
formatted_reasons = ['{count:{padding}d}: {value}'.format(value=reason, count=reason_count, padding=padding_reasons) for reason, reason_count in sorted_results_reasons]
string_reasons = '\n'.join(formatted_reasons)

sorted_results_sites = sorted(results_sites, key=lambda x: x[1], reverse=True)
formatted_sites = ['{count:{padding}d}: {value}'.format(value=site, count=site_count, padding=padding_sites) for site, site_count in sorted_results_sites]
string_sites = '\n'.join(formatted_sites)

print("\n---\nCache analysis\n")
print("{total:{padding}d}: Total\n".format(total=n_failed_caches, padding=padding_cache))
print(string_caches)

print("\n---\nReason analysis\n")
print("{total:{padding}d}: Total\n".format(total=n_failed_reasons, padding=padding_reasons))
print(string_reasons)

print("\n---\nSite analysis\n")
print("{total:{padding}d}: Total\n".format(total=n_sites, padding=padding_sites))
print(string_sites)

cache_reason_correlation = {cache: {reason: 0 for reason in set_failed_reasons} for cache in set_failed_caches}
for attempt in attempts_dicts:
    if 'Cache' in attempt.keys() and 'Cache Fail Reason' in attempt.keys():
        cache_reason_correlation[attempt['Cache']][attempt['Cache Fail Reason']] += 1

cache_reason_counts = []
for cache in set_failed_caches:
    for reason in set_failed_reasons:
        count = cache_reason_correlation[cache][reason]
        cache_reason_counts.append([count, cache, reason])

sorted_cache_reason_counts = sorted(cache_reason_counts, key=lambda x: x[0], reverse=True)
n_cache_reason_counts = sum([i[0] for i in sorted_cache_reason_counts])
padding_cache_reason_counts = len(str(n_cache_reason_counts))
formatted_cache_reason_counts = ['{count:{padding}d}: {value}'.format(value='/'.join([cache, reason]), count=count, padding=padding_cache_reason_counts) 
                                 for count, cache, reason in sorted_cache_reason_counts if count != 0]
string_cache_reason_counts = '\n'.join(formatted_cache_reason_counts)
print("\n---\nCache/Reason Correlation")
print("{total:{padding}d}: Total\n".format(total=n_cache_reason_counts, padding=padding_cache_reason_counts))
print(string_cache_reason_counts)
