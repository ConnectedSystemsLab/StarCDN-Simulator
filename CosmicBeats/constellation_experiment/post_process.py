import os, ast, re, sys
import numpy as np
LOG_DIR = sys.argv[1]
sat_files = []
user_files = []
gs_files = []
for dirpath, dirnames, filenames in os.walk(LOG_DIR):
    for file in filenames:
        if 'User' in file:
            user_files.append(os.path.join(dirpath, file))
        elif 'SAT' in file:
            sat_files.append(os.path.join(dirpath, file))
        elif 'GS' in file:
            gs_files.append(os.path.join(dirpath, file))
bandwith = []
global_hit = 0
global_req_byte = 0
for file in sat_files:
    pattern = r"uplink:([+-]?\d*\.\d+|\d+),\s*downlink:([+-]?\d*\.\d+|\d+),\s*byte_hit:([+-]?\d*\.\d+|\d+)"
    bandwith.append([])
    with open(file, 'r') as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                uplink = (float(match.group(1)))
                downlink = (float(match.group(2)))
                byte_hit = (float(match.group(3)))
                global_req_byte += downlink
                if uplink != 0 or downlink != 0:
                    bandwith[-1].append([uplink, downlink])
                global_hit += byte_hit 
bandwith = [i for i in bandwith if len(i) != 0]
overall_bandwith = []
for item in bandwith:
    overall_bandwith.append(np.sum(item, axis=0))
print(f"[Uplink, Downlink]: {np.sum(overall_bandwith, axis=0)}")
print(f'byte hit rate: {global_hit / global_req_byte}')

isl_usage = np.array([0.0, 0.0, 0.0, 0.0])
isl_avg_usage = []
already_in_cache = 0
for file in gs_files:
    with open(file, 'r') as f:
        for line in f:
            if '[Prefetch stat]' in line:
                stat = ast.literal_eval(line[line.rfind(':') + 1:line.rfind(']') + 1])
                if stat[-1] != [0, 0, 0, 0]:
                    isl_usage += np.array(stat[-1])
                    isl_avg_usage.append(stat[-1])
                already_in_cache += stat[1]
print(f'total isl traffic: {isl_usage}')
print(f"avg isl: {np.average(isl_avg_usage, axis=0)}\nstd isl: {np.std(isl_avg_usage, axis=0)}\nmax isl: {np.max(isl_avg_usage, axis=0)}\nmedian isl: {np.median(isl_avg_usage, axis=0)}")
print(f"25 percentile: {np.percentile(isl_avg_usage, 25, axis=0)}, 90 percentile: {np.percentile(isl_avg_usage, 90, axis=0)}")
print(f'byte already in cache {already_in_cache}')

