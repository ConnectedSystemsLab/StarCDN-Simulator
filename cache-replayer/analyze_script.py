import multiprocessing
import os
import ast
import numpy as np
import sys
from collections import defaultdict
import json

def process_file(file_name):
    """
    Custom function to process a file.
    Modify this function based on your specific processing requirements.
    """
    # Example: Read file and return its size in bytes
    hrc_ret = np.zeros((12,))
    hrc_time_based = {} 
    latency_ret = defaultdict(int)
    with open(file_name, "r") as f:
        line = f.readline()
        d = ast.literal_eval(line)
        for line in f:
            if line[:6] == "[Data]":

                line = line[len("[Data]: "):]
                hrc_temp = np.array(ast.literal_eval(line[line.find('['):line.rfind(']') + 1]))
                hrc_ret += hrc_temp 
                # hrc_time_based.setdefault(timestamp, np.zeros((12, )))
                # hrc_time_based[timestamp] += hrc_temp
            elif line[:6] == "[Laten":
                line = ast.literal_eval(line[line.rfind('{') : line.rfind('}') + 1])
                for k, v in line.items() :
                    latency_ret[k] += v

    return hrc_ret, latency_ret, hrc_time_based

def worker(file_name, output_dict):
    """
    Worker function for multiprocessing.
    """
    result = process_file(file_name)
    output_dict[file_name] = result
    print(f"Finish {file_name}")

def process_files_in_parallel(file_list):
    """
    Process a list of files in parallel and return a dictionary mapping
    file names to their results.
    """
    manager = multiprocessing.Manager()
    output_dict = manager.dict()  # Shared dictionary among processes

    # Create a process for each file
    processes = []
    for file_name in file_list:
        p = multiprocessing.Process(target=worker, args=(file_name, output_dict))
        processes.append(p)
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Convert the shared dictionary to a standard dictionary
    return dict(output_dict)

if __name__ == "__main__":
    # Example file list (replace with your actual files)
    file_list = []
    for root, dirs, files in os.walk(sys.argv[1]):
        cnt = 0
        for file in files:
            if "SAT" in file:
                file_list.append(os.path.join(root, file))

    # Run the processing function in parallel
    result = process_files_in_parallel(file_list)

    # Print the results
    print("Results:")
    agg_hrc_res = np.zeros((12,))
    latency_res = defaultdict(int)
    time_aggr_hrc_res = {}
    json_compatible_res = {}
    for file_name, output in result.items():
        json_compatible_res[file_name] = list(output[0])
        agg_hrc_res += np.array(output[0])
        for k, v in output[1].items():
            latency_res[k] += v
        # for k, v in output[2].items():
        #     time_aggr_hrc_res.setdefault(k, np.zeros((12,)))
        #     time_aggr_hrc_res[k] += v
    with open("temp.json", 'w') as f:
        json.dump(json_compatible_res, f)

    print(agg_hrc_res)
    print(agg_hrc_res[2] / agg_hrc_res[0], agg_hrc_res[3] / agg_hrc_res[1])
    print(dict(latency_res))
    print(sorted(time_aggr_hrc_res))
