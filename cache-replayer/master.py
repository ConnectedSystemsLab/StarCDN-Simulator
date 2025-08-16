import subprocess
import sys
import json
from datetime import datetime
import socket
import time
import os
import ast
from utils import *
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sat_conf = {} # id to config
topology = {} # id to (host, port)
client_conf = {}
conf_path = sys.argv[1]
fov_path = sys.argv[2]
log_dir = sys.argv[3]
cache_size = int(sys.argv[4])
os.makedirs(log_dir, exist_ok=True)
# Setup master socket
master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
master_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
master_sock.bind(('0.0.0.0', 0))
master_port = master_sock.getsockname()[1]
master_sock.listen()

# Read configuration file
with open(conf_path, 'r') as f:
    emulation_conf = json.load(f)
with open(sys.argv[5]) as f:
    logical_neighbor = json.load(f)
starttime = datetime.strptime(emulation_conf['simtime']['starttime'], "%Y-%m-%d %H:%M:%S").timestamp()

for d in emulation_conf['topologies'][0]['nodes']:
    node_id = d['nodeid']
    
    if d['type'] == 'SAT':
        #save neighbors ids as well
        neighbors = []
        for model in d['models']:
            if model['iname'] == 'ModelCDNProvider':
                neighbors = model.get('neighbors', [])
                break
            
        sat_conf[node_id] = {
            "log_dir": f"{log_dir}/{d['type']}_{node_id}",
            "cache_size": cache_size,
            "id": node_id,
            "neighbors": logical_neighbor[str(node_id)],
            "starttime": starttime,
            "trace": os.path.join(fov_path, f"Log_Constln1_0_SAT_{node_id}.log")
        }

processes = []
threshold = 2000 
cont = 0
# Start satellite server
def start_server(processes, topology, master_port, file_name, server_id):
    process = subprocess.Popen(['python', file_name, str(master_port), str(server_id)]) 
    processes.append(process)
    conn, addr = master_sock.accept()
    verb, data = read_from_socket(conn)
    data = json.loads(data.decode())
    topology[int(data['server_id'])] = ('0.0.0.0', int(data['port']))
    conn.shutdown(socket.SHUT_RDWR)
starting_threads = []
for sat_id, conf in sat_conf.items():
    server_thread = threading.Thread(target=start_server, args=[processes, topology, master_port, 'sat.py', sat_id])
    server_thread.start()
    starting_threads.append(server_thread)
    if cont > threshold:
        break
    cont += 1
for thread in starting_threads:
    thread.join()
time.sleep(1)
cont = 0
# Configuring servers
for sat_id, conf in sat_conf.items():
    host, port = topology[sat_id] 
    conf["topology"] = topology
    verb, ret = send_request_wait_response(host, int(port), "CONF", json.dumps(conf))
    assert verb == "ACK "
    if cont > threshold:
        break
    cont += 1

print("Finish configuring servers")

# Main emulation
cont = 0 # index for FoV
sat_links = {}
for sat_id, conf in sat_conf.items():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    host, port = topology[sat_id] 
    s.connect((host, port)) 
    write_to_socket(s, "REQS", "")
    read_from_socket(s)
    sat_links[sat_id] = s 
for cur_time in range(int(starttime), int(starttime) + 15 * 4 * 60 * 24 * 5, 15):
    
    with ThreadPoolExecutor(30) as executor:
        def orchestraClient(host, port, client_id, verb, message):
            write_to_socket(sat_links[client_id], verb, message) 
            read_from_socket(sat_links[client_id])
            # assert verb == "ACK "
        future_list = []
        for client_id, conf in sat_conf.items():
            host, port = topology[client_id] 
            future_list.append(executor.submit(orchestraClient, host, port, client_id, "REQ ", json.dumps({"time": cur_time})))
        for future in future_list:
            future.result()
    if cont % 2000 == 0:
        print(f"Emulation {cont}")
    cont += 1

for process in processes:
    process.terminate()