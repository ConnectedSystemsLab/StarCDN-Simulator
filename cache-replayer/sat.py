import sys
import socket
import json
import threading
import os
import traceback
from utils import *
from lru import LRU_Cache, LRU_Freq_Cache
from datetime import datetime
import ast
from collections import defaultdict

class Satellite():
    """
    This satellite instance directly read satellite's log file for requests
    """

    def save_logs(self):
        if self.__log_data:
            file_exists = os.path.isfile("satellite_requests.csv")
            with threading.Lock():  # Ensure thread safety
                with open("satellite_requests.csv", 'a') as log_file:
                    # Write the header only if the file is new
                    if not file_exists:
                        log_file.write("satellite_id,request_id,hit_or_miss,source,neighbor_id\n")
                    for log_entry in self.__log_data:
                        log_file.write(f"{log_entry['satellite_id']},{log_entry['request_id']},{log_entry['hit_or_miss']},{log_entry['source']},{log_entry['neighbor_id']}\n")
            # print(f"Satellite {self.__sat_id} logs saved.")

    def __handle_config(self, conn, data: str):
        data: dict = json.loads(data)
        self.__log_handler = open(data['log_dir'], 'w')
        self.__log_data = []  # List to store request logs
        self.__cache = LRU_Cache(int(data['cache_size']))
        self.__sat_id = data['id']
        # Store neighbors
        self.__neighbors = data.get('neighbors', [])  
        self.__trace = open(data['trace'], 'rb')
        self.__trace.readline()
        time_trace_start = int(datetime.strptime(self.__trace.readline().decode().split(',')[1][1:], "%Y-%m-%d %H:%M:%S").timestamp())
        emulation_start_time = data['starttime']
        self.__trace_emulation_time_diff = emulation_start_time - time_trace_start
        self.__cur_time = emulation_start_time
        self.__log_handler.write(f'{data}\n')
        self.__topology = data['topology']
        self.__isl = []
        for neigh in self.__neighbors:
            if int(neigh) == -1:
                self.__isl.append(None)
            else:
                host, port = self.__topology[str(neigh)]
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
                s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                s.connect((host, port)) 
                write_to_socket(s, "ISL ", "")
                read_from_socket(s)
                self.__isl.append(s)

        write_to_socket(conn, "ACK ", "")
        


    
    def __handle_req(self, conn, data: str):
        # print(f"Request result received by {self.__sat_id}")
        write_to_socket(conn, "ACK ", "")
        while True:
            verb, data = read_from_socket(conn)
            data = json.loads(data)
            emulation_time = data['time']
            cur_time = emulation_time 
            self.__cur_time = emulation_time
            total_obj, total_byte, hit_obj, hit_byte = 0, 0, 0, 0
            hit_obj_by_neigh, hit_byte_by_neigh = 0, 0
            hit_obj_by_pref, hit_byte_by_pref = 0, 0
            latency_array = [0, 0, 0, 0]
            latency_dict = defaultdict(int) 
            has_traffic = False
            while True:
                line = self.__trace.readline() 
                if line is None or len(line) == 0: 
                    # Already end of file
                    break
                line = line.decode()
                time_now = datetime.strptime(line.split(',')[1][1:], "%Y-%m-%d %H:%M:%S").timestamp()
                # print(time_now, cur_time)

                if int(time_now) <= cur_time:
                    idx = line.find("[Requests Records]")
                    if idx != -1:
                        has_traffic = True
                        line = line[idx + len("[Requests Records]: "):].strip()
                        # Read user id (location)
                        user_id = int(line[:line.find(",")])
                        if user_id not in self.__location_last_serve or self.__location_last_serve[user_id] - self.__cur_time >= 1800:
                            # Clear the LFU for stale
                            self.__location_lfu[user_id] = LRU_Freq_Cache(100000)
                            self.__log_handler.write(f"[DEBUG]: clear {user_id} cache\n")
                        self.__location_last_serve[user_id] = self.__cur_time
                        # Read latency
                        latency = ast.literal_eval(line[line.find('[') : line.find(']') + 1])
                        line = line[line.find(']') + 1 : ]

                        line_data = ast.literal_eval(line[line.find('['):-1])
                        for req_id, req_size in line_data:
                            remote_hit = False
                            total_obj += 1
                            total_byte += req_size
                            req_hit = req_id in self.__cache 
                            latency_array[0] += latency[0]
                            latency_array[1] += latency[1]
                            found_in_neighbor = False

                            if req_hit:
                                hit_byte += req_size
                                hit_obj += 1
                            else:
                                for neighbor_idx, neighbor_id in enumerate(self.__neighbors):
                                    if int(neighbor_idx) < 2:
                                        continue
                                    if int(neighbor_id) != -1 and self.__query_neighbor_by_idx(neighbor_idx, req_id):
                                        found_in_neighbor = True
                                        which_neighbor = neighbor_id
                                        latency_array[2 + (neighbor_idx // 2)] += 3
                                        latency_array[(neighbor_idx // 2)] += 3
                                        hit_obj += 1
                                        hit_byte += req_size
                                        hit_obj_by_neigh += 1
                                        hit_byte_by_neigh += req_size
                                        break
                            
                            if req_hit:
                                latency_dict[(latency[0] * 2, latency[1] * 2, 2)] += 1
                            elif found_in_neighbor:
                                latency_dict[(latency[0] * 2, latency[1] * 2 + 6, 2)] += 1
                            else:
                                latency_dict[(latency[0] * 2, latency[1] * 2 + 6, 4)] += 1



                            self.__cache.admit(req_id, req_size, 0)
                            

                else:
                    # Rewind if time is not up there yet
                    self.__trace.seek(-len(line), os.SEEK_CUR)
                    # Suggest some id to prefetch
                    break
            self.__log_handler.write(f"[Data]: {data['time']}, {[total_obj, total_byte, hit_obj, hit_byte, hit_obj_by_neigh, hit_byte_by_neigh] + latency_array + [hit_obj_by_pref, hit_byte_by_pref]}\n")
            self.__log_handler.write(f"[Latency]: {str(dict(latency_dict))}\n")
            self.__log_handler.flush()
            write_to_socket(conn, "ACK ", f"{[total_obj, total_byte, hit_obj, hit_byte, hit_obj_by_neigh, hit_byte_by_neigh]}") 

    def __query_neighbor_by_idx(self, neighbor_idx, object_id):
        write_to_socket(self.__isl[neighbor_idx], "CHK ", str(object_id))
        verb, data = read_from_socket(self.__isl[neighbor_idx])
        if verb == "ACK " and data.decode() == "FOUND":
            # print("***Found in neighbour:",neighbor_idx,"********")
            return True  # Object found in neighbor's cache
        return False

    def __query_neighbor(self, neighbor_id, object_id):
        try:
            # Get the neighbor's host and port from the topology
            host, port = self.__topology[str(neighbor_id)]
            # host = '127.0.0.1'

            # Establish a connection to the neighbor
            # print(f"Query {neighbor_id} for {object_id}")
            verb, data = send_request_wait_response(host,port,"CHK ",str(object_id))
            if verb == "ACK " and data.decode() == "FOUND":
                return True  # Object found in neighbor's cache
        except Exception as e:
            traceback.print_exc()
	    #print(f"Failed to query neighbor {neighbor_id}: {e}")

        return False  # Object not found or error occurred

    
    def __handle_get(self, conn, data: str):
        if data == 'cache_key':
            write_to_socket(conn, "ACK ", json.dumps(list(self.__cache.cache_keys))) 
        if data == 'cache_capacity':
            write_to_socket(conn, "ACK ", str(self.__cache.capacity))
        if data == 'cache_size':
            write_to_socket(conn, "ACK ", str(self.__cache.size))
    
    def __handle_isl(self, conn):
        write_to_socket(conn, "ACK ", "")
        while True:
            verb, data = read_from_socket(conn)
            if verb is None:
                break
            # assert verb == "CHK ", verb
            if verb == "CHK ": # Logic for collaboration check
                if data.decode() in self.__cache:
                    write_to_socket(conn, "ACK ", "FOUND")
                else:
                    write_to_socket(conn, "ACK ", "NOT_FOUND")
            elif verb == "PREF": # Logic for prefetch
                data = json.loads(data.decode()) 
                user_id = data['user']
                if user_id not in self.__prefetch_map_last_update or self.__cur_time - self.__prefetch_map_last_update[user_id] >= 30 * 60:
                    self.__log_handler.write(f"[DEBUG]: clear {user_id} map\n")
                    self.__prefetch_map[user_id] = []
                self.__prefetch_map_last_update[user_id] = self.__cur_time
                accepted_cnt = 0
                for req_id, req_size, req_freq in data['data']:
                    if req_id not in self.__cache:
                        self.__cache.admit(req_id, req_size, 0)

                        if user_id not in self.__location_last_serve or self.__location_last_serve[user_id] - self.__cur_time >= 30 * 60:
                            # Clear the LFU for stale
                            self.__location_lfu[user_id] = LRU_Freq_Cache(100000)
                            self.__log_handler.write(f"[DEBUG]: clear {user_id} cache\n")
                        self.__location_lfu[user_id].admit(req_id, 0)
                        self.__location_lfu[user_id].set_freq(req_id, int(req_freq * 0.5) + 1) 

                        self.__location_last_serve[user_id] = self.__cur_time
                        self.__prefetch_map.setdefault(user_id, [])
                        self.__prefetch_map[user_id].append(req_id)
                        accepted_cnt += 1
                self.__log_handler.write(f"[DEBUG]: Accept {accepted_cnt}/{len(data['data'])}\n")
                write_to_socket(conn, "ACK ", "")

    def __handle_client(self, conn, addr):
        with conn:
            verb, data = read_from_socket(conn) 
            try:
                if verb == 'CONF':
                    self.__handle_config(conn, data.decode())
                if verb == "REQS":
                    self.__handle_req(conn, data.decode())
                if verb == "GET ":
                    self.__handle_get(conn, data.decode())
                if verb == "KILL":
                    print("Kill received")
                    self.exit()
                if verb == "SAVE":
                    self.save_logs()  # Directly save logs
                    write_to_socket(conn, "ACK ", "Logs saved.")
                
                if verb == "ISL ":
                    self.__handle_isl(conn)

                #new flag for checking neighboring sats request for object
                if verb == "CHK ":
                    # Check if the requested object is in the local cache
                    if data.decode() in self.__cache:
                        write_to_socket(conn, "ACK ", "FOUND")
                    else:
                        write_to_socket(conn, "ACK ", "NOT_FOUND")
            except Exception as e:
                print(f'Exception {repr(e)}') 
                print(traceback.format_exc())
            finally:
                conn.shutdown(socket.SHUT_RDWR)
                self.__threads_conn.remove(conn)
    
    def run(self):
        # print(f'Server started at {self.__port}, waiting for connection...')
        # Register presence to master
        send_request('0.0.0.0', self.__master_port, 'REGR', json.dumps({"port": self.__port, "server_id": self.__server_id}))
        try:
            while not self.__end:
                conn, addr = self.__socket.accept()
                if self.__end:
                    break
                self.__threads_conn.append(conn)
                client_thread = threading.Thread(target=self.__handle_client, args=(conn, addr))
                client_thread.start()
        except KeyboardInterrupt:
            self.__socket.shutdown(socket.SHUT_RDWR) 
            for conn in self.__threads_conn:
                conn.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            print(f"Socket error: {e}")


    def __init__(self, master_port, server_id):
        host = '0.0.0.0'
        self.__threads_conn = []
        self.__master_port = master_port
        self.__server_id = server_id
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__socket.bind((host, 0))
        self.__port = self.__socket.getsockname()[1]
        self.__socket.listen()
        self.__end = False
        
        # Utility and metrics for prefetch
        self.__prefetch_map = {} # Map trace id to list of objects being prefetched
        self.__prefetch_map_last_update = {} # Last update time of prefetch
        self.__location_last_serve = {} # Last serving time of a location
        self.__location_lfu = {} # Map location to their lfu



    def exit(self):
        self.__end = True
        # self.save_logs()  # Save logs before shutting down
        self.__socket.shutdown(socket.SHUT_RDWR) 
        for conn in self.__threads_conn:
            conn.shutdown(socket.SHUT_RDWR) 
    @property
    def threads_conn(self,):
        return self.__threads_conn
    
    @property
    def neighbors(self):
        return self.__neighbors



if __name__ == '__main__':
    port = int(sys.argv[1])
    server_id = int(sys.argv[2]) 
    try:
        s = Satellite(port, server_id) 
        s.run()
    except KeyboardInterrupt:
        # Clean up socket
        print("Exiting...")
