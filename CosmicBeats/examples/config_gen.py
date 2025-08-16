import argparse, json, os

parser = argparse.ArgumentParser()

parser.add_argument('-o', required=True, help='output file name')
parser.add_argument('-m', required=True, choices=['motion', 'stationary', 'orbitstationary'], help='movement of satellite')
parser.add_argument('-u', required=True, help='user topology json directory')
parser.add_argument('-l', required=True, help='emulation log directory')

args = parser.parse_args()

# process user topology
with open(args.u, 'r') as f:
    user_topology = json.load(f)
user_idx = 10000

output_file = open(args.o, 'w+')
base_str = """
{
    "topologies":
    [
        {
            "name": "Constln1",
            "id": 0,
            "nodes":
            [
    """

motion_model_map = {
    'motion': 'ModelOrbit',
    'orbitstationary': 'ModelStationaryOrbit',
    'stationary': 'ModelOrbitNoMotion'
}

sat_string = """
                {
                    "type": "SAT",
                    "iname": "SatelliteBasic",
                    "nodeid": 2245,
                    "loglevel": "all",
                    "tle_1": "1 48589U 21041AN  24123.71060823 -.00001104  00000+0 -55242-4 0  9990", 
                    "tle_2": "2 48589  53.0533 286.2058 0001364  90.8859 269.2286 15.06396736163763",
                    "additionalargs": "",
                    "models":[
                        {
                            "iname": "%s"
                        },
                        {
                            "iname": "ModelCDNProvider",
                            "neighbors": [2579, 2698, 2654, 2685]
                        },
                        {
                            "iname": "ModelFovTimeBased",
                            "min_elevation": 25
                        }
                    ] 
                },\n""" % (motion_model_map[args.m])

def get_user_string(node_id, lat, lon, trace):
    string = """
                {
                    "type": "User",
                    "iname": "UserBasic",
                    "nodeid": %d,
                    "loglevel": "all",
                    "latitude": %f,
                    "longitude": %f,
                    "elevation": 0.0,
                    "trace": "%s",
                    "additionalargs": "",
                    
                    "models":[
                        {
                            "iname": "ModelCDNUser"
                        },
                        {
                            "iname": "ModelFovTimeBased",
                            "min_elevation": 25
                        }
                    ]
                }""" % (node_id, lat, lon,  trace)
    return string

output_file.write(base_str)
output_file.write(sat_string)
for user in user_topology:
    user_idx += 1
    output_file.write(get_user_string(user_idx, user[0], user[1], user[2]))
    output_file.write(',\n')

output_file.seek(output_file.tell() - 2, os.SEEK_SET)

start_time = '2024-05-02 12:00:00'
end_time = '2024-05-03 12:00:00' 
delta = 20
log_dir = args.l 
end_str = """
            ]
        }
    ],
    "simtime":
    {
        "starttime": "%s",
        "endtime": "%s",
        "delta": %s
    },
    "simlogsetup":
    {
        "loghandler": "LoggerFileChunkwise",
        "logfolder": "%s",
        "logchunksize": 100
    }
}
""" % (start_time, end_time, delta, log_dir)
output_file.write(end_str)
output_file.close()