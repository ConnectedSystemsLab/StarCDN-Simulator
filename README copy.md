# StarCDN-Simulator
The simulation framework for SIGCOMM 2025 paper: StarCDN: Moving Content Delivery Networks to Space.
```
@inproceedings{starcdn,
  title={StarCDN: Moving Content Delivery Networks to Space},
  author={William X. Zheng, Aryan Taneja, Maleeha Masood, Anirudh Sabnis, Ramesh K. Sitaraman, and Deepak Vasisht},
  series={SIGCOMM 2025},
  year={2025}
}
```
## 1. Installation
This repository uses python3.9+. To install dependendies run: `pip install -r requirements.txt`
## 2. Run CosmicBeats Simulator
### 2.1 Preppare config file
CosmicBeats requires a config file to run. Example config files is in `CosmicBeats/data/lru.json`.
There are several tunnable configurations:
```
===General===
logfolder: Output for the satellite traffic traces.
endtime: End time for the simulation. This should be changed based on the length of traces. The starttime should not be changed due to the collection time of the TLE data for this example. If user changes the TLE for satellites, then starttime should match the collection time.
delta: Increment of simulation in seconds.

===Satellites===
topology_file: The topology files for K=2 or K=3 (files in ./data).
ModelOrbit: This could be changed to ModelOrbitNoMotion if simulating stationary satellites.

===Clients===
latitude/longitude: Location of the CDN traces.
trace: path to the trace
min_elevation: minimum FoV elevation for a satellite to be scheduled.
```
The minimum requirement to change the config file is to set the `trace` fields for all locations. We keed the trace location and path we used for our experiment but we don't save the actual traces in this repo.
### 2.2 Run Simulation
Use `python3 main.py config_path` to run the simulation. Due to the limitaion of CosmicBeats, the program does not support multi-process. However, the time required to run our synthetic traces should be less than one day.
## 3. Run Cache Replayer
Before proceeding to this section, user must finish Step 2 and have a log directory produced by CosmicBeats.

To run the simulation use: `python3 master.py path_cosmicbeats_config path_cosmicbeats_output output_path cache_size relayed_fetch_config`.
The relayed_fetch_config should be selected based on the topology (K=2 or K=3) from `./cache-replayer/fetch_k_x.json`. The cache size is in the unit of KB.

Finally use `python3 analyze_script.py output_path` to process the replayer's output and get the hit rate stat.