import os, argparse

parser = argparse.ArgumentParser()

parser.add_argument('-u', required=True, help='user topology json directory')
args = parser.parse_args()

# Prefetch 80GB, aggresive and slow experiment
os.system(f'python3 ../scripts/config_gen.py -u "{args.u}" \
          -o gs_5.json \
          -m motion  \
          -l constellation_experiment/log_gs_5 \
          --gs "../scripts/gs_short.json" \
          -s "../scripts/starlinks_5_2_brief.txt"\
          --prefetchByte 80000000000\
          --useGs\
          --prefetch_strategy get_most_accessed_items')

# GS will recommand most recent objects
os.system(f'python3 ../scripts/config_gen.py -u "{args.u}" \
          -o gs_6.json \
          -m motion  \
          -l constellation_experiment/log_gs_6 \
          --gs "../scripts/gs_short.json" \
          -s "../scripts/starlinks_5_2_brief.txt"\
          --prefetchByte 20000000000\
          --useGs\
          --prefetch_strategy get_most_recent_items')

# GS will recommand most recent objects and prefetch 80GB
os.system(f'python3 ../scripts/config_gen.py -u "{args.u}" \
          -o gs_7.json \
          -m motion  \
          -l constellation_experiment/log_gs_7 \
          --gs "../scripts/gs_short.json" \
          -s "../scripts/starlinks_5_2_brief.txt"\
          --prefetchByte 80000000000\
          --useGs\
          --prefetch_strategy get_most_recent_items')

# fetch from neighbor on demand 
os.system(f'python3 ../scripts/config_gen.py -u "{args.u}" \
          -o gs_8.json \
          -m motion  \
          -l constellation_experiment/log_gs_8 \
          --gs "../scripts/gs_short.json" \
          -s "../scripts/starlinks_5_2_brief.txt"\
          --prefetchByte 80000000000\
          --prefetch_strategy get_most_recent_items,\
          --req_handle check_lru_on_demand')
