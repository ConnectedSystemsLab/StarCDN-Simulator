## Details on configuration files
- Fetch config (fetch_k_x.json): This file specifies the destination of relayed fetch. Specifically, it will outline the nearest neighbor x hops away. If the satellite is out of place it's going to be -1. 
## Analyze scripts
When running the analyze script users will see the following outputs
```
python3 analyze_script.py ./output/
...
Finish ./output/SAT_2292
Finish ./output/SAT_1465
Finish ./output/SAT_1891
Results:
[3.10815000e+05 1.79613149e+08 2.04301000e+05 7.06343860e+07
 2.61700000e+03 2.59907400e+06 1.29894000e+05 1.53964000e+05
 0.00000000e+00 7.85100000e+03 0.00000000e+00 0.00000000e+00]
0.6573074015089362 0.3932584356616341
```
The results are in format of `[number of objects requested, number of byte in KB requested, number of object hit, number of byte hit in KB]`, request hit rate, byte hit rate.
