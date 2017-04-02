# STNKDE-TOOLS


### Load Data Example

```
$ python load_data.py -host localhost -d test2 -u bromano -p password -n ./manhattan_streets/manhattan_streets.shp -e ./manhattan_crashes/manhattan_crashes.shp -s 26918
```


### Compute Distances Example
```
$ python compute_distances.py -host localhost -d test2 -u bromano -p password -l 50 -s 26918 -sb 100
```


### Compute Lixel Densities Example
```
$ python compute_lixel_densities.py -host localhost -d test2 -u bromano -p password -l 50 -s 26918 -sb 100
```

### Compute Arixel Densities Example
```
$ python compute_arixel_densities.py -host localhost -d test2 -u bromano -p password -l 50 -s 26918 -ssb 100 -tsb 2 -t y -df crash_date
```


# ISSUES
Soem of the arixel densities are negative. 
Possible causes are bad density computing functions or improper time distance computations... 
Even with this issue, the overall pattern of the 3d visualization looks consistent with the lixel approach.