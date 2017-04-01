# STNKDE-TOOLS


### Load Data Example

```
$ python load_data.py -host localhost -d test2 -u bromano -p password -n ./manhattan_streets/manhattan_streets.shp -e ./manhattan_crashes/manhattan_crashes.shp -s 26918
```


### Compute Distances Example
```
$ python compute_distances.py -host localhost -d test2 -u bromano -p password -l 50 -s 26918 -sb 100
```


### Compute Densities Example
```
$ python compute_densities.py -host localhost -d test2 -u bromano -p password -l 50 -s 26918 -sb 100
```
