# STNKDE-TOOLS

## Modern reconstruction and collision atlas

This repository now includes a reproducible reconstruction of the 2017 demo, calculation corrections, a complete-year Queensboro approach investigation, and the final print-ready NYC collision poster. The original scripts below remain unchanged as historical references; use `reproduction/pipeline.py` for the supported modern implementation.

| Deliverable | Contents |
|---|---|
| [Paper reconstruction](reproduction/README.md) | Pinned PostGIS/pgRouting environment, frozen inputs, corrected estimator, matched before/after figures, interactive Three.js report |
| [Queensboro study](revisit/README.md) | 2013–2025 complete years, hourly/seasonal exploration, investigative Report tab, source-backed policy context and COVID/reporting sensitivities |
| [Final poster and construction notes](poster/README.md) | Selected logarithmic composition, 4800 × 7800 PNG for 16 × 26 inches, standalone renderer, provenance and comparison grids |

Preview the delivered reports without a database or build step, from the repository root:

```sh
python3 -m http.server 8878 --bind 127.0.0.1
```

Open `/reproduction/report.html` or `/revisit/explorer.html#report`. Rebuild instructions and modeling limits are in each deliverable's README. These are recorded collision concentrations and burdens, not traffic-exposure-adjusted crash rates. The common graph uses current roads and cannot recover every detail of the missing historical network.

## Historical command examples


### Load Data Example

```
$ python load_data.py -host localhost -d test2 -u bromano -p password -n ./manhattan_streets/manhattan_streets.shp -e ./manhattan_crashes/manhattan_crashes.shp -s 26918
```

### Create Lixels Example
```
$ python create_lixels.py -host localhost -d test2 -u bromano -p password -l 50 -s 26918
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
Replace time_ids with display names in arixels table
Need to clean up code
