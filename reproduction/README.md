# STNKDE paper reconstruction

Open `report.html` using a local static server. It contains the entire Three.js bundle, computed bridge-area data, original paper images and regenerated figures in one file. The precomputed report needs neither Python nor PostgreSQL.

```sh
python3 -m http.server 8877 --bind 127.0.0.1
```

Visit http://127.0.0.1:8877/report.html. Direct file opening may also work in a browser, but verification used HTTP because the testing browser blocks file URLs.

## What was reproduced

The supplied paper is Benjamin Romano and Zhe Jiang, *Visualizing Traffic Accident Hotspots Based on Spatial-Temporal Network Kernel Density Estimation (Demo Paper)*, SIGSPATIAL 2017, DOI 10.1145/3139958.3139981.

The reconstruction uses its 50 m lixels, 100 m spatial bandwidth, annual bandwidth of one year, hourly bandwidth of two hours, and six snapshots at 08, 09, 10, 15, 16 and 17. Figures 2–5 are compared with reference images extracted from the supplied PDF. The interactive annual cube is rendered by Three.js. The large static road-wall plates, spatial maps, hourly closeups and profile use Matplotlib. Every reconstructed figure has separate before and after images; the modal supports 150–300% zoom. Figure 3 can hide its planar background without changing network estimates.

This is an approximate historical-data reconstruction, not recovery of the original experiment. The 2017 road/crash shapefiles, exact cutoff, QGIS project, cameras, class breaks and planar KDE bandwidth were unavailable. Downloaded historical records can have changed since publication. Current NYS roads are a substantially different input from the approximately 900 road features described in the paper. Both comparison sides use the same reconstructed graph, isolating numerical changes from topology differences.

## Inputs and environment

* 192,042 Manhattan crash records, from 2012 through April 30, 2017, downloaded from NYC Open Data. 187,294 have coordinates within the study bounds. Invalid/missing locations remain recorded with quality statuses.
* 15,755 current NYS road features selected by Manhattan county attributes. Deterministic planar noding on a 1 m grid produces 34,408 lixels, including 1,095 intersecting the bridge viewport.
* Contextual roads outside Manhattan are cartographic background only; they never enter network density calculations.
* PostgreSQL 18.3, PostGIS 3.6.3 and pgRouting 4.0.1, using the official pgRouting Docker image pinned by digest. The image runs under amd64 emulation on this Apple Silicon machine.
* Python 3.13; exact Python versions in `requirements.lock.txt`. Three.js 0.186.1 and Vite 8.3.2; JavaScript versions in `site/package-lock.json`.
* URLs, retrieval time and SHA-256 hashes: `data/sources.json`. The data snapshot is bundled, so future source changes need not alter this reconstruction.

The original clone was commit `be8555c014e2c6cac81f316c13c9bac680a4f593`. `legacy/` preserves its unmodified scripts as an arithmetic reference. `pipeline.py` is the working modern replacement; the original scripts still contain their historical bugs and are not the supported execution path.

## Correction stages

| Stage | Spatial argument | Source-lixel time smoothing | Assignment | Kernel |
|---|---|---|---|---|
| baseline | time distance, as in repository | same-time only | bounding-box nearest | Epanechnikov |
| formula | actual network distance | same-time only | bounding-box nearest | Epanechnikov |
| corrected | actual network distance | all supported times | exact nearest, ≤100 m | Epanechnikov |
| paper | actual network distance | all supported times | exact nearest, ≤100 m | quartic/biweight |

**Coverage is independent of these calculation stages.** Event-bearing-only output was an intentional original design choice to reduce verbosity. Complete calculated arrays are retained, then either event-bearing or full-field coverage is applied to both sides. Figure 5 uses event-bearing cells on both sides. Figure 4 uses full calculated fields on both sides, before the original sparse export, to reveal the road-wall pattern. The interactive cube also offers event-bearing coverage. This distinction is documented in `results/figure-settings.json`.

The corrected estimator sums `count × spatial_kernel / spatial_bandwidth × temporal_kernel / temporal_bandwidth` over source lixels and time bins. Spatial distance is shortest path between lixel midpoints, with a 100 m support cutoff. Hourly temporal distance wraps at midnight; annual distance uses actual calendar years. Complete explicit axes prevent gaps from compressing time. The historical data contain every year and hour, so the missing-bin defect does not contribute additional differences in this run.

The repository function called quartic actually computes `0.75 × (1 − u²)`, the Epanechnikov kernel. True quartic is `15/16 × (1 − u²)²`. Both are zero outside their support. Kernel replacement is deliberately a separate stage. With annual temporal bandwidth one, adjacent years are at the zero-support boundary; annual values do not borrow contributions from neighboring years.

Assignment records keep the chosen lixel, exact offset distance, fraction along the lixel and acceptance status. 43,820 matches differ between bounding-box and exact nearest geometry. Thirteen exact matches exceed 100 m and are excluded from corrected runs. This rejection threshold is an explicit additional quality policy, rather than a recovered paper setting. Original bounding-box ties were unspecified; the reconstruction resolves ties by lixel ID.

## Findings

On cells originally displayed, changing only the spatial argument reduces summed annual intensity by **38.49%** and hourly intensity by **46.52%**. Choosing full-field display additionally exposes smoothed zero-count cells: **111,272 annual cells** and **474,133 hourly cells** across Manhattan. The top 1% of cells overlap by **24.90% annually** and **27.38% hourly** between the historical sparse baseline and the corrected full field. Those overlaps mix calculation and coverage differences; they do not isolate arithmetic correctness.

The corrected full-map sum increases because the original sparse export intentionally omitted smoothed zero-count cells. This is compatible with the formula-only decrease. Sums of sampled densities are comparison statistics, not conserved probability mass or exposure-adjusted crash rates.

In the bridge viewport, evening/morning sampled-intensity ratio decreases from **1.45 to 1.31**, comparing 15–17 against 08–10. The peak moves from 17:00 to 16:00. Thus the qualitative evening pattern survives in this reconstructed dataset, while its magnitude and exact timing change. This is not a statistical test of the paper's original data.

All before/after figures share a numerical color scale. The default caps at the shared positive-value 99th percentile; higher values use red. The maximum option avoids capping. Change maps have a separate symmetric, explicitly labeled scale. Year and hour figures use different scales because their temporal units differ. Layer spacing is visual exaggeration and does not enter the estimator. Partial 2012 and 2017 coverage prevents interpreting raw annual comparisons as changes in crash risk.

## Database and performance changes

The modern schema separates datasets, lixel sets, assignments and analysis runs. Foreign keys, uniqueness and validity checks replace table names as the principal record of experiment settings. Source hashes reject stale input reuse; a geometry/radius fingerprint guards the distance cache. Completed phases can resume, and each database phase commits atomically. Failed topology construction is surfaced rather than swallowed.

Spatial GiST indexes support exact KNN event matching. A sparse midpoint graph and bounded Dijkstra replace repeated per-event database routing. Matrix multiplication aggregates spatial and temporal kernels without repeated neighbor queries. Canonical distance pairs have both source and target lookup support. Zero-count density is stored as an analytical result independently of the observed count.

One local, sampled driving-distance workload took approximately 0.08 s in pgRouting and 0.001 s in SciPy, excluding import and graph construction. These are local observations, not a whole-pipeline speedup claim. `results/summary.json` labels resumable timings explicitly; already-completed phases are reused.

## Validation

* Eight analytical tests pass: kernel normalization/support, invalid bandwidth, actual calendar gaps, midnight wrap, spatial decay, self-lixel temporal smoothing, hand-summed estimator equivalence, and network rather than straight-line distance.
* SciPy agrees with pgRouting on all 279 reachable midpoint distances from 24 deterministic sampled sources; maximum error is zero at reported precision. This validates the reconstructed midpoint graph, not equivalence to the missing 2017 topology.
* Optimized baseline agrees with the unmodified original arixel worker on 64 source fixtures over the full hourly axis; maximum absolute difference is `6.94e-18`. Neighbor SQL is mocked with actual reconstructed graph distances. The original sparse output mask is applied separately.
* Database checks account for every input record, conserve accepted raw counts in all eight runs, verify lixel lengths and completed statuses, retain positive zero-count output, and check that exported arrays are finite/nonnegative.
* Browser checks cover correction stages, all views, slider, shared scales, camera synchronization, inspection, PNG export, embedded images and 390 px mobile layout. Browser console checks on the standalone report show no warnings/errors.

Evidence is in `results/summary.json`, `results/database-validation.json`, `results/unit-tests.txt`, `results/bridge_temporal_profile.json` and `results/browser-qa.json`.

## Rebuild the analysis

Use Docker, Python 3.13 and Node 22.19 or newer (Node 24 also works). From this directory:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock.txt
docker compose up -d --wait
.venv/bin/python run_reproduction.py --build
```

The default connection is a localhost-only database on port 55437, user `postgres`, database `stnkde`, password `stnkde-local-only`. `STNKDE_DSN` can override it. The compose configuration is for this isolated local reproduction.

The original session's running database is container `stnkde-reproduction-01a0`, which already uses port 55437. To reuse it, skip `docker compose up`. To rebuild in a separate fresh database while retaining it, use a different compose port and matching DSN. Do not run two containers on the same port.

Bundled inputs are used by default. `--download` fetches missing inputs and verifies existing cached inputs; it preserves the original retrieval time. Fetching a new snapshot should use a separate output directory with no prior inputs or page caches. Reconstructing different inputs requires a fresh database; checksum mismatch raises an error instead of silently reusing stale tables. Schema/parameters are intentionally fixed to the recovered experiment, rather than exposed as arbitrary command-line options.

The runner regenerates `large_figures.py` after the analytical figures. The hourly plates use a 500 m frame around the bridge exit and an estimated −35° rotation. The static cube uses 12 m display spacing, 10 m strips and a closer camera. Figure 3 uses a closer estimated city frame; its planar bandwidth remains an explicitly labeled approximation. All generated PNGs are embedded in `report.html`. Vector SVGs are supplied for Figures 2 and 5. Original reference framing, input geometry and class breaks remain unavailable, so visual resemblance is not proof of numerical equivalence to the paper.

The extended **2013–2025 complete-year** study is in `../revisit/`. Serve the parent outputs directory to make cross-links work: `python3 -m http.server 8878 --bind 127.0.0.1 --directory ..`, then visit `/reproduction/report.html` or `/revisit/explorer.html`. Its Report tab contains the source-backed investigation.

## Remaining modeling limits

The common graph is planar and does not distinguish bridges, tunnels and grade-separated crossings. The current road source includes elevation attributes, but mapping them into a faithful 2017 topology is a separate substantive model change. No claim is made that this graph recovers physical connectivity at every crossing.

Crashes are represented at lixel midpoints after assignment, as in the historical method. Actual event offsets are preserved but are not used in the kernel; shorter lixels or event-to-network distances would reduce this approximation. Terminal edges and junctions have no explicit mass-conserving boundary/branch correction. The estimator is unnormalized intensity, not a probability density. Crash exposure, reporting changes, uncertainty and statistical significance are not modeled. Those remain important before using these maps to make transportation decisions.
