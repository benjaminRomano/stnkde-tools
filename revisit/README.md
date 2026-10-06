# Queensboro approach, revisited

`explorer.html` is the main self-contained deliverable. It includes the interactive network explorer, a **Report** tab with the investigative narrative, and the source-backed decision dashboard. The precomputed HTML needs no database, Python, Node or CDN. Serve the parent outputs directory so the paper-reconstruction link works:

```sh
python3 -m http.server 8878 --bind 127.0.0.1 --directory ..
```

Visit `/revisit/explorer.html#report` for the narrative or `#maps` for exploration. `investigation.html` and `decision-dashboard.html` are also standalone files.

## What the record supports

The fixed Manhattan approach-and-nearby-streets window contains 17,325 geolocated crashes in **13 complete calendar years, 2013–2025**. The wider buffered download contains 28,226 records. The NYC series starts in July 2012, so 2012 is excluded; 2026 is excluded as requested. The historical paper reproduction remains separate in `../reproduction/` and retains partial 2012/2017 observations solely to approximate the paper.

* Injury crashes: 233 in 2013 versus 203 in 2025, down 12.9%. This is not a uniform downward trend; 2017 had 335.
* Three-year injury totals: 687 in 2013–2015 versus 653 in 2023–2025, down 4.9%.
* Recent timing: 15:00 is the peak pooled hour; 15:00–17:59 contains more injury crashes than 08:00–10:59. The evening/morning ratio is 1.42 in 2021–2025 and 1.33 in 2022–2025.
* September is the peak recent month after dividing by observed days in each month. That observation does not control for travel, weather or daylight.

These are recorded injury burdens and calendar-day frequencies, **not per-trip crash rates**. The rectangle covers the Manhattan approach and nearby roads, not the entire bridge or Queens landing. Maps show a 700 m closeup by default; headline counts continue to refer to the full fixed rectangle.

COVID changed volume and travel behavior in 2020 and the recovery period. The year controls/gallery mark 2020 and 2021. Timing comparisons omit 2020, with a separate 2022–2025 sensitivity window omitting 2021 too. Exclusion does not establish a return to pre-pandemic exposure. NYPD's April 2020 property-damage-only reporting change also makes total-crash comparisons problematic.

The narrative links primary records for speed limits, Second Avenue connections installed near the bridge entrance in 2019, upper-deck lane reductions beginning February 2022, 24/7 speed cameras, congestion-zone tolling and the 2025 pedestrian/cycling separation. Dates form hypotheses; no causal effect is estimated. A local evaluation needs treatment locations/dates, comparison roads, aligned exposure, construction history and uncertainty.

## Reading the maps

Default: injury crashes, yearly snapshots, 75 m quartic kernel, event-bearing cells, no cutoff, a shared positive-value 99th-percentile cap. This is a useful visual starting choice, not a cross-validated optimum. Controls compare 50/75/100 m and both kernels. Rankings change: the top 75 full-field lixels have 73.3–100% overlap across the six tested settings.

Coverage and cutoff change display only. The original event-bearing-cell policy is retained intentionally. Full coverage includes positive smoothed estimates where raw counts are zero. Change maps use the full field on both sides and a symmetric diverging scale; unsmoothed views expose snapped counts. Click a segment for its name, count and intensity. Buffered contributors may appear in selected-segment counts; headline totals use raw points inside the rectangle.

Yearly bandwidth one has no adjacent-year borrowing. Hourly bandwidth two wraps through midnight and includes neighboring-hour contributions on the source segment. Monthly bandwidth one has no adjacent-month borrowing. Values are divided by the relevant calendar days and multiplied by 100. The method uses current roads and a planar midpoint graph: historical topology and grade separation remain limitations.

Yearly small multiples and synchronized maps are the principal decision views. The wall cube and stacked map cube show the same data and are secondary exploration views because occlusion makes precise comparisons harder. Export opens a 3200 × 1700 PNG preview with a Save PNG link. The in-app browser did not complete native download events during QA; the preview itself was checked and its actual rendered PNG is supplied as `figures/explorer-export.png`.

## Data and verification

`data/sources.json` records the exact public query, retrieval time and SHA-256. Keyset pagination, expected counts and collision-ID uniqueness are checked. All 17,325 local located points match within 100 m; 410 buffered records outside the local window exceed the threshold. Borough labels are not used as a spatial filter. Unlocatable crashes cannot be assigned locally; borough-wide coordinate completeness is only context. Injury and persons-killed fields are complete locally, but source fatalities are preliminary and rare.

`analyze.py` creates the isolated `revisit_2025` schema, preserving the paper `repro` schema. It reuses a fingerprint-checked sparse distance cache. Aggregate browser data contain counts, not individual crash IDs.

Seven conservation/calendar/graph checks passed. `validate_frontend.py` checks the **actual shipped JavaScript estimator** against independent direct CSV sums, exact database assignments and unrounded graph distances for six configurations over all 1,095 displayed lixels. Raw counts agree exactly; maximum intensity error is below 1e-9 from rounded browser distances. Midnight, leap years, month-day denominators and both kernels are covered.

The evidence readers use canonical Data Analytics artifacts and its official portable builder. Dashboard verification passed for 30 blocks/7 charts; narrative verification passed for 29 blocks/4 native charts/4 custom network-map blocks, source dialogs and 1440/390 px layouts. A later specialized static-extraction pass timed out even on the previously verified narrative file. The final report retains the previously verified chart SVGs after checking identical chart specifications and datasets, then passes the official source-equality/browser verifier. Cache reuse is fingerprint-gated in `package_evidence.mjs`; ordinary charts use the canonical reader, while network geometry is custom SVG/Canvas/Three.js.

UI evidence and remaining limits are in `results/browser-qa.json`. Original numerical validation remains in `../reproduction/results/`. `figures/report-tab.jpg` and `figures/report-mobile.jpg` show the implemented Report tab.

## Rebuild

First rebuild or reuse the common PostGIS graph with `../reproduction/run_reproduction.py`. Use its pinned Python requirements and running local Docker database. Then:

```sh
/path/to/python run_revisit.py --build
```

Node 24 works; JavaScript versions are locked. `DATA_ANALYTICS_PLUGIN_ROOT` can point to an installed Data Analytics plugin directory for rebuilding canonical evidence HTML. The default resolves the version used in this session under the user's Codex plugin cache. This dependency is needed only to rebuild the evidence readers; the delivered HTML is portable.

The download helper reuses the bundled checksum-verified snapshot. Fetching different source records requires a separate input directory and fresh analysis schema. The narrative is authored around this frozen snapshot; review and update its numerical claims if inputs change. Never reuse old chart SVGs for changed datasets: the rendering fingerprint rejects them.
