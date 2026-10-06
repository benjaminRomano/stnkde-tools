"""Export aggregate poster inputs from a rebuilt reproduction database."""
from pathlib import Path
import hashlib
import json
import sys

import numpy as np
import psycopg
import shapely

ROOT = Path(__file__).resolve().parent
REPRODUCTION = ROOT.parent / "reproduction"
sys.path.insert(0, str(REPRODUCTION))
from pipeline import DSN


def main():
    with psycopg.connect(DSN) as conn:
        rows = conn.execute("SELECT id,ST_AsBinary(geom) FROM repro.lixel ORDER BY id").fetchall()
        counts = np.zeros(len(rows))
        for lid, count in conn.execute("""
            SELECT a.lixel_id,count(*) FROM repro.assignment a
            JOIN repro.event e ON e.id=a.event_id
            WHERE a.method='exact' AND a.accepted AND e.year BETWEEN 2012 AND 2017
            GROUP BY a.lixel_id
        """):
            counts[lid - 1] = count
    assert all(lid == i + 1 for i, (lid, _) in enumerate(rows))
    fingerprint = hashlib.sha256(
        b"".join(bytes(geometry) for _, geometry in rows) + b"|100.0|midpoint-v1"
    ).hexdigest()
    assert fingerprint == json.loads(
        (REPRODUCTION / "results/distances.metadata.json").read_text()
    )["graph_sha256"]
    lines = [np.asarray(shapely.from_wkb(bytes(wkb)).coords)[:, :2] for _, wkb in rows]
    folder = ROOT / "data"
    folder.mkdir(exist_ok=True)
    destination = folder / "poster-inputs.npz"
    with np.load(REPRODUCTION / "results/densities.npz") as reference:
        np.savez_compressed(
            destination,
            ids=np.array([lid for lid, _ in rows]),
            coordinates=np.concatenate(lines),
            offsets=np.r_[0, np.cumsum([len(line) for line in lines])],
            counts=counts,
            reference_density=reference["spatial_corrected"],
        )
    metadata = {
        "file": destination.name,
        "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
        "graph_sha256": fingerprint,
        "lixels": len(rows),
        "accepted_crashes": int(counts.sum()),
        "crs": "EPSG:26918",
        "observation_window": "2012-07 through 2017-04-30",
        "inputs": "reproduction/data/sources.json",
        "assignment": "exact nearest, accepted within 100 meters; ID tie break",
        "provenance": "Exported from repro.lixel and exact accepted repro.assignment; reference_density from reproduction/results/densities.npz",
    }
    (folder / "sources.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Exported {len(rows):,} lixels and {int(counts.sum()):,} accepted collisions.")


if __name__ == "__main__":
    main()
