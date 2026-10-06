"""Render the selected poster or controlled color/framing/year alternatives.

Frozen aggregate geometry/counts and a fingerprinted network-distance cache
make rendering independent of PostgreSQL. Licensed fonts are supplied locally.
"""
from pathlib import Path
import argparse
import hashlib
import json
import os

ROOT = Path(__file__).resolve().parent
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".matplotlib"))

import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["mathtext.fontset"] = "dejavuserif"
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.colors import LinearSegmentedColormap, FuncNorm, Normalize, PowerNorm
from matplotlib.font_manager import FontProperties
from matplotlib.patches import Rectangle
from matplotlib.cm import ScalarMappable
from matplotlib.textpath import TextToPath
import numpy as np
from PIL import Image, ImageCms, PngImagePlugin
from scipy.sparse import load_npz, csr_matrix
import shapely

PALETTE = ["#1d70ad", "#36a3b8", "#ffd84a", "#ff7d24", "#e51e26"]
INK = "#20272d"
MUTED = "#67727a"


def fonts(directory):
    """Use owned font files, or extract installed macOS Baskerville faces locally."""
    paths = {name: directory / f"Baskerville-{name}.ttf" for name in ["Bold", "Regular"]}
    if not all(path.exists() for path in paths.values()):
        collection = Path("/System/Library/Fonts/Supplemental/Baskerville.ttc")
        if not collection.exists():
            raise FileNotFoundError(
                "Supply licensed Baskerville-Bold.ttf and Baskerville-Regular.ttf with --font-dir."
            )
        from fontTools.ttLib import TTCollection
        directory.mkdir(parents=True, exist_ok=True)
        for font in TTCollection(collection).fonts:
            style = font["name"].getDebugName(2)
            if style in paths and not paths[style].exists():
                font.save(paths[style])
    return paths


def inputs():
    repro = ROOT.parent / "reproduction"
    metadata = json.loads((ROOT / "data/sources.json").read_text())
    path = ROOT / "data/poster-inputs.npz"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == metadata["sha256"]
    with np.load(path) as saved:
        ids = saved["ids"]
        points, offsets = saved["coordinates"], saved["offsets"]
        lines = [points[a:b] for a, b in zip(offsets[:-1], offsets[1:])]
        counts, reference = saved["counts"], saved["reference_density"]
    assert np.array_equal(ids, np.arange(1, len(lines) + 1))
    geometries = [shapely.LineString(line) for line in lines]
    fingerprint = hashlib.sha256(
        b"".join(shapely.to_wkb(geometry) for geometry in geometries) + b"|100.0|midpoint-v1"
    ).hexdigest()
    assert fingerprint == metadata["graph_sha256"]
    assert fingerprint == json.loads((repro / "results/distances.metadata.json").read_text())["graph_sha256"]
    assert counts.sum() == metadata["accepted_crashes"]
    distances = load_npz(repro / "results/distances.npz").tocoo()
    context_path = repro / "data/context_roads.json"
    context_metadata = json.loads((repro / "data/context_sources.json").read_text())
    assert hashlib.sha256(context_path.read_bytes()).hexdigest() == context_metadata["sha256"]
    context = json.loads(context_path.read_text())
    background = [np.asarray(line)[:, :2] for feature in context["features"] for line in feature["geometry"]["paths"]]
    return metadata, lines, geometries, counts, reference, distances, background


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "generated/final.png")
    parser.add_argument("--font-dir", type=Path, default=ROOT / ".fonts")
    parser.add_argument("--scale", choices=["log1p", "linear", "linear-cap", "linear-p95", "rank", "power"], default="log1p")
    parser.add_argument("--framing", choices=["01-current", "02-manhattan", "03-queens", "04-midtown", "05-bridge", "06-approach"], default="01-current")
    parser.add_argument("--kernel", choices=["epanechnikov", "quartic"], default="epanechnikov")
    parser.add_argument("--bandwidth", type=float, choices=[50, 75, 100], default=100)
    parser.add_argument("--year", type=int, choices=range(2017, 2027))
    parser.add_argument("--matched-period", action="store_true", help="Use Jan 1–June 11 of the selected year, matching partial 2026.")
    args = parser.parse_args()
    if args.matched_period and args.year is None:
        parser.error("--matched-period requires --year")
    font = fonts(args.font_dir)
    metadata, lines, geometries, counts, reference, dist, background = inputs()
    b = np.maximum(0, 1 - (dist.data / args.bandwidth) ** 2)
    weight = (.75 * b if args.kernel == "epanechnikov" else .9375 * b * b) / args.bandwidth
    matrix = csr_matrix((weight, (dist.row, dist.col)), shape=dist.shape)
    original_b = np.maximum(0, 1 - (dist.data / 100) ** 2)
    original = csr_matrix((.75 * original_b / 100, (dist.row, dist.col)), shape=dist.shape)
    error = float(np.max(np.abs(original @ counts - reference)))
    assert error < 1e-12
    values = np.asarray(matrix @ counts)
    footer = "New York City Open Data, 2012–2017"
    scale_maximum = 5.60221994929095
    frame_data = json.loads((ROOT / "studies/poster-log-framing/methodology.json").read_text())
    extent = np.array(next(option["extent_m"] for option in frame_data["options"] if option["slug"] == args.framing))
    if args.year is not None:
        annual = ROOT / "annual/data"
        cache = json.loads((annual / "annual-counts.metadata.json").read_text())
        assert cache["source_key"]["graph_sha256"] == metadata["graph_sha256"]
        assert hashlib.sha256((annual / "annual-counts.npz").read_bytes()).hexdigest() == cache["counts_sha256"]
        with np.load(annual / "annual-counts.npz") as saved:
            all_counts = saved["same_period" if args.matched_period else "annual"]
            assert np.all(all_counts >= 0)
            counts = all_counts[:, args.year - 2017]
            values = np.asarray(matrix @ counts)
            # Original yearly study used this fixed viewport to calibrate every panel.
            calibration_extent = next(option["extent_m"] for option in frame_data["options"] if option["slug"] == "02-manhattan")
            calibration_ids = np.flatnonzero([g.intersects(shapely.box(*calibration_extent)) for g in geometries])
            with np.load(annual / "annual-counts.npz") as saved_annual:
                scale_maximum = float(np.max(np.asarray(matrix @ saved_annual["annual"])[calibration_ids]))
        footer = f"New York City Open Data, {args.year}"
        if args.matched_period:
            footer += " · Jan 1–Jun 11"
        elif args.year == 2026:
            footer += " · through Jun 11"
    ids = np.flatnonzero([g.intersects(shapely.box(*extent)) for g in geometries])
    active = ids[values[ids] > 0]
    positive = values[active]
    assert np.isfinite(values).all() and (values >= 0).all()
    if args.scale == "log1p":
        norm = FuncNorm((np.log1p, np.expm1), vmin=0, vmax=scale_maximum, clip=True)
    elif args.scale == "linear":
        norm = Normalize(0, scale_maximum)
    elif args.scale == "linear-cap":
        norm = Normalize(0, 2.5957330868922766)
    elif args.scale == "linear-p95":
        norm = Normalize(0, float(np.quantile(positive, .95)))
    elif args.scale == "power":
        norm = PowerNorm(1.5, vmin=0, vmax=2.5957330868922766, clip=True)
    else:
        unique, frequency = np.unique(positive, return_counts=True)
        rx = np.r_[0., unique]
        ry = np.r_[0., np.cumsum(frequency) / len(positive)]
        norm = FuncNorm((lambda a: np.interp(a, rx, ry), lambda a: np.interp(a, ry, rx)), vmin=0, vmax=float(positive.max()), clip=True)
    cmap = LinearSegmentedColormap.from_list("poster", PALETTE)

    def text(fig, x, y, content, size, path, color=INK, ha="left"):
        fig.text(x, y, content, fontproperties=FontProperties(fname=path, size=size), color=color, ha=ha, zorder=12)

    def tracked(fig, x, y, content, size, spacing):
        prop = FontProperties(fname=font["Bold"], size=size)
        tp = TextToPath()
        widths = [tp.get_text_width_height_descent(char, prop, False)[0] for char in content]
        total = sum(widths) + spacing * (len(content) - 1)
        conversion = fig.get_figwidth() * 72
        position = x - total / conversion / 2
        for char, width in zip(content, widths):
            text(fig, position, y, char, size, font["Bold"])
            position += (width + spacing) / conversion

    fig = plt.figure(figsize=(16, 26), facecolor="#fffefa")
    ax = fig.add_axes([.04, .050, .92, .850], facecolor="white")
    ax.add_collection(LineCollection(background, colors="#849bad", linewidths=.65, alpha=.85))
    ax.add_collection(LineCollection([lines[i] for i in ids], colors="#727f89", linewidths=.9))
    ax.add_collection(LineCollection([lines[i] for i in active], array=positive, cmap=cmap, norm=norm, linewidths=2.2))
    ax.set_xlim(extent[0], extent[2]); ax.set_ylim(extent[1], extent[3])
    ax.set_aspect("equal"); ax.set_xticks([]); ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    text(fig, .5, .941, "New York City", 74, font["Bold"], ha="center")
    tracked(fig, .5, .914, "TRAFFIC COLLISIONS", 21, 3.8)
    for x, y, width, height, stroke in [(.028, .023, .944, .962, .7), (.032, .026, .936, .956, .3)]:
        fig.add_artist(Rectangle((x, y), width, height, transform=fig.transFigure, fill=False, edgecolor="#46545d", linewidth=stroke, zorder=9))
    text(fig, .945, .036, footer, 12.5, font["Regular"], ha="right")
    x, y, width, height = .700, .083, .250, .057
    fig.add_artist(Rectangle((x, y), width, height, transform=fig.transFigure, facecolor="#fffefa", edgecolor="#aeb7bc", linewidth=.55, zorder=10))
    text(fig, x + .013, y + .036, "Collision Density", 17, font["Bold"])
    cax = fig.add_axes([x + .013, y + .019, width - .026, .010], zorder=12)
    cb = fig.colorbar(ScalarMappable(norm=norm, cmap=cmap), cax=cax, orientation="horizontal")
    cb.set_ticks([]); cb.outline.set_visible(False)
    text(fig, x + .013, y + .007, "Low", 11, font["Regular"], color=MUTED)
    text(fig, x + width - .013, y + .007, "High", 11, font["Regular"], color=MUTED, ha="right")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(".render.png")
    fig.savefig(temporary, dpi=300)
    plt.close(fig)
    with Image.open(temporary) as image:
        rgb = image.convert("RGB")
        pnginfo = PngImagePlugin.PngInfo()
        pnginfo.add_text("Title", "New York City · Traffic Collisions")
        window = metadata["observation_window"] if args.year is None else f"{args.year}: Jan 1–Jun 11" if args.matched_period or args.year == 2026 else f"{args.year}: complete calendar year"
        pnginfo.add_text("Description", f"Corrected {args.kernel} network intensity; {args.bandwidth:g} m bandwidth; {args.scale} color scale; Manhattan events; Queens contextual; actual observation window: {window}; {footer}.")
        rgb.save(args.output, dpi=(300, 300), icc_profile=ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes(), pnginfo=pnginfo)
    temporary.unlink()
    receipt = {
        "file": args.output.name, "dimensions_px": [4800, 7800], "ppi": 300,
        "poster_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "input_sha256": metadata["sha256"], "graph_sha256": metadata["graph_sha256"],
        "corrected_reference_max_error": error, "kernel": args.kernel,
        "bandwidth_m": args.bandwidth, "scale": args.scale, "scale_ceiling": float(norm.vmax),
        "framing": args.framing, "extent_m": extent.tolist(), "year": args.year,
        "matched_period": args.matched_period, "accepted_crashes": int(counts.sum()),
        "positive_lixels_in_view": len(active), "footer": footer,
        "observation_window": window,
    }
    args.output.with_suffix(".json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
