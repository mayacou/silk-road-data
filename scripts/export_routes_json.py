"""
export_routes_json.py
Converts silk_road_2023_with_emissions.csv into a routes.json file
that silk_road_map.html can fetch and render directly.

NOTE: Cotton (ch.52) and Silk (ch.50) are absent from the processed data.
Both chapters were collected with broad chapter-level codes ("50", "52") which
the Comtrade API returns as weight=0 aggregate rows — these were correctly
dropped by clean_data.py. This is flagged as a project limitation.

Available fabrics: wool, linen (vegetable fibers), polyester (synthetic),
viscose (artificial).
"""

import json
import os
import zipfile

import pandas as pd

BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DIR      = os.path.join(BASE_DIR, "data", "clean")
CENTROIDS_FILE = os.path.join(BASE_DIR, "data", "country_centroids.json")
OUTPUT_FILE    = os.path.join(BASE_DIR, "routes.json")

TOP_N_PER_FABRIC = 75  # routes per (year, fabric) combination, ranked by total emissions

# ── Fabric mapping ────────────────────────────────────────────────────────────
# Maps the first 2–6 digits of cmdCode to a visualization fabric category.

SYNTHETIC_CODES = {
    "540110", "5402", "5404", "5407",   # filament
    "5501", "5503", "550510", "5506",   # staple tow / raw
    "550810", "5509", "551110", "551120",  # yarn
    "5512", "5513", "5514", "5515",     # woven
}

ARTIFICIAL_CODES = {
    "540120", "5403", "5405", "5408",   # filament
    "5502", "5504", "550520", "5507",   # staple tow / raw
    "550820", "5510", "551130", "5516", # yarn / woven
}

LINEN_CHAPTER_CODES = {"5301", "5302", "5303", "5305", "5306", "5307",
                       "5309", "5310", "530820"}


def classify_fabric(cmd_code: str) -> str | None:
    code = str(cmd_code).strip()
    chapter = code[:2]
    if chapter == "51":
        return "wool"
    if code in LINEN_CHAPTER_CODES or chapter == "53":
        return "linen"
    if code in SYNTHETIC_CODES:
        return "polyester"
    if code in ARTIFICIAL_CODES:
        return "viscose"
    return None


# ── Country centroid lookup ───────────────────────────────────────────────────

def load_centroids() -> dict:
    print(f"Loading country centroids from {CENTROIDS_FILE}...")
    with open(CENTROIDS_FILE) as f:
        return json.load(f)


# ── Main ─────────────────────────────────────────────────────────────────────

def load_all_years() -> pd.DataFrame:
    """Read every silk_road_YYYY_with_emissions.csv (or .zip) in CLEAN_DIR."""
    frames = []
    for fname in sorted(os.listdir(CLEAN_DIR)):
        if not fname.startswith("silk_road_") or "with_emissions" not in fname:
            continue
        fpath = os.path.join(CLEAN_DIR, fname)
        year  = fname.split("_")[2]
        if fname.endswith(".zip"):
            with zipfile.ZipFile(fpath) as z:
                csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
                with z.open(csv_name) as f:
                    chunk = pd.read_csv(f, low_memory=False)
        else:
            chunk = pd.read_csv(fpath, low_memory=False)
        chunk["year"] = int(year)
        frames.append(chunk)
        print(f"  Loaded {year}: {len(chunk):,} rows")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main():
    print(f"Scanning {CLEAN_DIR} for emissions files...")
    df = load_all_years()

    if df.empty:
        print("No emissions files found. Run add_distance_and_emissions.py first.")
        return

    print(f"  Total rows across all years: {len(df):,}")

    # Classify fabric
    df["fabric"] = df["cmdCode"].astype(str).apply(classify_fabric)
    unclassified = df["fabric"].isna().sum()
    print(f"  Unclassified rows dropped: {unclassified:,}")
    df = df.dropna(subset=["fabric"])

    # Drop rows missing key fields
    df = df.dropna(subset=["reporterISO", "partnerISO", "Final_Mode",
                            "netWgt", "total_emissions_kgco2e", "distance_km"])
    df = df[df["netWgt"] > 0]

    print(f"  Rows after filtering: {len(df):,}")
    print(f"  Fabric breakdown:\n{df['fabric'].value_counts().to_string()}")

    # Aggregate: one row per (year, reporter, partner, fabric, mode)
    agg = (
        df.groupby(
            ["year", "reporterISO", "reporterDesc", "partnerISO", "partnerDesc",
             "fabric", "Final_Mode"],
            as_index=False
        )
        .agg(
            volume_kg=("netWgt", "sum"),
            total_emissions_kgco2e=("total_emissions_kgco2e", "sum"),
            distance_km=("distance_km", "mean"),  # same route → same distance
        )
    )

    agg["volume_tonnes"] = (agg["volume_kg"] / 1000).round(1)

    # Select top N per (year, fabric) by total emissions
    top = (
        agg.sort_values("total_emissions_kgco2e", ascending=False)
           .groupby(["year", "fabric"])
           .head(TOP_N_PER_FABRIC)
           .reset_index(drop=True)
    )

    print(f"\nTop {TOP_N_PER_FABRIC} per fabric → {len(top):,} routes total")

    # Load centroids
    centroids = load_centroids()

    # Build ROUTES list
    routes = []
    skipped = 0
    for _, row in top.iterrows():
        orig = row["reporterISO"]
        dest = row["partnerISO"]

        if orig not in centroids or dest not in centroids:
            skipped += 1
            continue

        o = centroids[orig]
        d = centroids[dest]

        routes.append({
            "id":               len(routes) + 1,
            "year":             int(row["year"]),
            "fabric":           row["fabric"],
            "from":             row["reporterDesc"],
            "to":               row["partnerDesc"],
            "fromISO":          orig,
            "toISO":            dest,
            "fromCoords":       [round(o["lon"], 2), round(o["lat"], 2)],
            "toCoords":         [round(d["lon"], 2), round(d["lat"], 2)],
            "mode":             row["Final_Mode"],
            "volume_tonnes":    row["volume_tonnes"],
            "distance_km":      round(row["distance_km"], 1),
            "total_emissions_kgco2e": round(row["total_emissions_kgco2e"], 1),
        })

    print(f"Routes built: {len(routes):,}  (skipped {skipped} missing centroid)")

    # Write JSON
    with open(OUTPUT_FILE, "w") as f:
        json.dump(routes, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\nSaved → {OUTPUT_FILE}  ({size_kb:.1f} KB)")
    from collections import Counter
    years_present = sorted(set(r["year"] for r in routes))
    print(f"\nYears in output: {years_present}")
    print("\nFabric distribution in output:")
    counts = Counter(r["fabric"] for r in routes)
    for fab, n in sorted(counts.items()):
        print(f"  {fab:<12} {n} routes")

    if not any(r["fabric"] in ("cotton", "silk") for r in routes):
        print("\nNOTE: Cotton and silk are absent — see module docstring for explanation.")


if __name__ == "__main__":
    main()
