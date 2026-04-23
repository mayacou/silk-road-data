"""
export_routes_json.py
Converts silk_road_2023_with_emissions.csv into a routes.json file.
Updated to ensure Mode Diversity (Air, Road, Rail, Ocean) and 
clean display names for the dashboard.
"""

import json
import os
import zipfile
import pandas as pd
from collections import Counter

BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DIR      = os.path.join(BASE_DIR, "data", "clean")
CENTROIDS_FILE = os.path.join(BASE_DIR, "data", "country_centroids.json")
OUTPUT_FILE    = os.path.join(BASE_DIR, "routes.json")

# Increasing diversity: We take the top 30 routes for EVERY combination 
# of Year + Fabric + Mode.
TOP_N_PER_GROUP = 30  

# ── Display Name Overrides ──────────────────────────────────────────────────
DISPLAY_NAMES = {
    "Viet Nam": "Vietnam",
    "T\u00fcrkiye": "Turkey",
    "Türkiye": "Turkey",
    "Rep. of Korea": "South Korea",
    "Russian Federation": "Russia",
    "United Kingdom": "UK",
    "United States of America": "USA"
}

# ── Fabric mapping ────────────────────────────────────────────────────────────
SYNTHETIC_CODES = {
    "540110", "5402", "5404", "5407",
    "5501", "5503", "550510", "5506",
    "550810", "5509", "551110", "551120",
    "5512", "5513", "5514", "5515",
}

ARTIFICIAL_CODES = {
    "540120", "5403", "5405", "5408",
    "5502", "5504", "550520", "5507",
    "550820", "5510", "551130", "5516",
}

LINEN_CHAPTER_CODES = {"5301", "5302", "5303", "5305", "5306", "5307",
                       "5309", "5310", "530820"}

def classify_fabric(cmd_code: str) -> str | None:
    code = str(cmd_code).strip()
    chapter = code[:2]
    if chapter == "50": return "silk"
    if chapter == "51": return "wool"
    if chapter == "52": return "cotton"
    if code in LINEN_CHAPTER_CODES or chapter == "53": return "linen"
    if code in SYNTHETIC_CODES: return "polyester"
    if code in ARTIFICIAL_CODES: return "viscose"
    return None

def load_centroids() -> dict:
    print(f"Loading country centroids from {CENTROIDS_FILE}...")
    with open(CENTROIDS_FILE) as f:
        return json.load(f)

def load_all_years() -> pd.DataFrame:
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
        print("No emissions files found. Run build_pipeline first.")
        return

    # Classify fabric and drop unclassified
    df["fabric"] = df["cmdCode"].astype(str).apply(classify_fabric)
    df = df.dropna(subset=["fabric"])

    # Drop rows missing key fields or with zero weight
    df = df.dropna(subset=["reporterISO", "partnerISO", "Final_Mode",
                            "netWgt", "total_emissions_kgco2e", "distance_km"])
    df = df[df["netWgt"] > 0]

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
            distance_km=("distance_km", "mean"),
        )
    )

    agg["volume_tonnes"] = (agg["volume_kg"] / 1000).round(1)

    # =========================================================================
    # CRITICAL CHANGE: Group by Final_Mode to ensure Rail/Ocean/Road all appear
    # =========================================================================
    top = (
        agg.sort_values("total_emissions_kgco2e", ascending=False)
           .groupby(["year", "fabric", "Final_Mode"]) # <-- Added Final_Mode here
           .head(TOP_N_PER_GROUP)
           .reset_index(drop=True)
    )

    print(f"\nDiversity Filter: Top {TOP_N_PER_GROUP} per (Fabric + Mode) -> {len(top):,} routes")

    centroids = load_centroids()
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

        # Use clean display names for the UI
        clean_from = DISPLAY_NAMES.get(row["reporterDesc"], row["reporterDesc"])
        clean_to   = DISPLAY_NAMES.get(row["partnerDesc"], row["partnerDesc"])

        routes.append({
            "id":               len(routes) + 1,
            "year":             int(row["year"]),
            "fabric":           row["fabric"],
            "from":             clean_from,
            "to":               clean_to,
            "fromISO":          orig,
            "toISO":            dest,
            "fromCoords":       [round(o["lon"], 2), round(o["lat"], 2)],
            "toCoords":         [round(d["lon"], 2), round(d["lat"], 2)],
            "mode":             row["Final_Mode"],
            "volume_tonnes":    row["volume_tonnes"],
            "distance_km":      round(row["distance_km"], 1),
            "total_emissions_kgco2e": round(row["total_emissions_kgco2e"], 1),
        })

    with open(OUTPUT_FILE, "w") as f:
        json.dump(routes, f, separators=(",", ":"))

    print(f"Routes built: {len(routes):,}  (skipped {skipped} missing centroid)")
    
    # Final Audit
    counts = Counter(r["mode"] for r in routes)
    print("\nMode distribution in final routes.json:")
    for mode, n in sorted(counts.items()):
        print(f"  {mode:<12} {n} routes")

if __name__ == "__main__":
    main()