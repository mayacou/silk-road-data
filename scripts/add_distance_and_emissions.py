import math
import os
import pandas as pd
import geopandas as gpd
import geodatasets

# =========================================================
# CONFIG
# =========================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_FILE = os.path.join(BASE_DIR, "data", "clean", "silk_road_2023_refined.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "clean")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "silk_road_2023_with_emissions.csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Project emission factors from README
# Units: kg CO2e / (kg * km)
EMISSION_FACTORS = {
    "Air": 0.00109,
    "Road": 0.000105,
    "Rail": 0.000022,
    "Ocean": 0.000015,
}

# =========================================================
# GEOGRAPHY HELPERS
# =========================================================

def load_map():
    """
    Uses the same Natural Earth loading strategy as clean_data.py.
    """
    for key in ["naturalearth.lowres", "naturalearth.countries"]:
        try:
            path = geodatasets.get_path(key)
            return gpd.read_file(path)
        except Exception:
            continue

    print("Downloading map directly from Natural Earth...")
    url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    return gpd.read_file(url)


def get_best_iso(row):
    """
    Same ISO-selection logic used by your teammate.
    """
    for col in ["iso_a3", "adm0_a3", "iso_a3_eh", "gu_a3"]:
        if col in row and pd.notnull(row[col]) and str(row[col]).strip() not in ["", "-99"]:
            return str(row[col]).strip()
    return None


def build_country_centroid_reference():
    """
    Builds ISO -> centroid lat/lon using the same Natural Earth map.
    """
    print("Loading world map and building centroid reference...")
    countries_gdf = load_map()
    countries_gdf.columns = [c.lower() for c in countries_gdf.columns]

    countries_gdf["final_iso"] = countries_gdf.apply(get_best_iso, axis=1)
    countries_gdf = countries_gdf.dropna(subset=["final_iso"]).copy()

    # Project before centroid calculation for better geometry behavior
    countries_proj = countries_gdf.to_crs(epsg=3857)
    centroids_proj = countries_proj.geometry.centroid
    centroids = gpd.GeoSeries(centroids_proj, crs="EPSG:3857").to_crs(epsg=4326)

    countries_gdf["centroid_lon"] = centroids.x
    countries_gdf["centroid_lat"] = centroids.y

    # Final lookup dictionary: ISO -> {"lat": ..., "lon": ...}
    country_ref = countries_gdf.set_index("final_iso")[["centroid_lat", "centroid_lon"]].to_dict("index")
    return country_ref


def haversine_km(lat1, lon1, lat2, lon2):
    """
    Great-circle distance between two lat/lon points in kilometers.
    """
    R = 6371.0

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def compute_distance_km(origin_iso, dest_iso, country_ref):
    """
    Returns centroid-to-centroid distance in km.
    """
    if origin_iso not in country_ref or dest_iso not in country_ref:
        return None

    origin = country_ref[origin_iso]
    dest = country_ref[dest_iso]

    return haversine_km(
        origin["centroid_lat"], origin["centroid_lon"],
        dest["centroid_lat"], dest["centroid_lon"]
    )


# =========================================================
# MAIN
# =========================================================

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Could not find {INPUT_FILE}")
        return

    print(f"Reading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    required_cols = ["reporterISO", "partnerISO", "Final_Mode", "netWgt"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}")
        return

    # Clean key columns
    df["reporterISO"] = df["reporterISO"].astype(str).str.strip().str.upper()
    df["partnerISO"] = df["partnerISO"].astype(str).str.strip().str.upper()
    df["Final_Mode"] = df["Final_Mode"].astype(str).str.strip()
    df["netWgt"] = pd.to_numeric(df["netWgt"], errors="coerce")

    # Build country centroid reference
    country_ref = build_country_centroid_reference()
    print(f"Built centroid reference for {len(country_ref):,} geographic entities.")

    # Compute distance
    print("Calculating distances...")
    df["distance_km"] = df.apply(
        lambda row: compute_distance_km(row["reporterISO"], row["partnerISO"], country_ref),
        axis=1
    )

    # Map emission factor
    print("Mapping emission factors...")
    df["emission_factor_kgco2e_per_kg_km"] = df["Final_Mode"].map(EMISSION_FACTORS)

    # Compute total emissions
    print("Calculating total emissions...")
    df["total_emissions_kgco2e"] = (
        df["netWgt"] *
        df["distance_km"] *
        df["emission_factor_kgco2e_per_kg_km"]
    )

    # Optional convenience metric
    df["emissions_per_kg"] = df["total_emissions_kgco2e"] / df["netWgt"]

    # Report missing values from the new calculations
    missing_distance = df["distance_km"].isna().sum()
    missing_factor = df["emission_factor_kgco2e_per_kg_km"].isna().sum()
    print(f"Rows missing distance: {missing_distance:,}")
    print(f"Rows missing emission factor: {missing_factor:,}")

    # Save NEW file
    df.to_csv(OUTPUT_FILE, index=False)

    print("\n" + "=" * 60)
    print("DISTANCE + EMISSIONS ENRICHMENT COMPLETE")
    print("=" * 60)
    print(f"Input rows:  {len(df):,}")
    print(f"Saved to:    {OUTPUT_FILE}")
    print("\nPreview:")
    print(df.head())


if __name__ == "__main__":
    main()