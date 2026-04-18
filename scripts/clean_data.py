import pandas as pd
import geopandas as gpd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Local Natural Earth shapefile — avoids any network dependency.
# Generated once; bundled in data/ so teammates don't need to download it.
MAP_FILE   = os.path.join(BASE_DIR, "data", "ne_110m_countries.zip")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(OUTPUT_DIR, exist_ok=True)

KEEPER_COLUMNS = [
    'refYear', 'reporterISO', 'reporterDesc', 'partnerISO', 'partnerDesc',
    'cmdCode', 'cmdDesc', 'motCode', 'motDesc', 'netWgt', 'fobvalue'
]

MOT_MAPPING = {
    1000: 'Air', 9200: 'Air',
    3200: 'Road', 3000: 'Road', 3900: 'Road',
    3100: 'Rail',
    2100: 'Ocean', 2000: 'Ocean', 2200: 'Ocean', 2900: 'Ocean'
}


def load_map():
    print(f"Loading world map from {MAP_FILE}...")
    gdf = gpd.read_file(MAP_FILE)
    gdf.columns = [c.lower() for c in gdf.columns]

    def get_best_iso(row):
        for col in ['iso_a3', 'adm0_a3', 'iso_a3_eh', 'gu_a3']:
            if col in row and pd.notnull(row[col]) and str(row[col]).strip() not in ['', '-99']:
                return str(row[col]).strip()
        return None

    gdf['final_iso'] = gdf.apply(get_best_iso, axis=1)
    gdf = gdf.dropna(subset=['final_iso'])

    cont_col = 'continent' if 'continent' in gdf.columns else 'continent_name'
    countries = gdf[['final_iso', cont_col, 'geometry']].set_index('final_iso')
    print(f"Map indexed — {len(countries)} geographic entities recognised.")
    return countries, cont_col


def run_inference(row, countries, cont_col):
    current_mode = row['Calculated_Mode']
    if current_mode != 'Unknown':
        return current_mode, False, "Reported"

    iso_o = row['reporterISO']
    iso_d = row['partnerISO']

    if iso_o not in countries.index or iso_d not in countries.index:
        return "Ocean", True, "Map Default"

    origin = countries.loc[iso_o]
    dest   = countries.loc[iso_d]

    if (row['fobvalue'] / row['netWgt']) > 50:
        return "Air", True, "Value Density"

    if origin.geometry.touches(dest.geometry) or origin.geometry.distance(dest.geometry) < 0.01:
        return "Road", True, "Shared Border"

    if origin[cont_col] != dest[cont_col]:
        return "Ocean", True, "Intercontinental"

    return "Ocean", True, "Intra-continental"


def clean_year(year: str, countries, cont_col):
    raw_file    = os.path.join(BASE_DIR, "data", "raw", year, "_combined_raw.csv")
    output_file = os.path.join(OUTPUT_DIR, f"silk_road_{year}_refined.csv")

    if not os.path.exists(raw_file):
        print(f"  Skipping {year} — raw file not found: {raw_file}")
        return

    print(f"\n── Cleaning {year} ──────────────────────────────────────")
    df = pd.read_csv(raw_file, low_memory=False)

    # Keep only useful columns (ignore missing ones gracefully)
    cols = [c for c in KEEPER_COLUMNS if c in df.columns]
    df = df[cols].copy()

    df = df[df['partnerISO'] != 'W00']
    df['netWgt'] = df['netWgt'].fillna(0)
    df = df[df['netWgt'] > 0]

    df['Calculated_Mode'] = df['motCode'].map(MOT_MAPPING).fillna('Unknown')

    print("  Running Geographic Inference Engine...")
    results = df.apply(lambda row: run_inference(row, countries, cont_col), axis=1)
    df['Final_Mode']       = [r[0] for r in results]
    df['is_inferred']      = [r[1] for r in results]
    df['inference_reason'] = [r[2] for r in results]
    df = df.drop(columns=['Calculated_Mode'])

    df.to_csv(output_file, index=False)
    print(f"  {year} done: {len(df):,} rows → {output_file}")
    print(f"  Mode breakdown:\n{df['Final_Mode'].value_counts().to_string()}")


def main():
    countries, cont_col = load_map()

    raw_base = os.path.join(BASE_DIR, "data", "raw")
    years = sorted([
        d for d in os.listdir(raw_base)
        if os.path.isdir(os.path.join(raw_base, d)) and d.isdigit()
    ])

    if not years:
        print(f"No year folders found under {raw_base}.")
        return

    print(f"Years to process: {years}")
    for year in years:
        clean_year(year, countries, cont_col)

    print("\n" + "=" * 50)
    print("ALL YEARS CLEANED")
    print("=" * 50)


if __name__ == "__main__":
    main()
