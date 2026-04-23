import pandas as pd
import geopandas as gpd
import numpy as np
import os
import json
import pickle
from math import radians, cos, sin, asin, sqrt

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


def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    return 6371 * c


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
    print(f"Map indexed: {len(countries)} geographic entities recognised.")
    return countries, cont_col


def clean_year(year: str, countries, cont_col, centroids, model, label_encoder):
    raw_file    = os.path.join(BASE_DIR, "data", "raw", year, "_combined_raw.csv")
    output_file = os.path.join(OUTPUT_DIR, f"silk_road_{year}_refined.csv")

    if not os.path.exists(raw_file):
        print(f"  Skipping {year}: raw file not found at {raw_file}")
        return

    print(f"\n-- Cleaning {year} ------------------------------------------")
    df = pd.read_csv(raw_file, low_memory=False)

    # Keep only useful columns (ignore missing ones gracefully)
    cols = [c for c in KEEPER_COLUMNS if c in df.columns]
    df = df[cols].copy()

    df = df[df['partnerISO'] != 'W00']
    df['netWgt'] = df['netWgt'].fillna(0)
    df = df[df['netWgt'] > 0]

    df['Calculated_Mode'] = df['motCode'].map(MOT_MAPPING).fillna('Unknown')

    print("  Running ML Prediction Engine...")

    # Identify unknown rows
    unknown_mask = df['Calculated_Mode'] == 'Unknown'

    # Calculate distance_km for unknown rows using centroids
    df.loc[unknown_mask, 'reporter_coords'] = df.loc[unknown_mask, 'reporterISO'].map(centroids)
    df.loc[unknown_mask, 'partner_coords'] = df.loc[unknown_mask, 'partnerISO'].map(centroids)

    def calc_distance(row):
        if pd.isna(row['reporter_coords']) or pd.isna(row['partner_coords']):
            return np.nan
        r = row['reporter_coords']
        p = row['partner_coords']
        return haversine(r['lon'], r['lat'], p['lon'], p['lat'])

    df.loc[unknown_mask, 'distance_km'] = df.loc[unknown_mask].apply(calc_distance, axis=1)

    # Calculate value_density and is_high_value for all rows
    df['value_density'] = df['fobvalue'] / df['netWgt']
    df['is_high_value'] = (df['value_density'] > 50).astype(int)

    # Predict for unknown rows
    features = df.loc[unknown_mask, ['netWgt', 'fobvalue', 'distance_km', 'value_density', 'is_high_value']].fillna(0)
    pred_encoded = model.predict(features)
    pred = label_encoder.inverse_transform(pred_encoded)
    df.loc[unknown_mask, 'Final_Mode'] = pred
    df.loc[unknown_mask, 'is_inferred'] = True
    df.loc[unknown_mask, 'inference_reason'] = "XGBoost ML Prediction"

    # For known rows
    known_mask = ~unknown_mask
    df.loc[known_mask, 'Final_Mode'] = df.loc[known_mask, 'Calculated_Mode']
    df.loc[known_mask, 'is_inferred'] = False
    df.loc[known_mask, 'inference_reason'] = "Reported"

    # ── Geographic Override ────────────────────────────────────────────────
    # Rail and Road are physically impossible across oceans. Override any
    # intercontinental Rail/Road assignment to Ocean, with an exception for
    # the Eurasian land bridge (Europe <-> Asia can use Rail/Road).
    print("  Applying geographic mode overrides...")

    iso_to_continent = countries[cont_col].to_dict()
    df['_orig_cont'] = df['reporterISO'].map(iso_to_continent)
    df['_dest_cont'] = df['partnerISO'].map(iso_to_continent)

    intercontinental = (
        df['_orig_cont'].notna() &
        df['_dest_cont'].notna() &
        (df['_orig_cont'] != df['_dest_cont'])
    )
    eurasian_bridge = (
        df['_orig_cont'].isin(['Europe', 'Asia']) &
        df['_dest_cont'].isin(['Europe', 'Asia'])
    )
    must_be_ocean = intercontinental & ~eurasian_bridge & df['Final_Mode'].isin(['Rail', 'Road'])

    n_overridden = must_be_ocean.sum()
    if n_overridden:
        df.loc[must_be_ocean, 'Final_Mode'] = 'Ocean'
        df.loc[must_be_ocean, 'inference_reason'] = 'Geographic Override (intercontinental)'
        print(f"  Overridden {n_overridden:,} impossible intercontinental Rail/Road rows -> Ocean")
    else:
        print("  No impossible intercontinental Rail/Road rows found.")

    df = df.drop(columns=['_orig_cont', '_dest_cont'])

    # Drop temporary columns
    df = df.drop(columns=['Calculated_Mode', 'reporter_coords', 'partner_coords'])

    df.to_csv(output_file, index=False)
    print(f"  {year} done: {len(df):,} rows -> {output_file}")
    print(f"  Mode breakdown:\n{df['Final_Mode'].value_counts().to_string()}")


def main():
    # Load ML assets
    model_path = os.path.join(BASE_DIR, 'models', 'baseline_model.pkl')
    encoder_path = os.path.join(BASE_DIR, 'models', 'label_encoder.pkl')
    centroids_path = os.path.join(BASE_DIR, 'data', 'country_centroids.json')

    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    print("Loaded XGBoost model")

    with open(encoder_path, 'rb') as f:
        label_encoder = pickle.load(f)
    print("Loaded LabelEncoder")

    with open(centroids_path, 'r') as f:
        centroids = json.load(f)
    print("Loaded country centroids")

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
        clean_year(year, countries, cont_col, centroids, model, label_encoder)

    print("\n" + "=" * 50)
    print("ALL YEARS CLEANED")
    print("=" * 50)


if __name__ == "__main__":
    main()