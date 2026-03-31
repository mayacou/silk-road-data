import pandas as pd
import geopandas as gpd
import geodatasets
import os

# --- CONFIGURATION ---
RAW_FILE = "data/raw/2023/_combined_raw.csv"
OUTPUT_DIR = "data/clean"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "silk_road_2023_refined.csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Essential Columns
KEEPER_COLUMNS = [
    'refYear', 'reporterISO', 'reporterDesc', 'partnerISO', 'partnerDesc',
    'cmdCode', 'cmdDesc', 'motCode', 'motDesc', 'netWgt', 'fobvalue'
]

# UN Mode Mapping
MOT_MAPPING = {
    1000: 'Air', 9200: 'Air',
    3200: 'Road', 3000: 'Road', 3900: 'Road',
    3100: 'Rail',
    2100: 'Ocean', 2000: 'Ocean', 2200: 'Ocean', 2900: 'Ocean'
}


# --- LOAD GEOGRAPHIC REFERENCE ---
print("Loading world map and standardizing ISO codes...")


def load_map():
    for key in ["naturalearth.lowres", "naturalearth.countries"]:
        try:
            path = geodatasets.get_path(key)
            return gpd.read_file(path)
        except:
            continue
    print("Downloading map directly from Natural Earth...")
    url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    return gpd.read_file(url)

countries_gdf = load_map()
countries_gdf.columns = [c.lower() for c in countries_gdf.columns]

# --- ROBUST ISO INDEXING ---
def get_best_iso(row):
    # Order of reliability for UN Comtrade matching
    for col in ['iso_a3', 'adm0_a3', 'iso_a3_eh', 'gu_a3']:
        if col in row and pd.notnull(row[col]) and str(row[col]).strip() not in ['', '-99']:
            return str(row[col]).strip()
    return None

countries_gdf['final_iso'] = countries_gdf.apply(get_best_iso, axis=1)
countries_gdf = countries_gdf.dropna(subset=['final_iso'])

# Identify Continent column
cont_col = 'continent' if 'continent' in countries_gdf.columns else 'continent_name'

# Create the final reference dictionary indexed by the standardized ISO
countries = countries_gdf[['final_iso', cont_col, 'geometry']].set_index('final_iso')
print(f"Map indexed. Recognized {len(countries)} geographic entities.")

def run_inference(row):
    """
    Assigns transport modes and returns a tuple:
    (Final_Mode, is_inferred, inference_reason)
    """
    current_mode = row['Calculated_Mode']
    
    # If already reported, no inference needed
    if current_mode != 'Unknown':
        return current_mode, False, "Reported"

    iso_o = row['reporterISO']
    iso_d = row['partnerISO']

    # 1. Map Default Rule (Missing from Geometry Index)
    if iso_o not in countries.index or iso_d not in countries.index:
        return "Ocean", True, "Map Default"

    origin = countries.loc[iso_o]
    dest = countries.loc[iso_d]

    # 2. Economic Logic Rule (High Value Density)
    if (row['fobvalue'] / row['netWgt']) > 50:
        return "Air", True, "Value Density"

    # 3. Shared Land Border Rule (Geography)
    # Using a small distance threshold to account for map vector precision
    if origin.geometry.touches(dest.geometry) or origin.geometry.distance(dest.geometry) < 0.01:
        return "Road", True, "Shared Border"

    # 4. Different Continents Rule (Geography)
    if origin[cont_col] != dest[cont_col]:
        return "Ocean", True, "Intercontinental"

    # 5. Same Continent Default
    return "Ocean", True, "Intra-continental"

def main():
    if not os.path.exists(RAW_FILE):
        print(f"Error:Cannot find {RAW_FILE}.")
        return

    print(f"Reading {RAW_FILE}...")
    df = pd.read_csv(RAW_FILE, low_memory=False)
    
    # --- PRE-REFINEMENT FILTERS ---
    df = df[KEEPER_COLUMNS].copy()
    df = df[df['partnerISO'] != 'W00'] # Remove World aggregates
    df['netWgt'] = df['netWgt'].fillna(0)
    df = df[df['netWgt'] > 0] # Remove zero-weight ghost rows

    print("Mapping known transport modes...")
    df['Calculated_Mode'] = df['motCode'].map(MOT_MAPPING).fillna('Unknown')

    print("Running Geographic Inference Engine...")
    inference_results = df.apply(run_inference, axis=1)
    
    # Unpack the three values into new columns
    df['Final_Mode'] = [res[0] for res in inference_results]
    df['is_inferred'] = [res[1] for res in inference_results]
    df['inference_reason'] = [res[2] for res in inference_results]

    # Cleanup temporary column
    df = df.drop(columns=['Calculated_Mode'])

    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "="*50)
    print("✨ DATA REFINEMENT COMPLETE (World Aggregates Removed)")
    print("="*50)
    print(f"Final Row Count: {len(df):,}")
    print("\nFinal Mode Breakdown:")
    print(df['Final_Mode'].value_counts())
    print(f"\nSaved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()