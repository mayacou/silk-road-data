import pandas as pd
import os

# Point this to your combined raw data file
FILE_PATH = "data/raw/2023/_combined_raw.csv"

MOT_DICT = {
    0: "TOTAL modes of transport",
    1000: "Air",
    2000: "Water",
    2100: "Sea",
    2200: "Inland waterway",
    2900: "Water, not else classified",
    3000: "Land",
    3100: "Railway",
    3200: "Road",
    3900: "Land, not else classified",
    9000: "Not elsewhere classified",
    9100: "Pipelines and cables",
    9110: "Pipelines",
    9120: "Cables",
    9190: "Pipelines and cables, not else classified",
    9200: "Postal consignments, mail or courier shipment",
    9300: "Self propelled goods",
    9900: "Other"
}

def analyze_transport_modes():
    if not os.path.exists(FILE_PATH):
        print(f"❌ Could not find {FILE_PATH}")
        return

    print("Loading data...\n")
    df = pd.read_csv(FILE_PATH, low_memory=False)
    total_rows = len(df)

    if 'motCode' not in df.columns:
        print("❌ Column 'motCode' not found in the dataset.")
        return

    # Count the occurrences of each transport code
    # We use dropna=False so we can also see if any are completely blank
    counts = df['motCode'].value_counts(dropna=False).to_dict()

    print("=" * 85)
    print(f" TRANSPORT MODE BREAKDOWN ({total_rows:,} Total Rows)")
    print("=" * 85)
    
    # Table Header
    print(f"{'Code':<6} | {'Description':<45} | {'Count (Rows)':<14} | {'Percent':<8}")
    print("-" * 85)

    total_counted = 0
    
    # Loop through the official dictionary to print the table in order
    for code, desc in MOT_DICT.items():
        # The API sometimes returns codes as floats or strings, so we check a few ways
        count = counts.get(code, 0)
        if count == 0 and str(code) in counts:
            count = counts.get(str(code), 0)
        if count == 0 and float(code) in counts:
            count = counts.get(float(code), 0)
             
        pct = (count / total_rows) * 100 if total_rows > 0 else 0
        total_counted += count
        
        # Only print if count > 0, or print all of them if you prefer? 
        # (Let's print all of them so you see the exact table you requested)
        print(f"{code:<6} | {desc:<45} | {count:>14,} | {pct:>6.2f}%")

    # Catch any weird undocumented codes the UN might have snuck in, or actual Nulls
    unexpected_codes = set(counts.keys()) - set(MOT_DICT.keys()) - set(str(k) for k in MOT_DICT.keys()) - set(float(k) for k in MOT_DICT.keys())
    for code in unexpected_codes:
        count = counts[code]
        pct = (count / total_rows) * 100
        total_counted += count
        label = "[BLANK / NULL]" if pd.isna(code) else "[UNKNOWN UN CODE]"
        print(f"{str(code):<6} | {label:<45} | {count:>14,} | {pct:>6.2f}%")

    # Table Footer (Total)
    print("-" * 85)
    total_pct = (total_counted / total_rows) * 100 if total_rows > 0 else 0
    print(f"{'TOTAL':<6} | {'All Rows Checked':<45} | {total_counted:>14,} | {total_pct:>6.2f}%")
    print("=" * 85)

def check_nulls():
    if not os.path.exists(FILE_PATH):
        print(f"Error: Could not find {FILE_PATH}.")
        return

    print(f"Loading {FILE_PATH}...\n")
    df = pd.read_csv(FILE_PATH, low_memory=False)
    
    total_rows = len(df)
    
    print("=" * 60)
    print(f" NULL VALUES REPORT ({total_rows:,} Total Rows)")
    print("=" * 60)

    # Calculate nulls and percentages
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = (null_count / total_rows) * 100
        
        # Formatting: Highlight columns that actually have missing data
        if null_count > 0:
            print(f" ⚠️ {col:<25} {null_count:>8,} nulls ({null_pct:>5.1f}%)")
        else:
            print(f" ✅ {col:<25} {null_count:>8,} nulls ({null_pct:>5.1f}%)")

    print("=" * 60)

if __name__ == "__main__":
    check_nulls()
    analyze_transport_modes()