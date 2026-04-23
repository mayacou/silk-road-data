import pandas as pd
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH = os.path.join(_ROOT, "data", "raw", "2023", "_combined_raw.csv")

# Metadata for Audit
MOT_MAPPING = {
    0: "TOTAL (Aggregated)",
    1000: "Air",
    2000: "Water",
    2100: "Sea",
    2200: "Inland waterway",
    2900: "Water (Other)",
    3000: "Land",
    3100: "Railway",
    3200: "Road",
    3900: "Land (Other)",
    9000: "Not elsewhere classified",
    9200: "Postal/Courier",
    9900: "Other"
}

# Mapping HS prefixes to human-readable names for audit
FABRIC_MAP = {
    "50": "Silk",
    "51": "Wool",
    "52": "Cotton",
    "53": "Vegetable Fibers (Linen/Hemp)",
    "54": "Synthetic Filaments",
    "55": "Man-made Staple Fibers"
}

def get_fabric_type(code):
    code_str = str(code)
    prefix = code_str[:2]
    return FABRIC_MAP.get(prefix, f"Other ({prefix})")

def run_full_audit():
    if not os.path.exists(FILE_PATH):
        print(f"ERROR: Could not find {FILE_PATH}")
        return

    print(f"Starting Full Data Audit on: {FILE_PATH}")
    df = pd.read_csv(FILE_PATH, low_memory=False)
    total_rows = len(df)

    if total_rows == 0:
        print("ERROR: Dataset is empty.")
        return

    # 1. TEMPORAL
    print("\n" + "="*30)
    print("1. TEMPORAL")
    print("="*30)
    year_counts = df['refYear'].value_counts().sort_index()
    for yr, count in year_counts.items():
        print(f"Year {yr}: {count:>10,} rows ({ (count/total_rows)*100:>5.1f}%)")

    # 2. FABRIC DIVERSITY
    print("\n" + "="*30)
    print("2. FABRIC DIVERSITY")
    print("="*30)
    df['fabric_audit'] = df['cmdCode'].apply(get_fabric_type)
    fab_counts = df['fabric_audit'].value_counts()
    for fab, count in fab_counts.items():
        pct = (count/total_rows)*100
        status = "OK" if count > 0 else "MISSING"
        print(f"[{status}] {fab:<30} {count:>10,} rows ({pct:>5.1f}%)")

    # 3. TRANSPORT MODE (MOT)
    print("\n" + "="*30)
    print("3. TRANSPORT MODE (MOT)")
    print("="*30)
    mot_counts = df['motCode'].value_counts(dropna=False)

    # Check for the "0" code (Total) which can ruin analysis if not handled
    total_agg_count = mot_counts.get(0, 0)
    if total_agg_count > 0:
        print(f"NOTE: {total_agg_count:,} rows use code '0' (Total). These are usually aggregates.")

    for code, count in mot_counts.items():
        desc = MOT_MAPPING.get(code, "Unknown/Unmapped")
        if pd.isna(code): desc = "MISSING/NULL"
        pct = (count/total_rows)*100
        print(f"{code:>5} | {desc:<25} | {count:>10,} rows ({pct:>5.1f}%)")

    # 4. GEOGRAPHIC COVERAGE
    print("\n" + "="*30)
    print("4. GEOGRAPHIC COVERAGE")
    print("="*30)
    reporters = df['reporterISO'].nunique()
    partners = df['partnerISO'].nunique()
    print(f"Exporting Countries (Reporters): {reporters}")
    print(f"Importing Countries (Partners):  {partners}")

    # 5. DATA QUALITY & NULLS
    print("\n" + "="*30)
    print("5. DATA QUALITY & NULLS")
    print("="*30)
    critical_cols = ['netWgt', 'fobvalue', 'motCode', 'qty']
    for col in critical_cols:
        if col in df.columns:
            nulls = df[col].isnull().sum()
            zeros = (df[col] == 0).sum()
            null_pct = (nulls/total_rows)*100
            zero_pct = (zeros/total_rows)*100

            print(f"Column: {col:<15}")
            null_flag = "OK" if null_pct < 5 else "WARNING"
            zero_flag = "INFO" if zeros > 0 else "OK"
            print(f"  - Nulls: {nulls:>10,} ({null_pct:>5.1f}%) [{null_flag}]")
            print(f"  - Zeros: {zeros:>10,} ({zero_pct:>5.1f}%) [{zero_flag}]")
        else:
            print(f"MISSING: Column {col} not found in file.")

    # 6. VOLUME CHECK
    print("\n" + "="*30)
    print("6. TRADE VOLUME SUMMARY")
    print("="*30)
    if 'netWgt' in df.columns:
        total_kg = df['netWgt'].sum()
        print(f"Total Weight: {total_kg/1e6:>15.2f} Million KG")
    if 'fobvalue' in df.columns:
        total_val = df['fobvalue'].sum()
        print(f"Total Value:  ${total_val/1e9:>14.2f} Billion USD")

    print("\n" + "="*85)
    print("AUDIT COMPLETE")
    print("="*85)

if __name__ == "__main__":
    run_full_audit()
