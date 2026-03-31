# =============================================================================
# Silk Road — Data Collection Script
# Year: 2023 | UN Comtrade Free Tier
# Captures: origin, destination, weight, mode of transport, HS code
# =============================================================================

import comtradeapicall
import pandas as pd
import os
import time
from dotenv import load_dotenv

# --- Load API key ---
load_dotenv()
API_KEY = os.getenv("COMTRADE_API_KEY")
if not API_KEY:
    raise ValueError("Add COMTRADE_API_KEY=your_key to your .env file.")

# --- Config ---
YEAR       = "2023"
BATCH_SIZE = 10
RAW_DIR    = "data/raw/2023"
os.makedirs(RAW_DIR, exist_ok=True)

# =============================================================================
# ALL HS CODES TO COLLECT
# =============================================================================

ALL_CODES = [
    # --- Silk (Chapter 50) ---
    "50",

    # --- Wool & fine animal hair (Chapter 51, specific codes only) ---
    "5101",
    "510211", "510219",
    "510310", "510320",
    "510510", "510521", "510531", "510539",
    "5106", "5107", "5108", "5109",
    "5111", "5115",

    # --- Cotton (Chapter 52) ---
    "52",

    # --- Natural / vegetable fibers ---
    "5301",   # Flax, raw or processed
    "5302",   # Hemp, raw or processed
    "5303",   # Jute & other textile bast fibers
    "5305",   # Coconut, abaca, sisal & other veg fibers
    "5306",   # Flax yarn
    "5307",   # Jute yarn
    "5309",   # Woven flax fabrics
    "5310",   # Woven jute fabrics
    "530820", # Hemp yarn

    # --- Synthetic filament (sewing thread, yarn, monofilament, woven) ---
    "540110", # Sewing thread of synthetic filament
    "5402",   # Synthetic filament yarn
    "5404",   # Synthetic monofilament
    "5407",   # Woven fabrics of synthetic filament yarn

    # --- Artificial filament (sewing thread, yarn, monofilament, woven) ---
    "540120", # Sewing thread of artificial filament
    "5403",   # Artificial filament yarn
    "5405",   # Artificial monofilament
    "5408",   # Woven fabrics of artificial filament yarn

    # --- Synthetic staple fibers ---
    "5501",   # Synthetic filament tow
    "5503",   # Synthetic staple fibers, not carded/combed
    "550510", # Waste of synthetic fibers
    "5506",   # Synthetic staple fibers, carded/combed
    "550810", # Sewing thread of synthetic staple fibers
    "5509",   # Yarn of synthetic staple fibers
    "551110", # Yarn ≥85% synthetic staple, retail
    "551120", # Yarn <85% synthetic, mixed w/ synthetic
    "5512",   # Woven fabrics ≥85% synthetic staple
    "5513",   # Woven fabrics <85% synthetic w/ cotton, <170g/m²
    "5514",   # Woven fabrics <85% synthetic w/ cotton, ≥170g/m²
    "5515",   # Other woven fabrics of synthetic staple fibers

    # --- Artificial staple fibers ---
    "5502",   # Artificial filament tow
    "5504",   # Artificial staple fibers, not carded/combed
    "550520", # Waste of artificial fibers
    "5507",   # Artificial staple fibers, carded/combed
    "550820", # Sewing thread of artificial staple fibers
    "5510",   # Yarn of artificial staple fibers
    "551130", # Yarn <85% artificial staple fibers
    "5516",   # Woven fabrics of artificial staple fibers
]

print(f"Total codes to collect: {len(ALL_CODES)}")

# =============================================================================
# HELPERS
# =============================================================================

def batch_list(lst, n):
    """Split list into chunks of size n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_batch(codes, label):
    """
    Call Comtrade for a batch of codes.
    Skips if file already exists on disk — safe to re-run after interruption.
    """
    filepath = os.path.join(RAW_DIR, f"{label}.csv")

    # Skip if already downloaded
    if os.path.exists(filepath):
        df_existing = pd.read_csv(filepath)
        if not df_existing.empty:
            print(f"  ⏭  {label} already on disk ({len(df_existing):,} rows) — skipping")
            return df_existing

    cmd_string = ",".join(codes)
    print(f"  → Calling API [{label}]: {cmd_string}")

    try:
        df = comtradeapicall.getFinalData(
            subscription_key = API_KEY,   # Move the key to the top
            typeCode         = "C",       # Commodities
            freqCode         = "A",       # Annual
            clCode           = "HS",      # HS classification
            period           = YEAR,
            reporterCode     = None,      # All reporting (exporting) countries
            cmdCode          = cmd_string,
            flowCode         = "X",       # Exports — ties record to the origin country
            partnerCode      = None,      # All destination countries
            partner2Code     = None,
            customsCode      = None,
            motCode          = None,      # All modes of transport (captured where reported)
            maxRecords       = 100000,
            format_output    = "JSON",
            countOnly        = None,
            includeDesc      = True       # Human-readable country/commodity names
        )

        # Respect the free tier — don't hammer the API
        time.sleep(2.5)

        if df is not None and not df.empty:
            df.to_csv(filepath, index=False)
            print(f"      {len(df):,} rows saved → {filepath}")
            return df
        else:
            print(f"     Error:  Empty response for {label}")
            pd.DataFrame().to_csv(filepath, index=False)  # placeholder so we don't retry
            return pd.DataFrame()

    except Exception as e:
        print(f"      Error on {label}: {e}")
        print(f"        Waiting 15s before continuing...")
        time.sleep(15)
        return pd.DataFrame()


# =============================================================================
# MAIN DOWNLOAD LOOP
# =============================================================================

print("=" * 60)
print(f"  Silk Road — Comtrade Pull  |  Year: {YEAR}")
print("=" * 60)

batches    = list(batch_list(ALL_CODES, BATCH_SIZE))
all_frames = []

print(f"\n  {len(ALL_CODES)} codes → {len(batches)} batches of {BATCH_SIZE}\n")

for i, batch in enumerate(batches):
    label = f"batch_{i+1:02d}_of_{len(batches):02d}"
    df = fetch_batch(batch, label)
    if not df.empty:
        all_frames.append(df)

# =============================================================================
# COMBINE & SAVE
# =============================================================================

print("\n[Combining all batches...]")

if not all_frames:
    print("Error: No data collected. Check your API key and internet connection.")
    exit()

combined = pd.concat(all_frames, ignore_index=True)
print(f"  Rows before deduplication: {len(combined):,}")
combined = combined.drop_duplicates()
print(f"  Rows after deduplication:  {len(combined):,}")

# Save the raw combined file
raw_output = os.path.join(RAW_DIR, "_combined_raw.csv")
combined.to_csv(raw_output, index=False)

# =============================================================================
# SUMMARY
# =============================================================================

print("\n" + "=" * 60)
print("  DOWNLOAD COMPLETE")
print("=" * 60)
print(f"  Total records:        {len(combined):,}")
print(f"  Unique HS codes:      {combined['cmdCode'].nunique() if 'cmdCode' in combined.columns else 'N/A'}")
print(f"  Exporting countries:  {combined['reporterISO'].nunique() if 'reporterISO' in combined.columns else 'N/A'}")
print(f"  Importing countries:  {combined['partnerISO'].nunique() if 'partnerISO' in combined.columns else 'N/A'}")

# Check what columns came back (useful to know before cleaning)
print(f"\n  Columns returned by API:")
for col in combined.columns.tolist():
    nulls = combined[col].isnull().sum()
    pct   = nulls / len(combined) * 100
    print(f"    {col:<30} {nulls:>8,} nulls ({pct:.1f}%)")

print(f"\n  Raw file → {raw_output}")
print("=" * 60)