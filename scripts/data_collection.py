# =============================================================================
# Silk Road — Data Collection Script
# UN Comtrade Free Tier
# Captures: origin, destination, weight, mode of transport, HS code
#
# YEARS: controls which years are fetched. Data lands in data/raw/<year>/.
# Raw files are skipped if they already exist, so this is safe to re-run.
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
# Add or remove years to control what gets collected.
# Free-tier limit: 500 requests/day — with 6 batches × 5 years = 30 requests total.
YEARS      = ["2019", "2020", "2021", "2022", "2023"]
BATCH_SIZE = 10

# =============================================================================
# ALL HS CODES TO COLLECT
# =============================================================================

ALL_CODES = [
    # --- Silk (Chapter 50) ---
    # FIX: chapter-level "50" returns weight=0 aggregates. Use 4-digit codes.
    "5002",   # Raw silk, not thrown
    "5003",   # Silk waste
    "5004",   # Silk yarn, not retail
    "5005",   # Yarn of silk waste, not retail
    "5007",   # Woven fabrics of silk or silk waste

    # --- Wool & fine animal hair (Chapter 51, specific codes only) ---
    "5101",
    "510211", "510219",
    "510310", "510320",
    "510510", "510521", "510531", "510539",
    "5106", "5107", "5108", "5109",
    "5111", "5115",

    # --- Cotton (Chapter 52) ---
    # FIX: chapter-level "52" returns weight=0 aggregates. Use 4-digit codes.
    "5201",   # Cotton, not carded or combed
    "5202",   # Cotton waste
    "5203",   # Cotton, carded or combed
    "5205",   # Cotton yarn, ≥85% cotton, not retail
    "5206",   # Cotton yarn, <85% cotton, not retail
    "5208",   # Woven fabrics, ≥85% cotton, ≤200 g/m²
    "5209",   # Woven fabrics, ≥85% cotton, >200 g/m²

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


def fetch_batch(codes, label, year, raw_dir):
    """
    Call Comtrade for a batch of codes for one year.
    Skips if file already exists on disk — safe to re-run after interruption.
    """
    filepath = os.path.join(raw_dir, f"{label}.csv")

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
            subscription_key = API_KEY,
            typeCode         = "C",       # Commodities
            freqCode         = "A",       # Annual
            clCode           = "HS",      # HS classification
            period           = year,
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

batches = list(batch_list(ALL_CODES, BATCH_SIZE))

print("=" * 60)
print(f"  Silk Road — Comtrade Pull")
print(f"  Years: {', '.join(YEARS)}  |  {len(ALL_CODES)} codes → {len(batches)} batches")
print("=" * 60)

for year in YEARS:
    raw_dir = os.path.join("data", "raw", year)
    os.makedirs(raw_dir, exist_ok=True)

    print(f"\n── Year {year} ──────────────────────────────────────────")
    year_frames = []

    for i, batch in enumerate(batches):
        label = f"batch_{i+1:02d}_of_{len(batches):02d}"
        df = fetch_batch(batch, label, year, raw_dir)
        if not df.empty:
            year_frames.append(df)

    if not year_frames:
        print(f"  Warning: No data collected for {year}.")
        continue

    combined = pd.concat(year_frames, ignore_index=True)
    combined = combined.drop_duplicates()

    raw_output = os.path.join(raw_dir, "_combined_raw.csv")
    combined.to_csv(raw_output, index=False)

    print(f"\n  {year} complete: {len(combined):,} rows → {raw_output}")

print("\n" + "=" * 60)
print("  ALL YEARS DOWNLOADED")
print("=" * 60)