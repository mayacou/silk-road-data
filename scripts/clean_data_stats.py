import pandas as pd
import os

REFINED_FILE = "data/clean/silk_road_2023_refined.csv"

def main():
    if not os.path.exists(REFINED_FILE):
        print("❌ Run clean_data.py first.")
        return

    df = pd.read_csv(REFINED_FILE)
    total_rows = len(df)
    inferred_df = df[df['is_inferred'] == True]
    inf_count = len(inferred_df)

    print("="*65)
    print("SILK ROAD INFERENCE AUDIT")
    print("="*65)

    # 1. Total Inferred
    print(f"Total Rows Inferred:      {inf_count:,}")
    print(f"Percentage of Dataset:    {(inf_count/total_rows)*100:.2f}%")
    print("-" * 65)

    # 2. Breakdown of each Inferred Mode
    print("INFERRED MODE BREAKDOWN:")
    mode_counts = inferred_df['Final_Mode'].value_counts()
    for mode, count in mode_counts.items():
        print(f"  - {mode:6}: {count:8,} rows ({(count/inf_count)*100:5.2f}% of inferences)")
    print("-" * 65)

    # 3. The Why (Inference Reason)
    print("INFERENCE LOGIC BREAKDOWN:")
    reason_counts = inferred_df['inference_reason'].value_counts()
    for reason, count in reason_counts.items():
        print(f"  - {reason:18}: {count:8,} rows ({(count/inf_count)*100:5.2f}% of inferences)")
    
    print("-" * 65)

    # 4. NEW: Map Default Country Breakdown
    print("MAP DEFAULT COUNTRY ANALYSIS (ISO codes missing from map):")
    map_defaults = inferred_df[inferred_df['inference_reason'] == 'Map Default']
    
    if not map_defaults.empty:
        # Check both reporters and partners that caused a miss
        # We'll create a combined list of missing ISOs and their associated Names
        missing_reporters = map_defaults[['reporterISO', 'reporterDesc']].rename(columns={'reporterISO':'iso', 'reporterDesc':'name'})
        missing_partners = map_defaults[['partnerISO', 'partnerDesc']].rename(columns={'partnerISO':'iso', 'partnerDesc':'name'})
        
        missing_all = pd.concat([missing_reporters, missing_partners])
        
        # We only care about the ones that actually aren't "standard" 
        # (Though in our logic, if they are in 'Map Default', they definitely weren't in the index)
        missing_stats = missing_all.groupby('iso')['name'].first().to_frame()
        missing_stats['row_count'] = missing_all['iso'].value_counts()
        
        # Sort by most frequent misses
        missing_stats = missing_stats.sort_values(by='row_count', ascending=False)

        print(f"{'ISO':<5} | {'COUNTRY NAME':<25} | {'IMPACTED ROWS'}")
        print("-" * 65)
        for iso, row in missing_stats.head(15).iterrows():
            print(f"{iso:<5} | {str(row['name'])[:25]:<25} | {row['row_count']:,}")
    else:
        print("  No map defaults found.")
    
    print("="*65)

if __name__ == "__main__":
    main()