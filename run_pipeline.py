"""
run_pipeline.py
Master orchestration script. Runs the data transformation and routing pipeline.

Demo mode (default): raw CSV data is absent, so steps 1-2 (clean + enrich)
are skipped. The pre-built demo emissions file drives steps 3-4 directly.

Full mode: if data/raw/2023/_combined_raw.csv is present (after running
fetch_data.py), all four pipeline steps run in sequence.
"""

import subprocess
import sys
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_DATA_FILE = os.path.join(BASE_DIR, "data", "raw", "2023", "_combined_raw.csv")

FULL_PIPELINE = [
    "clean_data.py",
    "add_distance_and_emissions.py",
    "export_routes_json.py",
    "route_optimization.py",
]

DEMO_PIPELINE = [
    "export_routes_json.py",
    "route_optimization.py",
]


def run_script(script_name):
    script_path = os.path.join(BASE_DIR, "pipeline", script_name)

    print(f"\n{'='*80}")
    print(f"EXECUTING: {script_path}")
    print(f"{'='*80}\n")

    result = subprocess.run([sys.executable, script_path])

    if result.returncode != 0:
        print(f"\nFATAL ERROR: Pipeline halted at {script_name}.")
        print("Please fix the error above before rebuilding.")
        sys.exit(1)

    print(f"\nSUCCESS: {script_name} completed.")


if __name__ == "__main__":
    raw_data_present = os.path.exists(RAW_DATA_FILE)

    if raw_data_present:
        print("Raw data detected — running FULL pipeline (all 4 steps).")
        pipeline = FULL_PIPELINE
    else:
        print("No raw data found — running DEMO pipeline (steps 3-4 only).")
        print("Pre-built demo dataset will be used for visualization.")
        print("To run the full pipeline, first execute: python fetch_data.py")
        pipeline = DEMO_PIPELINE

    print(f"\nSTARTING PIPELINE BUILD...")
    start_time = time.time()

    for script in pipeline:
        run_script(script)

    elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"PIPELINE COMPLETE IN {elapsed:.1f} SECONDS!")
    print("Your dashboard data is ready. Open the server and refresh your browser.")
    print(f"{'='*80}\n")
