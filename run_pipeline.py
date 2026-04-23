"""
run_pipeline.py
Master orchestration script. Runs the entire data transformation,
enrichment, and routing pipeline in sequential order.
"""

import subprocess
import sys
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PIPELINE = [
    "clean_data.py",
    "add_distance_and_emissions.py",
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
    print("STARTING FULL DATA PIPELINE BUILD...")
    start_time = time.time()

    for script in PIPELINE:
        run_script(script)

    elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"PIPELINE COMPLETE IN {elapsed:.1f} SECONDS!")
    print("Your dashboard data is fully updated. Refresh your browser.")
    print(f"{'='*80}\n")
