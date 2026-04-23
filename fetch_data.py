"""
fetch_data.py
Orchestrates the data collection phase.
Run this only when you need to pull fresh data from the UN Comtrade API.
"""

import subprocess
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_script(script_path):
    print(f"\n{'='*80}")
    print(f"INITIALIZING: {script_path}")
    print(f"{'='*80}\n")

    result = subprocess.run([sys.executable, script_path])

    if result.returncode != 0:
        print(f"\nERROR: Failed to execute {script_path}.")
        sys.exit(1)

    print("\nSUCCESS: Data collection complete.")


if __name__ == "__main__":
    target_script = os.path.join(BASE_DIR, "pipeline", "data_collection.py")
    run_script(target_script)
