================================================================================
SILK ROAD — Global Textile Trade Carbon Footprint Visualizer
Team 95 | CSE6242 Data and Visual Analytics | Georgia Tech Spring 2026
================================================================================

DESCRIPTION
-----------
Silk Road is an interactive web-based visualization that maps the carbon
footprint of global textile trade using UN Comtrade 2023 export data. The
system covers six fabric categories (Cotton, Silk, Wool, Linen/Hemp,
Synthetic, Viscose) across four transport modes (Ocean, Air, Road, Rail)
for HS Chapters 50-55.

The pipeline works in four stages:
  1. Raw UN Comtrade data is cleaned and transport modes are inferred for
     the ~50% of rows that lack a reported mode. Inference uses an XGBoost
     ML classifier trained on reported rows, followed by a geographic
     override pass that enforces physical constraints (e.g., Rail is
     impossible across the Pacific Ocean).
  2. Haversine great-circle distances between country centroids are
     calculated and combined with EPA/GLEC emission factors to produce a
     CO2e estimate for every shipment.
  3. The top 30 routes per fabric-mode combination are aggregated into
     routes.json for the frontend.
  4. Yen's k-shortest-paths algorithm generates up to 3
     alternative routes for each origin-destination pair, with estimated
     CO2 savings and transit time impact.

The frontend (silk_road_map.html) is a D3.js map that lets users:
  - Filter routes by fabric type and transport mode
  - Toggle between an overview of the 60 highest-emission trade corridors
    and all 693 individual routes
  - Click any route to see its emissions, volume, distance, and
    algorithmically-generated greener alternatives

DEMO VIDEO
----------
*** Add demo video?


INSTALLATION
------------
Requirements: Python 3.10+, pip, a modern web browser

1. Unzip the project folder and navigate into it:

      cd team95final

2. Install Python dependencies:

      pip install -r requirements.txt

   Key packages: pandas, geopandas, xgboost, scikit-learn, tabulate,
   shapely, requests

3. (Optional) UN Comtrade API key — only needed if re-fetching raw data.
   The processed data files (routes.json, data/recommendations.json) are
   included in the zip so the dashboard runs without an API key.

   If you do want to pull fresh data:
     a. Register for a free API key at https://comtradeplus.un.org
     b. Open pipeline/data_collection.py and set your key in the
        COMTRADE_API_KEY variable at the top of the file.

   NOTE: The raw CSV files (~500 MB) are NOT included in the zip per the
   submission guidelines. The cleaned and processed output files needed to
   run the dashboard ARE included.


EXECUTION
---------
--- Running the dashboard (no rebuild needed) ---

The frontend reads routes.json and data/recommendations.json, which are
included in the zip. To view the map:

  1. Start a local HTTP server from the project root:

        python -m http.server 8000

  2. Open your browser and go to:

        http://localhost:8000/silk_road_map.html

--- Rebuilding the data pipeline ---

If you want to reprocess the data from the included cleaned CSVs:

      python run_pipeline.py

This runs the four pipeline steps in sequence and takes ~45 seconds:
  Step 1: pipeline/clean_data.py            → data/clean/silk_road_2023_refined.csv
  Step 2: pipeline/add_distance_and_emissions.py → data/clean/silk_road_2023_with_emissions.csv
  Step 3: pipeline/export_routes_json.py    → routes.json
  Step 4: pipeline/route_optimization.py   → data/recommendations.json

The pipeline will stop and print an error message if any step fails.

--- Re-fetching raw data from UN Comtrade (requires API key) ---

      python fetch_data.py

Then re-run the pipeline:

      python run_pipeline.py

--- Running the model evaluation ---

      python analysis/ultimate_evaluation.py

Results are written to evaluation_results/evaluation_results.txt and
evaluation_results/evaluation_results.json.

================================================================================
