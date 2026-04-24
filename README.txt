================================================================================
SILK ROAD — Global Textile Trade Carbon Footprint Visualizer
Team 95 | CSE6242 Data and Visual Analytics | Georgia Tech Spring 2026
================================================================================

DESCRIPTION
-----------
Silk Road is an interactive web-based visualization that maps the carbon
footprint of global textile trade using UN Comtrade 2023 export data. The
system covers six fabric categories (Cotton, Silk, Wool, Linen/Hemp,
Synthetic, Viscose) across four transport modes (Ocean, Air, Road, Rail).

The pipeline works in four stages:
  1. Raw UN Comtrade data is collected, cleaned and transport modes are inferred for
     the ~50% of rows that lack a reported mode. Inference uses an XGBoost
     ML classifier trained on reported rows, followed by a geographic
     override pass that enforces physical constraints (e.g., Rail is
     impossible across the Pacific Ocean).
  2. Haversine great-circle distances between country centroids are
     calculated and combined with EPA/GLEC emission factors to produce a
     CO2e estimate for every shipment.
  3. The top 30 routes per fabric-mode combination are aggregated into
     routes.json for the frontend.
  4. Yen's k-shortest-paths algorithm and Dijkstra's algorithm generates up to 3 carbon-saving 
     alternative routes for each origin-destination pair, with estimated CO2 savings and
     transit time impact. Also we calculate the carbon saving potential of alternative
     transportation methods.

The frontend (silk_road_map.html) is a D3.js map that lets users:
  - Filter routes by fabric type and transport mode
  - Toggle between the 60 highest-emission trade routes and all routes
  - Filter for routes that have alternative carbon-saving routes
  - Filter for routes that have alternative carbon-saving transportation
  - Click on any country to see its trade routes.
  - Zoom into the map to better see and click countries.
  - Click any route to see its emissions, volume, distance, and
    algorithmically-generated greener alternatives including both mode
    switching (What-If analysis) and multi-hop re-routing


INSTALLATION
------------
Requirements: Python 3.10+, pip, a modern web browser

1. Unzip the project folder and navigate into it:

      cd silk-road

2. Install Python dependencies:

      pip install -r requirements.txt


3. (Optional) UN Comtrade API key — only needed to re-fetch raw trade data.
   A pre-built demo dataset covering all six fabric types, all four transport
   modes, and multi-hop route alternatives is included. The dashboard runs
   without an API key.

   If you want to pull fresh data from the UN Comtrade API follow these steps:
     a. Register for a free account at https://comtradeplus.un.org
     b. Once logged into your new account, hover over the top right profile icon and select "My API Portal"
     c. Login with the same credientals as your Comtrade account or create a new account.
     d. Once logged in, click "Explore APIs" on the homescreen or "Products" in the top right menu bar.
     e. Select "Free APIs' under Products
     f. Then under "Your Subscriptions", type in the name you would like to call your API into the "Your new product subscription name" box.
     g. Click on "Subscribe"
     h. The page should reload and under "Subscriptions" you should be able to see your primary key when you toggle the visibility using 
        the eye that spawns when you hover over the primary key.
     i. Create a file named .env in the project root with this content 
        (make sure to paste your primary key where it says your_key_here):

           COMTRADE_API_KEY=your_key_here

     j. Then run:  python fetch_data.py


EXECUTION
---------
--- Running the dashboard (recommended — no raw data needed) ---

The demo dataset (data/raw/2023/_combined_raw.csv) is included.
To rebuild routes.json and recommendations.json from it and launch the map:

  1. Run the pipeline (takes ~10 seconds):

        python run_pipeline.py

     The script auto-detects whether raw data is present. Without raw data
     it runs only the export and optimization steps (steps 3-4). With raw
     data present it runs all four steps.

  2. Start a local HTTP server from the project root:

        python -m http.server 8000

  3. Open your browser and go to:

        http://localhost:8000/silk_road_map.html


================================================================================
