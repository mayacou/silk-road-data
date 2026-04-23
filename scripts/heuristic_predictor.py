import pandas as pd
import geopandas as gpd

MOT_MAPPING = {
    1000: 'Air', 9200: 'Air',
    3200: 'Road', 3000: 'Road', 3900: 'Road',
    3100: 'Rail',
    2100: 'Ocean', 2000: 'Ocean', 2200: 'Ocean', 2900: 'Ocean'
}

LAND_CONNECTED = {'Europe', 'Asia'}
ISLAND_ISOS = {'GBR', 'JPN', 'PHL', 'AUS', 'IRL'}


def run_inference(row, countries, cont_col):
    current_mode = row['Calculated_Mode']
    if current_mode != 'Unknown':
        return current_mode, False, "Reported"

    iso_o = row['reporterISO']
    iso_d = row['partnerISO']

    if iso_o not in countries.index or iso_d not in countries.index:
        return "Ocean", True, "Map Default"

    origin = countries.loc[iso_o]
    dest   = countries.loc[iso_d]
    distance = origin.geometry.distance(dest.geometry)
    value_density = row['fobvalue'] / row['netWgt'] if row['netWgt'] else float('inf')

    if iso_o != iso_d and (iso_o in ISLAND_ISOS or iso_d in ISLAND_ISOS):
        return "Ocean", True, "Island"

    if value_density > 50:
        return "Air", True, "Value Density"

    if origin.geometry.touches(dest.geometry) or distance < 0.01:
        return "Road", True, "Shared Border"

    same_continent = origin[cont_col] == dest[cont_col]
    same_landmass = same_continent or (
        origin[cont_col] in LAND_CONNECTED and dest[cont_col] in LAND_CONNECTED
    )

    if same_landmass:
        if distance > 1000 and value_density < 5:
            return "Rail", True, "Bulk Rail"
        return "Road", True, "Same Landmass"

    return "Ocean", True, "Intercontinental"