import geopandas as gpd


def are_contiguous(reporter_iso, partner_iso, iso_to_geom):
    if reporter_iso not in iso_to_geom or partner_iso not in iso_to_geom:
        return False
    try:
        return iso_to_geom[reporter_iso].touches(iso_to_geom[partner_iso])
    except Exception:
        return False


def predict_hybrid(row, m1_pred, m2_pred, iso_to_geom, iso_to_continent):
    if m1_pred == m2_pred:
        return m1_pred

    if row['value_density'] > 50:
        return 'Air'
    elif are_contiguous(row['reporterISO'], row['partnerISO'], iso_to_geom):
        return 'Road'
    elif (row['reporterISO'] in iso_to_continent and row['partnerISO'] in iso_to_continent and
          iso_to_continent[row['reporterISO']] != iso_to_continent[row['partnerISO']]):
        return 'Ocean'
    else:
        return m2_pred