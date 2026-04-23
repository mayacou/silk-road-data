import os
import sys
import json
import pickle
import random
import pandas as pd
import numpy as np
import io
from math import radians, cos, sin, asin, sqrt
from sklearn.metrics import accuracy_score, classification_report
from tabulate import tabulate

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
pipeline_dir = os.path.join(project_root, 'pipeline')

sys.path.insert(0, script_dir)    # analysis/ — for heuristic_predictor, hybrid_arbitrator
sys.path.insert(0, pipeline_dir)  # pipeline/ — for clean_data
import clean_data
import heuristic_predictor as hp
import hybrid_arbitrator as ha

# File paths
data_path = os.path.join(project_root, 'data', 'clean', 'silk_road_2023_refined.csv')
centroids_path = os.path.join(project_root, 'data', 'country_centroids.json')
model_path = os.path.join(project_root, 'models', 'baseline_model.pkl')
encoder_path = os.path.join(project_root, 'models', 'label_encoder.pkl')

# Capture output to file
old_stdout = sys.stdout
sys.stdout = buffer = io.StringIO()

print("="*70)
print("ULTIMATE EVALUATION: ARBITRATOR HYBRID ARCHITECTURE")
print("="*70)

# ============================================================================
# STEP 1: Load Saved Assets
# ============================================================================
print("\n[1/5] Loading saved assets...")
try:
    with open(model_path, 'rb') as f:
        model_b = pickle.load(f)
    print("  Loaded XGBoost model")
except FileNotFoundError:
    print(f"  Model file not found at {model_path}")
    exit(1)

try:
    with open(encoder_path, 'rb') as f:
        label_encoder = pickle.load(f)
    print("  Loaded LabelEncoder")
except FileNotFoundError:
    print(f"  Encoder file not found at {encoder_path}")
    exit(1)

# ============================================================================
# STEP 2: Load Ground Truth Data
# ============================================================================
print("\n[2/5] Loading ground truth data...")
df = pd.read_csv(data_path)
print(f"  Total rows in dataset: {len(df):,}")

df = df[df['is_inferred'] == False].copy()
print(f"  Rows after filtering (is_inferred == False): {len(df):,}")

print("  Loading country centroids...")
with open(centroids_path, 'r') as f:
    centroids = json.load(f)

# Distance helpers
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    return 6371 * c


def angular_distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    return c * (180 / np.pi)

print("  Calculating distances...")
distances_km = []
distances_deg = []
for idx, row in df.iterrows():
    reporter_iso = row['reporterISO']
    partner_iso = row['partnerISO']

    if reporter_iso in centroids and partner_iso in centroids:
        reporter_coords = centroids[reporter_iso]
        partner_coords = centroids[partner_iso]
        distances_km.append(haversine(reporter_coords['lon'], reporter_coords['lat'], partner_coords['lon'], partner_coords['lat']))
        distances_deg.append(angular_distance(reporter_coords['lon'], reporter_coords['lat'], partner_coords['lon'], partner_coords['lat']))
    else:
        distances_km.append(np.nan)
        distances_deg.append(np.nan)

    if (idx + 1) % 100000 == 0:
        print(f"    Processed {idx + 1:,} rows...")


df['distance_km'] = distances_km
df['distance_deg'] = distances_deg
print("  Creating derived features...")
df['value_density'] = df['fobvalue'] / df['netWgt']
df['is_high_value'] = (df['value_density'] > 50).astype(int)

required_cols = ['netWgt', 'fobvalue', 'distance_km', 'value_density', 'is_high_value', 'reporterISO', 'partnerISO', 'Final_Mode']
initial_len = len(df)
df = df.dropna(subset=required_cols)
print(f"  Rows after dropping NaN values: {len(df):,} (dropped {initial_len - len(df):,})")

# ============================================================================
# STEP 3: Load Geographic Helper Data
# ============================================================================
print("\n[3/5] Loading geographic helper data...")
countries, cont_col = clean_data.load_map()

iso_to_geom = {}
iso_to_continent = {}
for iso, row in countries.iterrows():
    iso_to_geom[iso] = row['geometry']
    iso_to_continent[iso] = row[cont_col]

print(f"  Loaded {len(iso_to_geom)} geographic entities")

# ============================================================================
# STEP 4: Define Model Prediction Functions
# ============================================================================
print("\n[4/5] Defining model functions...")

def predict_model1_clean_data(test_df):
    test_df = test_df.copy()
    test_df['Calculated_Mode'] = 'Unknown'
    return [hp.run_inference(row, countries, cont_col)[0] for _, row in test_df.iterrows()]


def predict_model2_ml(test_X):
    y_pred = model_b.predict(test_X)
    return label_encoder.inverse_transform(y_pred)


def predict_model3_arbitrator(test_df, model1_preds, model2_preds):
    predictions = []
    for i, (_, row) in enumerate(test_df.iterrows()):
        m1 = model1_preds[i]
        m2 = model2_preds[i]
        predictions.append(ha.predict_hybrid(row, m1, m2, iso_to_geom, iso_to_continent))
    return predictions

print("  Model functions defined")

# ============================================================================
# STEP 5: Generate Predictions and Analyze
# ============================================================================
print("\n[5/5] Generating predictions and analyzing results...")

test_X = df[['netWgt', 'fobvalue', 'distance_km', 'value_density', 'is_high_value']]
y_true = df['Final_Mode']

print("  Generating Model 1 predictions (Clean Data Inference)...")
y_pred_1 = predict_model1_clean_data(df)

print("  Generating Model 2 predictions (Pure ML)...")
y_pred_2 = predict_model2_ml(test_X)

print("  Generating Model 3 predictions (Arbitrator Hybrid)...")
y_pred_3 = predict_model3_arbitrator(df, y_pred_1, y_pred_2)

print("  Predictions generated")

# Accuracy metrics
acc_1 = accuracy_score(y_true, y_pred_1)
acc_2 = accuracy_score(y_true, y_pred_2)
acc_3 = accuracy_score(y_true, y_pred_3)

# Error overlap analysis
ml_errors = np.array(y_pred_2) != np.array(y_true)
clean_errors = np.array(y_pred_1) != np.array(y_true)

total_ml_errors = int(np.sum(ml_errors))
total_clean_errors = int(np.sum(clean_errors))
mutual_failures = int(np.sum(np.logical_and(ml_errors, clean_errors)))
ml_failed_clean_succeeded = int(np.sum(np.logical_and(ml_errors, ~clean_errors)))
clean_failed_ml_succeeded = int(np.sum(np.logical_and(clean_errors, ~ml_errors)))

random.seed(42)
ml_error_indices = np.where(ml_errors)[0].tolist()
sample_indices = random.sample(ml_error_indices, min(15, len(ml_error_indices))) if ml_error_indices else []

audit_rows = []
for idx in sample_indices:
    row = df.iloc[idx]
    audit_rows.append([
        row['reporterISO'],
        row['partnerISO'],
        row['Final_Mode'],
        y_pred_2[idx],
        y_pred_1[idx]
    ])

improvement_1 = 0.0
improvement_2 = ((acc_2 - acc_1) / acc_1 * 100) if acc_1 else float('nan')
improvement_3 = ((acc_3 - acc_1) / acc_1 * 100) if acc_1 else float('nan')

accuracy_table = [
    ['Model 1 (Clean Data Inference)', f'{acc_1*100:6.2f}%', f'{improvement_1:6.2f}%'],
    ['Model 2 (Pure ML)', f'{acc_2*100:6.2f}%', f'{improvement_2:6.2f}%'],
    ['Model 3 (Arbitrator Hybrid)', f'{acc_3*100:6.2f}%', f'{improvement_3:6.2f}%']
]

print("\n" + "="*70)
print("ACCURACY COMPARISON TABLE")
print("="*70)
print(tabulate(accuracy_table, headers=['Model', 'Accuracy', 'Improvement vs. Baseline (Model 1)'], tablefmt='github'))
print("="*70)

# Mode distribution comparison
mode_labels = ['Air', 'Ocean', 'Rail', 'Road']
y_true_counts = pd.Series(y_true).value_counts().reindex(mode_labels, fill_value=0)
y_pred_1_counts = pd.Series(y_pred_1).value_counts().reindex(mode_labels, fill_value=0)
y_pred_2_counts = pd.Series(y_pred_2).value_counts().reindex(mode_labels, fill_value=0)
y_pred_3_counts = pd.Series(y_pred_3).value_counts().reindex(mode_labels, fill_value=0)

distribution_table = []
for mode in mode_labels:
    distribution_table.append([
        mode,
        int(y_true_counts[mode]),
        int(y_pred_1_counts[mode]),
        int(y_pred_2_counts[mode]),
        int(y_pred_3_counts[mode])
    ])

print("\nMODE PREDICTION DISTRIBUTION COMPARISON")
print("="*70)
print(tabulate(distribution_table, headers=['Mode', 'True Labels', 'Model 1', 'Model 2', 'Model 3'], tablefmt='github'))
print("="*70)

print("\nDETAILED CLASSIFICATION REPORT (Model 1 - Clean Data Inference)")
print("="*70)
print(classification_report(y_true, y_pred_1, labels=mode_labels))
print("="*70)

print("\nDETAILED CLASSIFICATION REPORT (Model 2 - Pure ML)")
print("="*70)
print(classification_report(y_true, y_pred_2, labels=mode_labels))
print("="*70)

print("\nDETAILED CLASSIFICATION REPORT (Model 3 - Arbitrator Hybrid)")
print("="*70)
print(classification_report(y_true, y_pred_3, labels=mode_labels))
print("="*70)

print("\nERROR OVERLAP STATISTICS")
print("="*70)
print(f"Total ML Errors:                {total_ml_errors}")
print(f"Total Clean Data Errors:        {total_clean_errors}")
print(f"Mutual Failures:                {mutual_failures}")
print(f"ML Failed, Clean Succeeded:     {ml_failed_clean_succeeded}")
print(f"Clean Failed, ML Succeeded:     {clean_failed_ml_succeeded}")
print("="*70)

print("\nRANDOM SAMPLE AUDIT — ML INCORRECT ROWS")
print("="*70)
if audit_rows:
    print(tabulate(audit_rows, headers=['Origin ISO', 'Destination ISO', 'True UN Label', 'ML Prediction', 'Clean Data Prediction'], tablefmt='github'))
else:
    print("No ML errors found in the filtered dataset.")
print("="*70)

print("\nUltimate evaluation complete.")

# Restore stdout and save output to file
sys.stdout = old_stdout
results_dir = os.path.join(project_root, 'evaluation_results')
os.makedirs(results_dir, exist_ok=True)
output_file = os.path.join(results_dir, 'evaluation_results.txt')
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(buffer.getvalue())
print(f"Results saved to {output_file}")

# Save key metrics to JSON
results = {
    'accuracies': {
        'model1': acc_1,
        'model2': acc_2,
        'model3': acc_3
    },
    'improvements': {
        'model2': improvement_2,
        'model3': improvement_3
    },
    'distributions': {
        'true': y_true_counts.to_dict(),
        'model1': y_pred_1_counts.to_dict(),
        'model2': y_pred_2_counts.to_dict(),
        'model3': y_pred_3_counts.to_dict()
    },
    'error_stats': {
        'total_ml_errors': total_ml_errors,
        'total_clean_errors': total_clean_errors,
        'mutual_failures': mutual_failures,
        'ml_failed_clean_succeeded': ml_failed_clean_succeeded,
        'clean_failed_ml_succeeded': clean_failed_ml_succeeded
    },
    'audit_rows': audit_rows
}
json_file = os.path.join(results_dir, 'evaluation_results.json')
with open(json_file, 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=4)
print(f"Key metrics saved to {json_file}")
