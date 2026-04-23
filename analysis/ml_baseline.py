import os
import json
import pickle
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.class_weight import compute_sample_weight
from sklearn.metrics import accuracy_score, classification_report
import xgboost as xgb

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
data_path = os.path.join(project_root, 'data', 'clean', 'silk_road_2023_refined.csv')
centroids_path = os.path.join(project_root, 'data', 'country_centroids.json')

# Function to calculate distance between two lat/lon points (haversine formula)
def haversine(lon1, lat1, lon2, lat2):
    """Calculate distance in km between two points on Earth"""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

# Load the data
print("Loading data...")
df = pd.read_csv(data_path)

# Load country centroids
print("Loading country centroids...")
with open(centroids_path, 'r') as f:
    centroids = json.load(f)

# Calculate distance_km from reporter to partner countries
print("Calculating distances...")
distances = []
for idx, row in df.iterrows():
    reporter_iso = row['reporterISO']
    partner_iso = row['partnerISO']
    
    if reporter_iso in centroids and partner_iso in centroids:
        reporter_coords = centroids[reporter_iso]
        partner_coords = centroids[partner_iso]
        dist = haversine(reporter_coords['lon'], reporter_coords['lat'], 
                        partner_coords['lon'], partner_coords['lat'])
        distances.append(dist)
    else:
        distances.append(np.nan)
    
    if (idx + 1) % 50000 == 0:
        print(f"  Processed {idx + 1} rows...")

df['distance_km'] = distances
print(f"Distance calculation complete. Missing values: {df['distance_km'].isna().sum()}")

# Filter for ground truth rows only (is_inferred == False)
print(f"Total rows: {len(df)}")
df = df[df['is_inferred'] == False]
print(f"Rows after filtering ground truth (is_inferred == False): {len(df)}")

# Feature engineering: Create value_density
df['value_density'] = df['fobvalue'] / df['netWgt']

# is_high_value (1 if value_density > 50, else 0)
df['is_high_value'] = (df['value_density'] > 50).astype(int)

# Define columns to check for NaN values
required_columns = ['netWgt', 'fobvalue', 'distance_km', 'value_density', 'is_high_value']

# Drop rows with missing values in required columns
initial_rows = len(df)
df = df.dropna(subset=required_columns)
print(f"Rows after removing NaN values: {len(df)} (dropped {initial_rows - len(df)} rows)")

# Define features and target
X = df[['netWgt', 'fobvalue', 'distance_km', 'value_density', 'is_high_value']]
y = df['Final_Mode']

print(f"\nTarget classes: {y.unique()}")
print(f"Class distribution:\n{y.value_counts()}")

# Encode target variable using LabelEncoder
label_encoder = LabelEncoder()
y_encoded = label_encoder.fit_transform(y)

print(f"\nEncoded classes: {label_encoder.classes_}")

# Split the data: 80% training, 20% testing
X_train, X_test, y_train, y_test = train_test_split(X, y_encoded, test_size=0.2, random_state=42)

print(f"\nTraining set size: {len(X_train)}")
print(f"Testing set size: {len(X_test)}")

# Compute sample weights to handle class imbalance
sample_weights = compute_sample_weight('balanced', y_train)

# Train the XGBoost model
print("\nTraining XGBoost Classifier...")
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=10,
    random_state=42,
    eval_metric='mlogloss',
    tree_method='hist'
)
model.fit(X_train, y_train, sample_weight=sample_weights, verbose=False)

# Make predictions on the test set
y_pred = model.predict(X_test)

# Calculate and display accuracy
accuracy = accuracy_score(y_test, y_pred)
print("\n" + "="*70)
print("MODEL RESULTS")
print("="*70)
print(f"Overall Accuracy: {accuracy * 100:.2f}%")

# Convert predictions and test labels back to original class names for reporting
y_test_labels = label_encoder.inverse_transform(y_test)
y_pred_labels = label_encoder.inverse_transform(y_pred)

# Print classification report with original class names
print("\nClassification Report:")
print(classification_report(y_test_labels, y_pred_labels))

# Print feature importances as percentages
print("Feature Importances:")
feature_names = ['netWgt', 'fobvalue', 'distance_km', 'value_density', 'is_high_value']
importances = model.feature_importances_
for name, importance in zip(feature_names, importances):
    print(f"  {name}: {importance * 100:.2f}%")

print("="*70)

# Save the trained model and label encoder
print("\nSaving model and label encoder...")
models_dir = os.path.join(project_root, 'models')
os.makedirs(models_dir, exist_ok=True)

model_path = os.path.join(models_dir, 'baseline_model.pkl')
encoder_path = os.path.join(models_dir, 'label_encoder.pkl')

with open(model_path, 'wb') as f:
    pickle.dump(model, f)

with open(encoder_path, 'wb') as f:
    pickle.dump(label_encoder, f)

print(f"Model saved to: {model_path}")
print(f"Label encoder saved to: {encoder_path}")
