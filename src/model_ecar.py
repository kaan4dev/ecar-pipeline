import os
import duckdb
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("ecar-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

PROCESSED_DIR = os.path.join(CURRENT_DIR, "data/processed/ecar")
MODELED_DIR = os.path.join(CURRENT_DIR, "data/modeled/ecar")
os.makedirs(MODELED_DIR, exist_ok=True)

latest = sorted(os.listdir(PROCESSED_DIR))[-1]
processed_path = os.path.join(PROCESSED_DIR, latest)
df = pd.read_parquet(processed_path)

dim_brand = df[['brand']].drop_duplicates().reset_index(drop=True)
dim_brand['brand_id'] = dim_brand.index + 1
dim_model = df[['model', 'brand']].drop_duplicates().reset_index(drop=True)
dim_model = dim_model.merge(dim_brand, on='brand')
dim_model['model_id'] = dim_model.index + 1

fact_vehicle = df.merge(dim_model[['model', 'model_id']], on='model', how='left')
fact_vehicle = fact_vehicle[['model_id', 'top_speed_kmh', 'battery_capacity_kWh', 'range_km', 'length_m', 'width_m', 'height_m', 'cargo_volume_l', 'towing_capacity_kg']]

dim_brand.to_parquet(os.path.join(MODELED_DIR, "dim_brand.parquet"), index=False)
dim_model.to_parquet(os.path.join(MODELED_DIR, "dim_model.parquet"), index=False)
fact_vehicle.to_parquet(os.path.join(MODELED_DIR, "fact_vehicle_spec.parquet"), index=False)

logging.info(f"Modeled tables saved successfully in: {MODELED_DIR}")
