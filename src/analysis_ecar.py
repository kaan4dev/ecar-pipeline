import os
import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("ecar-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

MODELED_DIR = os.path.join(CURRENT_DIR, "data/modeled/ecar")
OUT_DIR = os.path.join(CURRENT_DIR, "data/analysis/ecar")
os.makedirs(OUT_DIR, exist_ok=True)

db_path = os.path.join(OUT_DIR, "ecar_analytics.duckdb")
con = duckdb.connect(database=db_path, read_only=False)

con.execute(f"CREATE OR REPLACE VIEW dim_brand AS SELECT * FROM parquet_scan('{MODELED_DIR}/dim_brand.parquet');")
con.execute(f"CREATE OR REPLACE VIEW dim_model AS SELECT * FROM parquet_scan('{MODELED_DIR}/dim_model.parquet');")
con.execute(f"CREATE OR REPLACE VIEW fact_vehicle AS SELECT * FROM parquet_scan('{MODELED_DIR}/fact_vehicle_spec.parquet');")

logging.info("Tables registered successfully in DuckDB memory.")

query_base = """
    SELECT 
        f.model_id,
        m.model,
        b.brand,
        f.top_speed_kmh,
        f.battery_capacity_kWh,
        f.range_km,
        f.cargo_volume_l,
        f.towing_capacity_kg
    FROM fact_vehicle f
    JOIN dim_model m ON f.model_id = m.model_id
    JOIN dim_brand b ON m.brand_id = b.brand_id
"""
con.execute(f"CREATE OR REPLACE VIEW vehicle_full AS {query_base}")
logging.info("Created joined view: vehicle_full")

logging.info("Top 10 longest range cars:")
result1 = con.execute("""
    SELECT brand, model, range_km
    FROM vehicle_full
    ORDER BY range_km DESC
    LIMIT 10
""").fetchdf()
logging.info(f"\n{result1}")

logging.info("Average battery capacity by brand:")
result2 = con.execute("""
    SELECT brand, ROUND(AVG(battery_capacity_kWh), 2) AS avg_battery
    FROM vehicle_full
    GROUP BY brand
    ORDER BY avg_battery DESC
    LIMIT 10
""").fetchdf()
logging.info(f"\n{result2}")

logging.info("Average top speed by brand:")
result3 = con.execute("""
    SELECT brand, ROUND(AVG(top_speed_kmh), 1) AS avg_speed
    FROM vehicle_full
    GROUP BY brand
    ORDER BY avg_speed DESC
    LIMIT 10
""").fetchdf()
logging.info(f"\n{result3}")

corr = con.execute("""
    SELECT corr(battery_capacity_kWh, range_km) AS correlation
    FROM vehicle_full
""").fetchone()[0]
logging.info(f"Correlation between battery size and range: {corr:.3f}")

result1.to_csv(os.path.join(OUT_DIR, "top_longest_range.csv"), index=False)
result2.to_csv(os.path.join(OUT_DIR, "avg_battery_by_brand.csv"), index=False)
result3.to_csv(os.path.join(OUT_DIR, "avg_speed_by_brand.csv"), index=False)

logging.info(f"SQL analyses completed successfully. Results saved to {OUT_DIR}")
