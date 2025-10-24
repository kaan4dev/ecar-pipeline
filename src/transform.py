import sys
import os
import logging
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, trim
import great_expectations as ge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("ecar-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

os.chdir(CURRENT_DIR)
logging.info(f"Working directory set to: {CURRENT_DIR}")

RAW_DIR = os.path.join(CURRENT_DIR, "data/raw/ecar")
PROCESSED_DIR = os.path.join(CURRENT_DIR, "data/processed/ecar")
os.makedirs(PROCESSED_DIR, exist_ok=True)

def transform_ecar_data():
    spark = SparkSession.builder.appName("ECar_Transform_Pipeline").getOrCreate()

    latest_file = sorted(os.listdir(RAW_DIR))[-1]
    raw_path = os.path.join(RAW_DIR, latest_file)
    out_path = os.path.join(PROCESSED_DIR, f"processed_{latest_file}")

    logging.info(f"Reading from: {raw_path}")
    if raw_path.endswith(".csv"):
        df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)
    elif raw_path.endswith(".parquet"):
        df = spark.read.parquet(raw_path)
    else:
        raise ValueError("Unsupported file format: only CSV and Parquet are supported")

    logging.info("Initial Schema:")
    df.printSchema()

    # step 1 -> replace the null values with "Unknown"
    df_clean = df.withColumn(
        "model",
        when((col("model").isNull()) | (trim(col("model")) == ""), "Unknown")
        .otherwise(col("model"))
    )

    df_clean = df_clean.withColumn(
        "brand",
        when((col("brand").isNull()) | (trim(col("brand")) == ""), "Unknown")
        .otherwise(col("brand"))
    )

    # step 2 -> drop the negative values
    df_clean = df_clean.filter(
        (col("top_speed_kmh") > 0) &
        (col("battery_capacity_kWh") > 0) &
        (col("range_km") > 0)
    )

    # step 3 -> turn milimetre values into metre values
    df_clean = df_clean.withColumn("length_m", col("length_mm") / 1000)
    df_clean = df_clean.withColumn("width_m", col("width_mm") / 1000)
    df_clean = df_clean.withColumn("height_m", col("height_mm") / 1000)
    df_clean = df_clean.drop("length_mm", "width_mm", "height_mm")

    # step 4 -> fix "banana box" and nulls in cargo_volume_l
    cleaned_cargo_value = regexp_replace(col("cargo_volume_l"), "[^0-9.]", "")
    df_clean = df_clean.withColumn(
        "cargo_volume_l",
        when(
            col("cargo_volume_l").rlike("banana"),
            cleaned_cargo_value.cast("double") * 20,
        ).otherwise(cleaned_cargo_value.cast("double"))
    )

    df_clean = df_clean.fillna({"towing_capacity_kg": 0})

    # step 5 -> drop battery_type because all values are same
    df_clean = df_clean.drop("battery_type")

    logging.info("Cleaned Schema:")
    df_clean.printSchema()

    logging.info("Sample after cleaning:")
    df_clean.show(5)

    # Great Expectations (Validator API)
    logging.info("Running Data Quality Validation with Great Expectations...")

    context = ge.get_context()
    datasource = context.data_sources.add_or_update_spark(name="spark_datasource")
    data_asset = datasource.add_dataframe_asset(name="ecar_dataframe")
    batch_request = data_asset.build_batch_request(options={"dataframe": df_clean})
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="ecar_suite",
    )

    validator.expect_column_values_to_not_be_null("brand")
    validator.expect_column_values_to_not_be_null("model")
    validator.expect_column_values_to_be_between("top_speed_kmh", min_value=1, max_value=500)
    validator.expect_column_values_to_be_between("battery_capacity_kWh", min_value=5, max_value=300)
    validator.expect_column_values_to_be_between("range_km", min_value=20, max_value=2000)
    validator.expect_column_distinct_values_to_be_in_set("drivetrain", ["FWD", "RWD", "AWD", "4WD"])
    validator.expect_column_values_to_be_of_type("length_m", "DoubleType")
    validator.expect_column_values_to_be_of_type("cargo_volume_l", "DoubleType")

    results = validator.validate()

    report_path = os.path.join(CURRENT_DIR, "expectations/ecar_validation_report.json")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(results.to_json_dict(), f, indent=4)

    logging.info(f"Validation report saved to: {report_path}")

    if not results.success:
        logging.error("Data validation failed! Check the report for details.")
        raise ValueError("Data validation failed!")

    # step 6 -> save processed data
    df_clean.write.mode("overwrite").parquet(out_path)
    logging.info(f"Cleaned data saved to: {out_path}")

if __name__ == "__main__":
    transform_ecar_data()
