# ECar ETL Pipeline

End-to-End Data Engineering project that builds a complete **ETL workflow** for Electric Car specifications — from raw data extraction to transformation, validation, modeling, analysis, and loading into Azure Data Lake, orchestrated with **Apache Airflow**.

---

## Project Overview

This project simulates a real-world data engineering pipeline for **electric vehicle datasets**, implementing the following key stages:

1. **Extract** – Load raw CSV data and convert it into efficient Parquet format.  
2. **Transform** – Clean and standardize data using **PySpark**, including unit conversions, null handling, and schema normalization.  
3. **Validate** – Apply **Great Expectations** to ensure data quality before loading.  
4. **Load** – Upload processed Parquet files to **Azure Data Lake Storage Gen2**.  
5. **Model** – Build dimensional model tables (`dim_brand`, `dim_model`, `fact_vehicle_spec`) for analysis.  
6. **Analyze** – Run SQL analytics on modeled data using **DuckDB**.  
7. **Orchestrate** – Schedule and monitor all tasks with **Apache Airflow** DAGs.

---

## Tech Stack

| Category | Tools |
|-----------|-------|
| Programming | Python 3.13, PySpark |
| Data Quality | Great Expectations |
| Orchestration | Apache Airflow (Docker Compose setup) |
| Storage | Azure Data Lake Gen2 |
| Data Modeling | Dimensional (Star Schema) |
| Query Engine | DuckDB |
| Logging | Python `logging` module |
| File Format | CSV → Parquet |

---

## Project Structure

```
ecar-pipeline/
│
├── data/
│   ├── raw/                 # Original CSV input
│   ├── raw_parquet/         # Converted raw data (Parquet)
│   ├── processed/           # Cleaned and validated data
│   ├── modeled/             # Dimensional tables
│   └── analysis/            # DuckDB queries and results
│
├── src/
│   ├── extract_ecar.py      # Extract & convert CSV to Parquet
│   ├── transform_ecar.py    # PySpark transformation + validation
│   ├── load.py              # Upload to Azure Data Lake
│   ├── model_ecar.py        # Build star schema tables
│   └── analysis_ecar.py     # SQL-based analytics with DuckDB
│
├── utils/
│   └── io_datalake.py       # Azure Data Lake utility functions
│
├── dags/
│   └── ecar_dag.py          # Airflow DAG orchestration
│
├── expectations/
│   └── ecar_validation_report.json
│
├── docker-compose.yaml
├── .env                     # Azure credentials and configs
└── README.md
```

---

## Pipeline Flow

### 1️Extract
Reads raw CSVs, logs shape and encoding, and converts them to Parquet for faster processing.

### 2️Transform & Validate
Cleans and standardizes the dataset:
- Fills missing brand/model names  
- Filters out invalid numerical values  
- Converts millimeters → meters  
- Normalizes cargo volume units  
- Drops redundant columns  
- Applies **Great Expectations** validation suite (8 critical tests)

### 3️Load
Uploads processed data to **Azure Data Lake**, using environment variables:
```
AZURE_STORAGE_ACCOUNT_NAME=
AZURE_STORAGE_ACCOUNT_KEY=
AZURE_FILE_SYSTEM=
```

### 4️Model
Creates a dimensional structure:
- `dim_brand`
- `dim_model`
- `fact_vehicle_spec`

### 5️Analyze
Runs analytical SQL queries via **DuckDB**:
- Top 10 longest-range vehicles  
- Average battery capacity per brand  
- Average top speed per brand  
- Correlation between battery capacity and range

### 6️Orchestrate with Airflow
Automates the ETL flow (`extract → transform → load`) through DAG:
```
extract_raw_data >> transform_and_validate >> load_to_azure
```

---

## Example Outputs

**Analysis Results (saved as CSVs):**
- `top_longest_range.csv`
- `avg_battery_by_brand.csv`
- `avg_speed_by_brand.csv`

Each file is stored under `data/analysis/ecar/` and can be visualized or imported into BI tools.

---

## Key Learnings

- Practical use of **PySpark** transformations at scale.  
- Building **data quality gates** with Great Expectations.  
- Secure and modular **Azure Data Lake** integration.  
- End-to-end orchestration with **Airflow DAGs** in Docker.  
- Dimensional modeling and analytical SQL design using **DuckDB**.
