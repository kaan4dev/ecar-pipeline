import os
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",  
    handlers=[
        logging.StreamHandler() 
    ]
)

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("ecar-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

os.chdir(CURRENT_DIR)
logging.info(f"Working directory set to: {CURRENT_DIR}")

RAW_DIR = os.path.join(CURRENT_DIR, "data/raw/ecar")
OUT_DIR = os.path.join(CURRENT_DIR, "data/raw_parquet/ecar")
os.makedirs(OUT_DIR, exist_ok=True)

files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
if not files:
    raise FileNotFoundError(f"No CSV file found in {RAW_DIR}")

dataframes = {}
for file in files:
    path = os.path.join(RAW_DIR, file)
    name = file.replace(".csv", "")
    try:
        df = pd.read_csv(path, encoding="utf-8")
        dataframes[name] = df
        logging.info(f"Loaded {file} -> {df.shape[0]} rows, {df.shape[1]} columns")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin1")
        dataframes[name] = df
        logging.info(f"Loaded {file} (latin1) -> {df.shape[0]} rows, {df.shape[1]} columns")
    except Exception as e:
        logging.error(f"Error reading {file}: {e}")
        continue

for name, df in dataframes.items():
    logging.info(f"Preview of {name}:\n{df.head(3)}")

for name, df in dataframes.items():
    out_path = os.path.join(OUT_DIR, f"raw_{name}.parquet")
    try:
        df.to_parquet(out_path, index=False)
        logging.info(f"Saved to {out_path}")
    except Exception as e:
        logging.error(f"Error saving {name} to parquet: {e}")
