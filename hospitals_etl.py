#!/usr/bin/env python3
"""CMS "Hospitals" daily ETL
Downloads updated hospital‑theme datasets, cleans headers, and stores them.
"""

import os, json, re, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd, requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

META_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DATA_DIR, META_FILE, THEME, MAX_THREADS, TIMEOUT = "data", "run_metadata.json", "Hospitals", 8, 60

def to_snake(col):
    """Convert header to snake_case (lowercase, no spaces/punct)."""
    return re.sub(r"\s+", "_", re.sub(r"[^\w\s]", "", col.strip().replace("'", ""))).lower()

def load_meta():
    return json.load(open(META_FILE)) if os.path.exists(META_FILE) else {}

def save_meta(meta):
    json.dump(meta, open(META_FILE, "w"), indent=2)

def fetch_catalog():
    """Return list of hospital‑theme datasets from metastore."""
    try:
        response = requests.get(META_URL, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        # API returns a list directly, not a dict with "items"
        hospital_datasets = [d for d in data if THEME in d.get("theme", [])]
        logger.info(f"Found {len(hospital_datasets)} hospital-themed datasets")
        return hospital_datasets
    except Exception as e:
        logger.error(f"Error fetching catalog: {e}")
        raise

def get_csv_url(ds):
    """Extract CSV download URL from dataset distribution."""
    distributions = ds.get("distribution", [])
    for dist in distributions:
        if dist.get("mediaType") == "text/csv":
            return dist.get("downloadURL")
    # Fallback: use first distribution if available
    if distributions:
        return distributions[0].get("downloadURL")
    return None

def needs_update(ds, meta):
    # Use "identifier" instead of "id" and "modified" is a string, not nested
    return meta.get(ds["identifier"]) != ds["modified"]

def download(ds, out_dir):
    """Download and process a single dataset."""
    dsid = ds["identifier"]
    title = ds.get("title", "Unknown")
    
    try:
        logger.info(f"Downloading {dsid}: {title}")
        
        # Get the actual CSV download URL from distribution
        csv_url = get_csv_url(ds)
        if not csv_url:
            logger.error(f"No CSV download URL found for {dsid}")
            return dsid, ds["modified"], False
        
        response = requests.get(csv_url, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Try different pandas options for problematic CSVs
        try:
            df = pd.read_csv(csv_url)
        except pd.errors.ParserError:
            logger.warning(f"Standard CSV parsing failed for {dsid}, trying with error_bad_lines=False")
            try:
                df = pd.read_csv(csv_url, on_bad_lines='skip')
            except:
                logger.warning(f"Trying with different encoding for {dsid}")
                df = pd.read_csv(csv_url, encoding='latin-1', on_bad_lines='skip')
        
        # Convert column names to snake_case
        df.columns = [to_snake(c) for c in df.columns]
        
        # Save to file
        path = os.path.join(out_dir, f"{dsid}.csv")
        df.to_csv(path, index=False)
        
        logger.info(f"Successfully processed {dsid} ({len(df)} rows, {len(df.columns)} columns)")
        return dsid, ds["modified"], True
        
    except Exception as e:
        logger.error(f"Failed to download {dsid}: {e}")
        return dsid, ds["modified"], False

def main():
    target_dir = os.path.join(DATA_DIR, dt.date.today().isoformat())
    os.makedirs(target_dir, exist_ok=True)
    
    logger.info(f"Starting ETL job, output directory: {target_dir}")
    
    old_meta, new_meta = load_meta(), {}
    todo = []
    
    for ds in fetch_catalog():
        if needs_update(ds, old_meta): 
            todo.append(ds)
        # Use "identifier" instead of "id" and "modified" is a string
        new_meta[ds["identifier"]] = ds["modified"]
    
    if not todo:
        logger.info("No new datasets to download")
        save_meta(new_meta)
        return
    
    logger.info(f"Downloading {len(todo)} dataset(s)...")
    
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(MAX_THREADS) as pool:
        future_to_ds = {pool.submit(download, ds, target_dir): ds for ds in todo}
        
        for future in as_completed(future_to_ds):
            dsid, modified, success = future.result()
            if success:
                successful += 1
                logger.info(f"✓ {dsid}")
            else:
                failed += 1
                logger.error(f"✗ {dsid}")
    
    save_meta(new_meta)
    logger.info(f"ETL job complete: {successful} successful, {failed} failed")

if __name__ == "__main__": main()

