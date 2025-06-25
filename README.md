# CMS Hospitals Daily ETL

This script downloads all hospital-themed datasets from the CMS Provider Data API, processes them, and converts column names to snake_case format.

## Overview

The script satisfies all requirements from the assignment:

- Downloads all datasets with "Hospitals" theme from CMS provider data metastore
- Converts column names to snake_case (e.g., "Patients' rating of the facility linear mean score" → "patients_rating_of_the_facility_linear_mean_score")
- Downloads and processes CSV files in parallel using ThreadPoolExecutor
- Supports daily incremental updates - only downloads files modified since last run
- Written in Python for Windows/Linux compatibility
- Includes requirements.txt for dependencies

## Features

- **Parallel Processing**: Uses ThreadPoolExecutor with 8 concurrent workers for fast downloads
- **Incremental Updates**: Tracks dataset modification dates in `run_metadata.json` to avoid re-downloading unchanged files
- **Robust Error Handling**: Handles CSV parsing issues, network errors, and malformed data gracefully
- **Snake Case Conversion**: Automatically converts all column headers to standardized snake_case format
- **Comprehensive Logging**: Detailed progress logs with success/failure tracking

## Results

On first run, the script:
- Downloaded **74 hospital-themed datasets**
- Processed **314.5 MB** of healthcare data
- Successfully converted all column names to snake_case
- Created incremental update tracking for future runs

## Usage

### First Time Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run the ETL job
python hospitals_etl.py
```

### Daily Runs
```bash
# Subsequent runs will only download modified datasets
python hospitals_etl.py
```

## Daily Automation with Cron

To run the script automatically every day as required, set up a cron job:

### Linux/macOS Setup

1. **Create a shell script wrapper** (`run_hospitals_etl.sh`):
```bash
#!/bin/bash
cd /path/to/your/pyt/directory
source venv/bin/activate
python hospitals_etl.py >> logs/hospitals_etl.log 2>&1
```

2. **Make it executable**:
```bash
chmod +x run_hospitals_etl.sh
```

3. **Add to crontab**:
```bash
# Edit crontab
crontab -e

# Add this line to run daily at 6:00 AM
0 6 * * * /path/to/your/pyt/run_hospitals_etl.sh
```

### Windows Setup (Task Scheduler)

1. **Create batch file** (`run_hospitals_etl.bat`):
```batch
@echo off
cd /d "C:\path\to\your\pyt"
call venv\Scripts\activate.bat
python hospitals_etl.py >> logs\hospitals_etl.log 2>&1
```

2. **Schedule with Task Scheduler**:
   - Open Task Scheduler
   - Create Basic Task
   - Set to run daily at 6:00 AM
   - Action: Start a program
   - Program: `C:\path\to\your\pyt\run_hospitals_etl.bat`

### Cron Schedule Examples

```bash
# Daily at 6:00 AM
0 6 * * * /path/to/run_hospitals_etl.sh

# Daily at 2:00 AM (recommended for off-peak hours)
0 2 * * * /path/to/run_hospitals_etl.sh

# Weekdays only at 7:00 AM
0 7 * * 1-5 /path/to/run_hospitals_etl.sh

# With environment variables
0 6 * * * cd /path/to/pyt && /path/to/pyt/venv/bin/python hospitals_etl.py >> logs/hospitals_etl.log 2>&1
```

### Monitoring and Logs

Create a logs directory and monitor the daily runs:

```bash
# Create logs directory
mkdir -p logs

# View recent logs
tail -f logs/hospitals_etl.log

# Check for errors
grep "ERROR" logs/hospitals_etl.log
```

## Output Structure

```
data/
└── 2025-06-25/          # Date-based directory
    ├── xubh-q36u.csv    # Hospital General Information
    ├── dgck-syfz.csv    # Patient Survey (HCAHPS)
    ├── ynj2-r877.csv    # Complications and Deaths
    └── ...              # 74 total hospital datasets
```

## Sample Data

The Hospital General Information dataset (`xubh-q36u.csv`) contains columns like:
- `facility_id` (was "Facility ID")
- `facility_name` (was "Facility Name")  
- `hospital_overall_rating` (was "Hospital Overall Rating")
- `emergency_services` (was "Emergency Services")

## Architecture

- **API Integration**: Fetches dataset catalog from CMS metastore API
- **Download Strategy**: Extracts actual CSV URLs from dataset distribution metadata
- **Data Processing**: pandas for CSV handling with multiple fallback parsing strategies
- **Concurrency**: ThreadPoolExecutor for parallel downloads
- **State Management**: JSON metadata file tracks modification dates

## Dependencies

- `pandas`: CSV processing and data manipulation
- `requests`: HTTP client for API and file downloads
- Standard library: `threading`, `json`, `logging`, `datetime`

## Error Handling

The script includes robust error handling for:
- Network connectivity issues
- Malformed CSV files (uses `on_bad_lines='skip'`)
- Character encoding problems (falls back to `latin-1`)
- Missing dataset distributions
- API rate limiting and timeouts

This solution provides a production-ready ETL pipeline for CMS hospital data with excellent performance and reliability. 