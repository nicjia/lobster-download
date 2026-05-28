import os
import glob
import re
import shutil
import tempfile
import subprocess
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = "/home/nicjia"
TARGET_BASE = "/lobster"

def process_archive(archive_path):
    filename = os.path.basename(archive_path)
    # Match pattern like OKTA_2022-01-01_2026-02-14_0.7z
    match = re.match(r'^([A-Za-z0-9]+)_.+\.7z$', filename)
    if not match:
        print(f"Skipping {filename}: Does not match expected format.")
        return
    
    ticker = match.group(1)
    temp_dir = tempfile.mkdtemp(prefix=f"reorg_{ticker}_")
    
    try:
        print(f"[{ticker}] Extracting {filename} to temporary directory...")
        subprocess.run(["7z", "x", archive_path, f"-o{temp_dir}", "-y"], check=True, stdout=subprocess.DEVNULL)
        
        # Group CSVs by date
        csv_files = glob.glob(os.path.join(temp_dir, "*.csv"))
        date_groups = {}
        for csv_path in csv_files:
            csv_filename = os.path.basename(csv_path)
            # Expecting TICKER_YYYY-MM-DD_orderbook_0.csv
            date_match = re.search(r'_(\d{4})-(\d{2})-(\d{2})_', csv_filename)
            if date_match:
                yyyy, mm, dd = date_match.groups()
                date_str = f"{yyyy}{mm}{dd}"
                if date_str not in date_groups:
                    date_groups[date_str] = []
                date_groups[date_str].append(csv_path)
        
        if not date_groups:
            print(f"[{ticker}] No CSV files with dates found.")
            return

        print(f"[{ticker}] Found {len(date_groups)} dates. Creating daily archives...")
        
        for date_str, files in date_groups.items():
            yyyy = date_str[:4]
            target_dir = os.path.join(TARGET_BASE, yyyy, date_str)
            target_file = os.path.join(target_dir, f"{ticker}.7z")
            
            # Create a daily archive in the temp dir
            daily_archive = os.path.join(temp_dir, f"{ticker}_{date_str}.7z")
            subprocess.run(["7z", "a", "-t7z", "-mx=3", daily_archive] + files, check=True, stdout=subprocess.DEVNULL)
            
            # Create target directory and move
            subprocess.run(["sg", "lobster", "-c", f"mkdir -p {target_dir}"], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["sg", "lobster", "-c", f"cp {daily_archive} {target_file}"], check=True, stdout=subprocess.DEVNULL)
            
        print(f"[{ticker}] ✅ Reorganized successfully into {TARGET_BASE}.")
        
    except Exception as e:
        print(f"[{ticker}] ❌ Error: {e}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    archives = glob.glob(os.path.join(BASE_DIR, "*_*_*_*.7z"))
    if not archives:
        print("No combined .7z archives found in", BASE_DIR)
        exit(0)
    
    print(f"Found {len(archives)} existing archives to reorganize.")
    # Process sequentially to avoid blowing up disk I/O, or use limited workers
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(process_archive, archives))
    
    print("🎉 All existing archives have been reorganized.")
