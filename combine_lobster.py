import os
import glob
import subprocess
import shutil
import sys
import re
from concurrent.futures import ThreadPoolExecutor

def process_ticker(ticker):
    base_dir = f"./lobster_data/{ticker}"
    if not os.path.exists(base_dir):
        print(f"Directory {base_dir} does not exist.")
        return

    zips = glob.glob(os.path.join(base_dir, "*.zip"))
    if not zips:
        print(f"No zip files found in {base_dir}.")
        return

    # Extract all dates to find global start and end
    start_dates = []
    end_dates = []
    levels = set()

    for z in zips:
        filename = os.path.basename(z)
        # We NO LONGER skip empty zips when calculating timeline to ensure 
        # the final filename reflects the entire requested range.
            
        # Assuming format like R3881_MMYT_2022-01-01_2022-01-31_0.zip
        match = re.search(rf'{ticker}_(\d{{4}}-\d{{2}}-\d{{2}})_(\d{{4}}-\d{{2}}-\d{{2}})_(\d+)\.zip', filename)
        if match:
            start_dates.append(match.group(1))
            end_dates.append(match.group(2))
            levels.add(match.group(3))

    if not start_dates or not end_dates:
        print(f"[{ticker}] No valid data (all zip files are empty or unparseable).")
        return

    start_date = min(start_dates)
    end_date = max(end_dates)
    level = next(iter(levels)) if levels else "0"
    
    final_archive = f"{ticker}_{start_date}_{end_date}_{level}.7z"
    if os.path.exists(final_archive):
        print(f"[{ticker}] '{final_archive}' already exists! Skipping combination process.")
        old_archives = glob.glob(f"{ticker}_*_*_*.7z")
        for old_arch in old_archives:
            if old_arch != final_archive:
                print(f"[{ticker}] Deleting older obsolete archive: {old_arch}")
                os.remove(old_arch)
        return

    print(f"[{ticker}] Detected timeline: {start_date} to {end_date} (Level {level})")

    temp_dir = f"./temp_{ticker}"
    extract_dir = os.path.join(temp_dir, "extracted")

    
    # Clean up temp dir if it exists from a previous failed run
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        
    os.makedirs(extract_dir, exist_ok=True)

    print(f"[{ticker}] Found {len(zips)} zip files. Unzipping monthly files...")
    def extract_zip(z):
        # Isolate extraction to avoid race conditions
        base_name = os.path.basename(z).replace('.zip', '')
        unique_dir = os.path.join(temp_dir, "zips", base_name)
        os.makedirs(unique_dir, exist_ok=True)
        try:
            subprocess.run(["unzip", "-q", "-o", z, "-d", unique_dir], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            if b"zipfile is empty" in e.stderr or b"Empty" in e.stdout or os.path.getsize(z) < 100:
                print(f"[{ticker}] Skipping empty or invalid zip file: {os.path.basename(z)}")
            else:
                print(f"[{ticker}] Error extracting {os.path.basename(z)}: {e.stderr.decode('utf-8')}")

    # Limit workers to prevent I/O bottleneck and threading errors
    workers = min(4, os.cpu_count() or 1)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(extract_zip, zips))

    daily_7z = []
    for root, _, files in os.walk(os.path.join(temp_dir, "zips")):
        for file in files:
            if file.endswith('.7z'):
                daily_7z.append(os.path.join(root, file))

    print(f"[{ticker}] Found {len(daily_7z)} daily .7z files. Extracting CSVs...")
    
    def extract_7z(d7z):
        subprocess.run(["7z", "e", d7z, f"-o{extract_dir}", "-y"], check=True, stdout=subprocess.DEVNULL)
        
    with ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(extract_7z, daily_7z))
        
    print(f"[{ticker}] Compressing all CSVs into {final_archive} (Fast Multi-Core mode)...")
    
    # Zip everything in the extracted folder into the final .7z archive
    # Use -mmt=on for multithreading, and -mx=3 for faster compression
    compress_cmd = ["7z", "a", "-t7z", "-mx=3", "-mmt=on", final_archive, f"{extract_dir}/*"]
    subprocess.run(compress_cmd, check=True, stdout=subprocess.DEVNULL)
    
    # Remove older incomplete .7z archives for this ticker if they exist
    old_archives = glob.glob(f"{ticker}_*_*_*.7z")
    for old_arch in old_archives:
        if old_arch != final_archive:
            print(f"[{ticker}] Deleting older obsolete archive: {old_arch}")
            os.remove(old_arch)

    print(f"[{ticker}] Cleaning up temporary files...")
    shutil.rmtree(temp_dir)
    print(f"🎉 Done! Created {final_archive}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No ticker specified. Scanning for all tickers in ./lobster_data...")
        if not os.path.exists("./lobster_data"):
            print("No lobster_data directory found.")
            sys.exit(1)
        
        tickers = [d for d in os.listdir("./lobster_data") if os.path.isdir(os.path.join(".", "lobster_data", d))]
        for t in tickers:
            process_ticker(t)
    else:
        ticker = sys.argv[1].upper()
        process_ticker(ticker)
