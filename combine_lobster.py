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
        # Skip empty zips (under 100 bytes) when calculating timeline
        if os.path.getsize(z) < 100:
            continue
            
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

    print(f"[{ticker}] Detected timeline: {start_date} to {end_date} (Level {level})")

    temp_dir = f"./temp_{ticker}"
    extract_dir = os.path.join(temp_dir, "extracted")

    
    # Clean up temp dir if it exists from a previous failed run
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        
    os.makedirs(extract_dir, exist_ok=True)

    print(f"[{ticker}] Found {len(zips)} zip files. Unzipping monthly files using multiple cores...")
    def extract_zip(z):
        try:
            # -n instead of -o to not overwrite, and capture output
            subprocess.run(["unzip", "-q", "-o", z, "-d", temp_dir], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            # Check if empty zip (unzip exits with 1 for empty or warning)
            if b"zipfile is empty" in e.stderr or b"Empty" in e.stdout or os.path.getsize(z) < 100:
                print(f"[{ticker}] Skipping empty or invalid zip file: {os.path.basename(z)}")
            else:
                print(f"[{ticker}] Error extracting {os.path.basename(z)}: {e.stderr.decode('utf-8')}")
                # Don't fail the whole process if one zip is bad

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        list(executor.map(extract_zip, zips))

    daily_7z = glob.glob(os.path.join(temp_dir, "*.7z"))
    print(f"[{ticker}] Found {len(daily_7z)} daily .7z files. Extracting CSVs using multiple cores...")
    
    # Suppress output of 7z to avoid console spam
    def extract_7z(d7z):
        subprocess.run(["7z", "e", d7z, f"-o{extract_dir}", "-y"], check=True, stdout=subprocess.DEVNULL)
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        list(executor.map(extract_7z, daily_7z))
        
    final_archive = f"{ticker}_{start_date}_{end_date}_{level}.7z"
    if os.path.exists(final_archive):
        os.remove(final_archive)
        
    print(f"[{ticker}] Compressing all CSVs into {final_archive} (Fast Multi-Core mode)...")
    
    # Zip everything in the extracted folder into the final .7z archive
    # Use -mmt=on for multithreading, and -mx=3 for faster compression
    compress_cmd = ["7z", "a", "-t7z", "-mx=3", "-mmt=on", final_archive, f"{extract_dir}/*"]
    subprocess.run(compress_cmd, check=True, stdout=subprocess.DEVNULL)
    
    print(f"[{ticker}] Cleaning up temporary files...")
    shutil.rmtree(temp_dir)
    print(f"🎉 Done! Created {final_archive}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python combine_lobster.py <TICKER>")
        print("Example: python combine_lobster.py MMYT")
        sys.exit(1)
    
    ticker = sys.argv[1].upper()
    process_ticker(ticker)
