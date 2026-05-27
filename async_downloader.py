import os
import re
import time
import glob
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from lobsterdata import LobsterClient
import threading
import concurrent.futures
import socket

# Prevent the script from hanging indefinitely if the LOBSTER API server stops responding
socket.setdefaulttimeout(30)

# 1. PASTE TICKERS
RAW_TICKERS = """
APLD, ODP, LTBR, LMB, INTR, TRI, WULF, VRTX, IDCC, 
CDNA, TSLQ, SPNS, RING, MEDP, FUTU, TSMX, ALGN, TCOM, NTCT, 
GRAB, ETSY, APA
"""

# 2. CONFIG
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2026, 2, 14)
LEVEL = 0
EXCHANGE = "NASDAQ"
BASE_DIR = "./lobster_data"
LOG_FILE = "download_errors.csv"

# 3. SETUP
SYMBOLS = [t.strip().upper() for t in re.split(r'[\s,]+', RAW_TICKERS.strip()) if t.strip()]
load_dotenv()

def get_downloaded_from_log():
    downloaded = set()
    try:
        with open("download_success.txt", "r") as f:
            for line in f:
                # Extracts SYMBOL, start_date, end_date from strings like:
                # ..._MMYT_2022-01-01_2022-01-31_0.zip
                match = re.search(r'_([A-Z]+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_', line)
                if match:
                    downloaded.add((match.group(1), match.group(2), match.group(3)))
    except FileNotFoundError:
        pass
    return downloaded

DOWNLOADED_SET = get_downloaded_from_log()

def is_already_downloaded(symbol, start_str, end_str):
    if (symbol, start_str, end_str) in DOWNLOADED_SET:
        return True
        
    ticker_dir = os.path.join(BASE_DIR, symbol)
    if not os.path.exists(ticker_dir):
        return False
    # Format typically: R4521_TSLQ_2024-04-17_2024-05-17_0.zip
    pattern = os.path.join(ticker_dir, f"*_{symbol}_{start_str}_{end_str}_*.zip")
    matches = glob.glob(pattern)
    for match in matches:
        if os.path.getsize(match) > 100: # Ensure it's not an empty broken file
            return True
    return False

def generate_jobs(client):
    jobs = []
    
    # Prevents data leakage / duplicate submissions by checking the server first
    print("🔍 Fetching existing requests from server to prevent duplicates...")
    try:
        existing_reqs = client.list_requests()
        # Keep track of what the server is already processing or has finished (but not deleted)
        active_on_server = {
            (r["symbol"], r["start_date"], r["end_date"]) 
            for r in existing_reqs
            if r.get("status") not in ("error", "failed", "deleted")
        }
    except Exception as e:
        print(f"⚠️ Could not fetch existing requests: {e}")
        active_on_server = set()
    
    for s in SYMBOLS:
        curr = START_DATE
        while curr <= END_DATE:
            next_d = min(curr + timedelta(days=30), END_DATE)
            s_str = curr.strftime("%Y-%m-%d")
            e_str = next_d.strftime("%Y-%m-%d")
            
            # Check local disk
            if not is_already_downloaded(s, s_str, e_str):
                # Check if it's already on the server
                if (s, s_str, e_str) not in active_on_server:
                    jobs.append((s, s_str, e_str))
                else:
                    print(f"⏭️ Skipping {s} {s_str} to {e_str}, already processing on server.")
            curr = next_d + timedelta(days=1)
    return jobs

def submit_worker(jobs, client):
    """Submits requests sequentially, obeying the rate limit."""
    print(f"🚀 Submitter Thread starting. {len(jobs)} chunks to request.")
    for i, (symbol, start, end) in enumerate(jobs):
        for attempt in range(3):
            try:
                print(f"📤 [{i+1}/{len(jobs)}] Submitting {symbol} {start} to {end} (Attempt {attempt+1})...")
                client.submit_request(symbol, start, end, LEVEL, EXCHANGE)
                time.sleep(4.5) # Guard against 20req/min rate limit (increased to 4.5s)
                break # Success, move to next job
            except Exception as e:
                print(f"⚠️ Failed to submit {symbol} {start}-{end} (Attempt {attempt+1}/3): {e}")
                time.sleep(10)
                if attempt == 2: # Last attempt
                    print(f"❌ Giving up on {symbol} {start}-{end}.")
                    with open(LOG_FILE, "a") as f:
                        f.write(f"{datetime.now()},{symbol},{start},{end},\"{e}\"\n")
    print("✅ Submitter Thread finished all submissions.")

def poll_and_download_worker(client):
    """Polls the server periodically and downloads ready files."""
    print("🚀 Poller/Downloader Thread starting.")
    while True:
        try:
            # Check what's ready
            ready = client.list_downloadable_requests()
            if ready:
                print(f"📥 Found {len(ready)} requests ready for download.")
                
                # Download them concurrently (max 4 workers to save memory/disk IO)
                def download_req(req):
                    rid = req.get("request_id") or req.get("id")
                    sym = req.get("symbol")
                    print(f"🔄 Downloading Request ID {rid} ({sym})...")
                    ticker_dir = os.path.join(BASE_DIR, sym)
                    os.makedirs(ticker_dir, exist_ok=True)
                    try:
                        filepath = client.download_request(rid, download_dir=ticker_dir)
                        print(f"✅ Saved: {filepath}")
                        client.delete_request(rid)
                        print(f"🗑️ Deleted ID {rid} from server.")
                        with open("download_success.txt", "a") as f:
                            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Saved {sym} chunk -> {filepath}\n")
                    except Exception as e:
                        print(f"❌ Download failed for ID {rid}: {e}")
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    executor.map(download_req, ready)
            
            # Sleep before polling again
            time.sleep(60)
        except Exception as e:
            print(f"⚠️ Poller encountered network error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("Timestamp,Ticker,Start_Date,End_Date,Error_Message\n")
            
    client = LobsterClient(
        api_key=os.getenv("LOBSTER_API_KEY"),
        api_secret=os.getenv("LOBSTER_API_SECRET"),
        is_pilot=True
    )
    
    jobs = generate_jobs(client)
    if not jobs:
        print("✅ All jobs are already downloaded!")
        exit(0)
        
    print(f"🚀 Ready to process {len(jobs)} chunks.")
    
    submit_thread = threading.Thread(target=submit_worker, args=(jobs, client))
    poll_thread = threading.Thread(target=poll_and_download_worker, args=(client,), daemon=True)
    
    poll_thread.start()
    submit_thread.start()
    
    submit_thread.join()
    print("✅ Submissions complete. Waiting for final downloads...")
    
    while True:
        try:
            alive = client.list_alive_requests()
            if not alive:
                print("✅ All requests downloaded and cleared. Exiting.")
                break
            print(f"⏳ Waiting for {len(alive)} requests to finish processing on server...")
            time.sleep(30)
        except Exception as e:
            time.sleep(30)
            
    print("\n✅ All downloads completed. Starting auto-combination process...")
    import combine_lobster
    for s in SYMBOLS:
        combine_lobster.process_ticker(s)