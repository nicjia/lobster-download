import os
import re
import time
import glob
import sys
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
from lobsterdata import LobsterClient
import threading
import concurrent.futures
import socket

# Prevent the script from hanging indefinitely if the LOBSTER API server stops responding
socket.setdefaulttimeout(30)

# 2. CONFIG
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2026, 2, 14)
LEVEL = 0
EXCHANGE = "NASDAQ"
BASE_DIR = "./lobster_data"
LOG_FILE = "download_errors.csv"
PENDING_FILE = "pending_tickers.csv"

# 3. SETUP
load_dotenv()

count_lock = threading.Lock()
# Limit simultaneous combinations so we don't crash the server's CPU/RAM
combine_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2) 

def get_pending_tickers():
    tickers = []
    try:
        with open(PENDING_FILE, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row: continue
                t = row[0].strip().upper()
                if t and t != "TICKER":
                    tickers.append(t)
    except FileNotFoundError:
        pass
    return list(dict.fromkeys(tickers)) # remove duplicates

def remove_ticker_from_csv(symbol):
    try:
        with count_lock:
            tickers = []
            with open(PENDING_FILE, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    if row:
                        t = row[0].strip()
                        if t.upper() != symbol:
                            tickers.append(t)
            with open(PENDING_FILE, "w", newline="") as f:
                writer = csv.writer(f)
                for t in tickers:
                    writer.writerow([t])
    except Exception as e:
        print(f"⚠️ Error updating {PENDING_FILE}: {e}")

def get_downloaded_from_log():
    downloaded = set()
    try:
        with open("download_success.txt", "r") as f:
            for line in f:
                match = re.search(r'_([A-Z]+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_', line)
                if match:
                    downloaded.add((match.group(1), match.group(2), match.group(3)))
    except FileNotFoundError:
        pass
    return downloaded

DOWNLOADED_SET = get_downloaded_from_log()

def get_downloaded_from_disk(symbol):
    downloaded = set()
    ticker_dir = os.path.join(BASE_DIR, symbol)
    if os.path.exists(ticker_dir):
        for f in os.listdir(ticker_dir):
            if f.endswith('.zip') and os.path.getsize(os.path.join(ticker_dir, f)) > 100:
                match = re.search(rf'_{symbol}_(\d{{4}}-\d{{2}}-\d{{2}})_(\d{{4}}-\d{{2}}-\d{{2}})_', f)
                if match:
                    downloaded.add((symbol, match.group(1), match.group(2)))
    return downloaded

def combine_and_remove(sym):
    import combine_lobster
    try:
        combine_lobster.process_ticker(sym)
        remove_ticker_from_csv(sym)
        print(f"✅ Successfully combined and removed {sym} from {PENDING_FILE}")
    except Exception as e:
        print(f"❌ Error combining {sym}: {e}")

def generate_jobs(client):
    jobs = []
    pending_counts = {}
    
    print("🔍 Fetching existing requests from server to prevent duplicates...")
    try:
        existing_reqs = client.list_requests()
        active_on_server = {
            (r["symbol"], r["start_date"], r["end_date"]) 
            for r in existing_reqs
            if r.get("status") not in ("error", "failed", "deleted")
        }
    except Exception as e:
        print(f"⚠️ Could not fetch existing requests: {e}")
        active_on_server = set()
    
    SYMBOLS = get_pending_tickers()
    if not SYMBOLS:
        print(f"✅ {PENDING_FILE} is empty or not found.")
        return [], {}

    for s in SYMBOLS:
        disk_downloaded = get_downloaded_from_disk(s)
        curr = START_DATE
        job_count = 0
        
        while curr <= END_DATE:
            next_d = min(curr + timedelta(days=30), END_DATE)
            s_str = curr.strftime("%Y-%m-%d")
            e_str = next_d.strftime("%Y-%m-%d")
            
            chunk_tuple = (s, s_str, e_str)
            if chunk_tuple not in DOWNLOADED_SET and chunk_tuple not in disk_downloaded:
                job_count += 1
                if chunk_tuple not in active_on_server:
                    jobs.append(chunk_tuple)
                else:
                    print(f"⏭️ Skipping {s} {s_str} to {e_str}, already processing on server.")
            curr = next_d + timedelta(days=1)
            
        if job_count > 0:
            pending_counts[s] = job_count
        else:
            print(f"✅ {s} is already fully downloaded. Triggering combine...")
            combine_executor.submit(combine_and_remove, s)
            
    return jobs, pending_counts

def submit_worker(jobs, client):
    print(f"🚀 Submitter Thread starting. {len(jobs)} chunks to request.")
    for i, (symbol, start, end) in enumerate(jobs):
        for attempt in range(3):
            try:
                print(f"📤 [{i+1}/{len(jobs)}] Submitting {symbol} {start} to {end} (Attempt {attempt+1})...")
                client.submit_request(symbol, start, end, LEVEL, EXCHANGE)
                time.sleep(4.5)
                break
            except Exception as e:
                print(f"⚠️ Failed to submit {symbol} {start}-{end} (Attempt {attempt+1}/3): {e}")
                time.sleep(10)
                if attempt == 2:
                    print(f"❌ Giving up on {symbol} {start}-{end}.")
                    with open(LOG_FILE, "a") as f:
                        f.write(f"{datetime.now()},{symbol},{start},{end},\"{e}\"\n")
    print("✅ Submitter Thread finished all submissions.")

def poll_and_download_worker(client, pending_counts):
    print("🚀 Poller/Downloader Thread starting.")
    while True:
        try:
            ready = client.list_downloadable_requests()
            if ready:
                print(f"📥 Found {len(ready)} requests ready for download.")
                
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
                            
                        # Decrement job count for this ticker
                        with count_lock:
                            if sym in pending_counts:
                                pending_counts[sym] -= 1
                                if pending_counts[sym] <= 0:
                                    print(f"🎉 All chunks for {sym} downloaded! Queuing background combine...")
                                    combine_executor.submit(combine_and_remove, sym)
                                    del pending_counts[sym]
                                    
                    except Exception as e:
                        print(f"❌ Download failed for ID {rid}: {e}")
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    executor.map(download_req, ready)
            
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
    
    jobs, pending_counts = generate_jobs(client)
    if not jobs and not pending_counts:
        print("✅ No pending jobs or active downloads.")
        exit(0)
        
    print(f"🚀 Ready to process {len(jobs)} total chunks across {len(pending_counts)} active tickers.")
    
    submit_thread = threading.Thread(target=submit_worker, args=(jobs, client))
    poll_thread = threading.Thread(target=poll_and_download_worker, args=(client, pending_counts), daemon=True)
    
    poll_thread.start()
    submit_thread.start()
    
    submit_thread.join()
    print("✅ Submissions complete. Waiting for final downloads...")
    
    while True:
        try:
            with count_lock:
                active = len(pending_counts)
            if active == 0:
                print("✅ All requests downloaded and combined. Exiting.")
                break
            print(f"⏳ Waiting for {active} tickers to finish downloading/combining...")
            time.sleep(30)
        except Exception as e:
            time.sleep(30)