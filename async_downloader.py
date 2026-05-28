import os
import re
import time
import glob
import sys
import csv
import shutil
import subprocess
import zipfile
from datetime import datetime, timedelta
from dotenv import load_dotenv
from lobsterdata import LobsterClient
import threading
import concurrent.futures
import socket
import fcntl
import tempfile

# Prevent the script from hanging indefinitely if the LOBSTER API server stops responding
socket.setdefaulttimeout(30)

# 2. CONFIG
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2026, 2, 14)
LEVEL = 0
EXCHANGE = "NASDAQ"
BASE_DIR = "./lobster_data"
LOG_FILE = "download_errors.csv"
PENDING_FILE = "/lobster/pending_tickers.csv"
LOBSTER_TARGET = "/lobster"
LOCK_EXPIRY_SECONDS = 7200  # 2 hours
BATCH_SIZE = 5

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
        with open(PENDING_FILE, "r+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.seek(0)
            lines = f.readlines()
            f.seek(0)
            f.truncate()
            for line in lines:
                if not line.strip(): continue
                if line.strip().split(',')[0].strip().upper() != symbol:
                    f.write(line)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        
        shared_locks_dir = os.path.join(tempfile.gettempdir(), "lobster_locks")
        lock_path = os.path.join(shared_locks_dir, symbol)
        if os.path.exists(lock_path):
            try:
                ts_file = os.path.join(lock_path, "timestamp")
                if os.path.exists(ts_file):
                    os.remove(ts_file)
                os.rmdir(lock_path)
            except OSError:
                pass
                
        shared_completed_dir = os.path.join(tempfile.gettempdir(), "lobster_completed")
        if not os.path.exists(shared_completed_dir):
            try:
                os.makedirs(shared_completed_dir, exist_ok=True)
                os.chmod(shared_completed_dir, 0o777)
            except Exception: pass
            
        completed_path = os.path.join(shared_completed_dir, symbol)
        try:
            os.makedirs(completed_path, exist_ok=True)
            os.chmod(completed_path, 0o777)
        except Exception: pass
        
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

def organize_chunk(zf, sym):
    """Take a single downloaded zip file and move daily .7z files into /lobster/YYYY/YYYYMMDD/TICKER.7z format."""
    if not os.path.exists(zf):
        return
        
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(prefix=f"lobster_org_{sym}_")
        subprocess.run(["unzip", "-q", "-o", zf, "-d", temp_dir],
                      check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        
        for archive in glob.glob(os.path.join(temp_dir, "*.7z")):
            filename = os.path.basename(archive)
            # Match: TICKER_YYYY-MM-DD.7z (dashes in date)
            match = re.match(r'^([A-Za-z0-9]+)_(\d{4})-(\d{2})-(\d{2})\.7z$', filename)
            if not match:
                continue
            
            ticker = match.group(1)
            yyyy = match.group(2)
            mm = match.group(3)
            dd = match.group(4)
            
            target_dir = os.path.join(LOBSTER_TARGET, yyyy, f"{yyyy}{mm}{dd}")
            target_path = os.path.join(target_dir, f"{ticker}.7z")
            
            if not os.path.exists(target_dir):
                subprocess.run(["sg", "lobster", "-c", f"mkdir -p {target_dir}"],
                              check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            subprocess.run(["sg", "lobster", "-c", f"cp {archive} {target_path}"],
                          check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
        # Delete source zip after organizing successfully
        os.remove(zf)
    except subprocess.CalledProcessError as e:
        stderr_msg = e.stderr.decode('utf-8', errors='replace') if e.stderr else str(e)
        if "empty" not in stderr_msg.lower() and os.path.getsize(zf) >= 100:
            print(f"[organizer] ⚠️ Error processing {os.path.basename(zf)}: {stderr_msg}")
    except Exception as e:
        print(f"[organizer] ⚠️ Error processing {os.path.basename(zf)}: {e}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

def combine_and_remove(sym):
    try:
        ticker_dir = os.path.join(BASE_DIR, sym)
        if os.path.exists(ticker_dir):
            shutil.rmtree(ticker_dir, ignore_errors=True)
        remove_ticker_from_csv(sym)
        print(f"✅ Successfully finished and removed {sym} from {PENDING_FILE}")
    except Exception as e:
        print(f"❌ Error finalizing {sym}: {e}")

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

    shared_locks_dir = os.path.join(tempfile.gettempdir(), "lobster_locks")
    if not os.path.exists(shared_locks_dir):
        try:
            os.makedirs(shared_locks_dir, exist_ok=True)
            os.chmod(shared_locks_dir, 0o777)
        except Exception as e:
            print(f"⚠️ Could not create or chmod shared locks dir: {e}")

    shared_completed_dir = os.path.join(tempfile.gettempdir(), "lobster_completed")

    claimed_symbols = []
    for s in SYMBOLS:
        # If someone else completed it, catch up our local CSV and skip
        if os.path.exists(os.path.join(shared_completed_dir, s)):
            remove_ticker_from_csv(s)
            continue
            
        lock_path = os.path.join(shared_locks_dir, s)
        try:
            os.mkdir(lock_path)
            # Write a timestamp file so we can detect stale locks
            try:
                ts_file = os.path.join(lock_path, "timestamp")
                with open(ts_file, "w") as tf:
                    tf.write(str(time.time()))
                os.chmod(ts_file, 0o666)
            except Exception:
                pass
            claimed_symbols.append(s)
            print(f"🔒 Claimed {s} for processing.")
            if len(claimed_symbols) >= BATCH_SIZE:
                break
        except FileExistsError:
            # Check if the lock is stale (older than LOCK_EXPIRY_SECONDS)
            try:
                ts_file = os.path.join(lock_path, "timestamp")
                if os.path.exists(ts_file):
                    with open(ts_file, "r") as tf:
                        lock_time = float(tf.read().strip())
                    if time.time() - lock_time > LOCK_EXPIRY_SECONDS:
                        print(f"⏰ Lock for {s} is stale ({(time.time() - lock_time)/3600:.1f}h old). Reclaiming...")
                        # Remove stale lock and reclaim
                        try:
                            os.remove(ts_file)
                            os.rmdir(lock_path)
                            os.mkdir(lock_path)
                            with open(ts_file, "w") as tf:
                                tf.write(str(time.time()))
                            os.chmod(ts_file, 0o666)
                            claimed_symbols.append(s)
                            print(f"🔒 Reclaimed stale lock for {s}.")
                            if len(claimed_symbols) >= BATCH_SIZE:
                                break
                        except Exception as e:
                            print(f"⚠️ Could not reclaim stale lock for {s}: {e}")
                    else:
                        continue  # Lock is fresh, skip
                else:
                    continue  # No timestamp file, respect the lock
            except Exception:
                continue  # On any error reading lock, skip
            
    if not claimed_symbols:
        print("⏳ All pending tickers are currently locked by other workers. Waiting...")
        return [], {}

    for s in claimed_symbols:
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
            # Fetch all requests to manually find downloadable AND empty ones
            all_reqs = client.list_requests()
            ready = []
            empty = []
            
            for req in all_reqs:
                if req.get("status") == "finished" and not req.get("request_file_deleted", False):
                    if req.get("request_data_size", 0) > 0:
                        ready.append(req)
                    else:
                        empty.append(req)
            
            if empty:
                print(f"🗑️ Found {len(empty)} empty requests (e.g. pre-IPO dates). Clearing them...")
                for req in empty:
                    rid = req.get("request_id") or req.get("id")
                    sym = req.get("symbol")
                    start = req.get("start_date")
                    end = req.get("end_date")
                    try:
                        client.delete_request(rid)
                        print(f"🗑️ Deleted empty Request ID {rid} ({sym}) from server.")
                        with open("download_success.txt", "a") as f:
                            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Saved {sym} chunk -> EMPTY_{sym}_{start}_{end}_0.zip\n")
                            
                        # Decrement job count for this ticker
                        with count_lock:
                            if sym in pending_counts:
                                pending_counts[sym] -= 1
                                if pending_counts[sym] <= 0:
                                    print(f"🎉 All chunks for {sym} processed! Queuing background organize...")
                                    combine_executor.submit(combine_and_remove, sym)
                                    del pending_counts[sym]
                    except Exception as e:
                        print(f"❌ Failed to clear empty request {rid}: {e}")
                        
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
                            
                        # Organize this chunk immediately and delete the zip
                        organize_chunk(filepath, sym)
                            
                        # Decrement job count for this ticker
                        with count_lock:
                            if sym in pending_counts:
                                pending_counts[sym] -= 1
                                if pending_counts[sym] <= 0:
                                    print(f"🎉 All chunks for {sym} downloaded! Queuing background organize...")
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
    import argparse
    parser = argparse.ArgumentParser(description="Async LOBSTER Downloader")
    parser.add_argument("--batch-size", type=int, default=5, help="Number of tickers to claim at once")
    parser.add_argument("--base-dir", type=str, default="./lobster_data", help="Base directory to save data")
    args = parser.parse_args()

    BASE_DIR = args.base_dir
    BATCH_SIZE = args.batch_size

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("Timestamp,Ticker,Start_Date,End_Date,Error_Message\n")
            
    client = LobsterClient(
        api_key=os.getenv("LOBSTER_API_KEY"),
        api_secret=os.getenv("LOBSTER_API_SECRET"),
        is_pilot=True
    )
    
    while True:
        jobs, pending_counts = generate_jobs(client)
        if not jobs and not pending_counts:
            print("✅ No jobs claimed. Either everything is done or waiting for locks. Retrying in 60s...")
            SYMBOLS = get_pending_tickers()
            if not SYMBOLS:
                print("🎉 All tickers completed! Exiting.")
                break
            time.sleep(60)
            continue
            
        print(f"🚀 Ready to process {len(jobs)} total chunks across {len(pending_counts)} active tickers.")
        
        submit_thread = threading.Thread(target=submit_worker, args=(jobs, client))
        poll_thread = threading.Thread(target=poll_and_download_worker, args=(client, pending_counts), daemon=True)
        
        poll_thread.start()
        submit_thread.start()
        
        submit_thread.join()
        print("✅ Submissions complete. Waiting for final downloads in this batch...")
        
        while True:
            try:
                with count_lock:
                    active = len(pending_counts)
                if active == 0:
                    print("✅ Batch complete. Starting next batch...")
                    break
                print(f"⏳ Waiting for {active} tickers to finish downloading/combining...")
                time.sleep(30)
            except Exception as e:
                time.sleep(30)