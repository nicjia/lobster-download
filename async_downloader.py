import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from dotenv import load_dotenv
from lobsterdata import LobsterClient

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
MAX_WORKERS = 10 
BASE_DIR = "./lobster_data"
LOG_FILE = "download_errors.csv"
SUCCESS_LOG = "download_success.txt"

# 3. SETUP
SYMBOLS = [t.strip().upper() for t in re.split(r'[\s,]+', RAW_TICKERS.strip()) if t.strip()]
load_dotenv()

import threading
log_lock = threading.Lock()

def log_error(symbol, start, end, error_msg):
    with log_lock:
        with open(LOG_FILE, "a") as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"{timestamp},{symbol},{start},{end},\"{error_msg}\"\n")
    print(f"❌ ERROR: {error_msg}")

def log_success(msg):
    with log_lock:
        with open(SUCCESS_LOG, "a") as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"[{timestamp}] {msg}\n")
    print(f"✅ {msg}")

def process_chunk(job):
    """Function to be run by each thread worker"""
    symbol, start, end = job
    ticker_dir = os.path.join(BASE_DIR, symbol)
    os.makedirs(ticker_dir, exist_ok=True)
    
    # Each thread needs its own client instance to avoid state collisions
    client = LobsterClient(
        api_key=os.getenv("LOBSTER_API_KEY"),
        api_secret=os.getenv("LOBSTER_API_SECRET"),
        is_pilot=True
    )
    
    try:
        # Submit
        res = client.submit_request(symbol, start, end, LEVEL, EXCHANGE)
        req_id = res["data"]["request_id"]
        
        # Poll
        while True:
            req = client.get_request(req_id)
            if req.get("status") == "finished":
                break
            elif req.get("status") == "error":
                raise Exception("Server Error")
            time.sleep(30)
            
        # Download & Cleanup
        filepath = client.download_request(req_id, download_dir=ticker_dir)
        client.delete_request(req_id)
        success_msg = f"Done: {symbol} {start} to {end} -> {filepath}"
        log_success(success_msg)
        return f"✅ {success_msg}"
    except Exception as e:
        log_error(symbol, start, end, str(e))
        return f"❌ Failed: {symbol} {start} | {e}"

def generate_jobs():
    jobs = []
    for s in SYMBOLS:
        curr = START_DATE
        while curr <= END_DATE:
            next_d = min(curr + timedelta(days=30), END_DATE)
            jobs.append((s, curr.strftime("%Y-%m-%d"), next_d.strftime("%Y-%m-%d")))
            curr = next_d + timedelta(days=1)
    return jobs

if __name__ == "__main__":
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w") as f:
            f.write("Timestamp,Ticker,Start_Date,End_Date,Error_Message\n")
            
    jobs = generate_jobs()
    print(f"🚀 Starting parallel download of {len(jobs)} chunks with {MAX_WORKERS} workers.")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for result in executor.map(process_chunk, jobs):
            print(result)