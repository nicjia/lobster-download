import time
import os
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from lobsterdata import LobsterClient

# ---------------------------------------------------------
# 1. PASTE YOUR TICKERS HERE
# You can paste directly from Excel/Sheets. 
# Spaces, newlines, and commas will be automatically cleaned.
# ---------------------------------------------------------
RAW_TICKERS = """
MMYT, APLD, ODP, LTBR, LMB, INTR, TRI, WULF, VRTX, IDCC, 
CDNA, TSLQ, SPNS, RING, MEDP, FUTU, TSMX, ALGN, TCOM, NTCT, 
GRAB, ETSY, APA
"""

# --- 2. CONFIGURATION ---
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2026, 2, 14)
LEVEL = 0
EXCHANGE = "NASDAQ"

BASE_DOWNLOAD_DIR = "./lobster_data"
LOG_FILE = "download_errors.csv"
SUCCESS_LOG = "download_success.txt"
MAX_WAIT_TIME = 2700  # 45 minutes timeout

# --- 3. PARSE TICKERS ---
# This regex splits by any whitespace (newlines, spaces, tabs) or commas
SYMBOLS = [t.strip().upper() for t in re.split(r'[\s,]+', RAW_TICKERS.strip()) if t.strip()]

# --- 4. AUTHENTICATION ---
load_dotenv()
my_api_key = os.getenv("LOBSTER_API_KEY")
my_api_secret = os.getenv("LOBSTER_API_SECRET")

if not my_api_key or not my_api_secret:
    print("❌ CRITICAL ERROR: API keys missing. Check your .env file.")
    exit(1)

client = LobsterClient(
    api_key=my_api_key,
    api_secret=my_api_secret,
    is_pilot=True
)

# --- 5. LOGGING SYSTEM ---
def log_error(symbol, start, end, error_msg):
    with open(LOG_FILE, "a") as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"{timestamp},{symbol},{start},{end},\"{error_msg}\"\n")
    print(f"❌ ERROR: {error_msg}")

def log_success(msg):
    with open(SUCCESS_LOG, "a") as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"[{timestamp}] {msg}\n")
    print(f"✅ {msg}")

# --- 6. EXECUTION PIPELINE ---
print(f"🚀 Loaded {len(SYMBOLS)} unique tickers. Starting pipeline...\n")
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w") as f:
        f.write("Timestamp,Ticker,Start_Date,End_Date,Error_Message\n")

for symbol in SYMBOLS:
    # Create a specific folder for this ticker so all its chunks stay organized together
    ticker_dir = os.path.join(BASE_DOWNLOAD_DIR, symbol)
    os.makedirs(ticker_dir, exist_ok=True)
    
    current_start = START_DATE
    
    while current_start <= END_DATE:
        current_end = current_start + timedelta(days=30)
        if current_end > END_DATE:
            current_end = END_DATE

        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")

        print(f"\n--- Processing {symbol}: {start_str} to {end_str} ---")
        
        # --- SUBMIT ---
        try:
            res = client.submit_request(
                symbol=symbol,
                start_date=start_str,
                end_date=end_str,
                level=LEVEL,
                exchange=EXCHANGE
            )
            req_id = res["data"]["request_id"]
        except Exception as e:
            log_error(symbol, start_str, end_str, f"Failed to submit: {e}")
            current_start = current_end + timedelta(days=1)
            time.sleep(5)
            continue

        # --- WAIT FOR BUILD ---
        ready = False
        wait_start = time.time()
        
        while not ready:
            if time.time() - wait_start > MAX_WAIT_TIME:
                log_error(symbol, start_str, end_str, f"Timeout on req {req_id}")
                try: 
                    client.delete_request(req_id)
                except: 
                    pass
                break 
            
            try:
                requests = client.list_requests()
                for req in requests:
                    rid = req.get("request_id") or req.get("id")
                    if str(rid) == str(req_id):
                        status = req.get("status")
                        if status == "finished" and (req.get("request_data_size", 0) > 0):
                            ready = True
                        elif status == "error":
                            log_error(symbol, start_str, end_str, "Server error building request")
                            ready = "ERROR"
            except Exception as e:
                print(f"⚠️ Status check failed: {e}. Retrying...")
            
            if ready is not True: 
                time.sleep(30)

        # --- DOWNLOAD & DELETE ---
        if ready == True:
            try:
                print(f"⬇️ Downloading to {ticker_dir}...")
                filepath = client.download_request(req_id, download_dir=ticker_dir)
                log_success(f"Saved {symbol} chunk -> {filepath}")
                
                # Immediately purge from LOBSTER server
                client.delete_request(req_id)
                time.sleep(3)
                
            except Exception as e:
                log_error(symbol, start_str, end_str, f"Failed download/delete: {e}")
        
        # Advance the timeline
        current_start = current_end + timedelta(days=1)
        time.sleep(4) # Rate limit buffer

print("\n🎉 ALL TICKERS PROCESSED. Check download_errors.csv for any dropped chunks.")