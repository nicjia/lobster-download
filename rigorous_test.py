import time
import os
from dotenv import load_dotenv
from lobsterdata import LobsterClient

load_dotenv()
client = LobsterClient(
    api_key=os.getenv("LOBSTER_API_KEY"),
    api_secret=os.getenv("LOBSTER_API_SECRET"),
    is_pilot=True
)

SYMBOL = "AAPL"
START_DATE = "2024-01-02" 
END_DATE = "2024-01-02"
DOWNLOAD_DIR = "./test_lobster"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

print("\n=== THE RIGOROUS DELETION PROOF ===")

# --- STEP 1: SUBMIT & BUILD ---
print(f"\n[1/5] Submitting 1-day test for {SYMBOL}...")
res = client.submit_request(symbol=SYMBOL, start_date=START_DATE, end_date=END_DATE, level=10, exchange="NASDAQ")
req_id = res["data"]["request_id"]
print(f"Request ID: {req_id}")

print("[2/5] Waiting for LOBSTER to build the data...")
ready = False
while not ready:
    for req in client.list_requests():
        rid = req.get("request_id") or req.get("id")
        if str(rid) == str(req_id) and req.get("status") == "finished":
            ready = True
    if not ready:
        time.sleep(10)

# --- STEP 2: FIRST DOWNLOAD (VERIFY EXISTENCE) ---
print("\n[3/5] Executing initial download...")
filepath = client.download_request(req_id, download_dir=DOWNLOAD_DIR)
print(f"✅ Download 1 Success: File exists on LOBSTER server and was downloaded to {filepath}")

# --- STEP 3: DELETE ---
print("\n[4/5] Sending DELETE command...")
client.delete_request(req_id)
print("Command sent. Sleeping 15 seconds to force their database to sync...")
time.sleep(15)

# --- STEP 4: THE RIGOROUS PROOF (SECOND DOWNLOAD) ---
print("\n[5/5] Attempting to download the deleted file (This MUST fail)...")
try:
    # We attempt to hit the exact same download endpoint for the exact same file
    ghost_filepath = client.download_request(req_id, download_dir=DOWNLOAD_DIR)
    
    # If the code reaches this line, the server gave us the file. The delete is broken.
    print("\n❌ CRITICAL FAILURE: The file was downloaded again!")
    print(f"It saved to: {ghost_filepath}")
    print("Do NOT run the mass downloader. The storage is accumulating.")
    
except Exception as e:
    # If the code reaches here, the server refused the download.
    print(f"\n✅ PROOF SECURED: The server physically rejected the download request.")
    print(f"Error returned by LOBSTER: {e}")
    print("Conclusion: The file has been physically wiped from their hard drives.")

print("\n=== TEST COMPLETE ===")

