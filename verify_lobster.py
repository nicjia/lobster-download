import os
import re
from datetime import datetime
from collections import defaultdict

LOBSTER_DIR = "/lobster"
LOG_FILE = "/home/nicjia/download_success.txt"
OUTPUT_REPORT = "/home/nicjia/verification_report.txt"

def main():
    print("🔍 Scanning /lobster directory for all valid trading days...")
    
    # 1. Gather all "trading days" from the /lobster directory
    # We assume if a folder exists (e.g. /lobster/2022/20220103), the market was open.
    trading_days = []
    if os.path.exists(LOBSTER_DIR):
        for year in os.listdir(LOBSTER_DIR):
            year_path = os.path.join(LOBSTER_DIR, year)
            if os.path.isdir(year_path) and year.isdigit():
                for day in os.listdir(year_path):
                    if day.isdigit() and len(day) == 8:
                        trading_days.append(day)
                        
    trading_days.sort()
    if not trading_days:
        print("❌ No trading days found in /lobster. Exiting.")
        return

    print(f"✅ Found {len(trading_days)} total trading days on disk.")

    def get_trading_days_in_range(start_str, end_str):
        start_dt = datetime.strptime(start_str, "%Y-%m-%d")
        end_dt = datetime.strptime(end_str, "%Y-%m-%d")
        start_s = start_dt.strftime("%Y%m%d")
        end_s = end_dt.strftime("%Y%m%d")
        return [d for d in trading_days if start_s <= d <= end_s]

    print(f"📖 Parsing {LOG_FILE}...")
    
    # 2. Parse download_success.txt
    chunks_by_ticker = defaultdict(list)
    
    if not os.path.exists(LOG_FILE):
        print(f"❌ Could not find {LOG_FILE}")
        return

    with open(LOG_FILE, "r") as f:
        for line in f:
            # We skip EMPTY chunks since LOBSTER explicitly told us there is no data for them
            if "EMPTY_" in line:
                continue
                
            # Match standard chunks: R123_AAPL_2022-01-01_2022-01-31
            m = re.search(r"_([A-Z]+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_", line)
            if m:
                sym, start, end = m.groups()
                chunks_by_ticker[sym].append((start, end))

    print(f"✅ Parsed valid chunks for {len(chunks_by_ticker)} unique tickers.")
    print("🔎 Verifying all extracted files against downloaded chunks (this may take a minute)...")

    # 3. Verify
    missing_files = defaultdict(list)
    total_expected = 0
    total_found = 0

    for sym, chunks in chunks_by_ticker.items():
        for start, end in chunks:
            expected_days = get_trading_days_in_range(start, end)
            total_expected += len(expected_days)
            
            for day in expected_days:
                year = day[:4]
                target_path = os.path.join(LOBSTER_DIR, year, day, f"{sym}.7z")
                
                if os.path.exists(target_path):
                    total_found += 1
                else:
                    missing_files[sym].append(day)

    # 4. Write out the report
    with open(OUTPUT_REPORT, "w") as f:
        f.write("========================================================\n")
        f.write("              LOBSTER DATA VERIFICATION REPORT          \n")
        f.write("========================================================\n\n")
        f.write(f"Total Tickers Checked: {len(chunks_by_ticker)}\n")
        f.write(f"Total Expected Daily Files: {total_expected}\n")
        f.write(f"Total Daily Files Found: {total_found}\n")
        f.write(f"Total Daily Files Missing: {total_expected - total_found}\n\n")
        
        if not missing_files:
            f.write("🎉 PERFECT MATCH! Every single downloaded chunk has contiguous daily data!\n")
        else:
            f.write("⚠️ WARNING: The following tickers are missing data for certain trading days:\n")
            f.write("(Note: This can legitimately happen if a stock was halted, delisted, or simply didn't trade that day)\n\n")
            
            for sym, days in sorted(missing_files.items()):
                f.write(f"[{sym}] Missing {len(days)} days: {', '.join(days)}\n")

    print(f"\n📊 Verification complete!")
    print(f"Expected: {total_expected} files")
    print(f"Found: {total_found} files")
    print(f"Missing: {total_expected - total_found} files")
    print(f"Report saved to: {OUTPUT_REPORT}")

if __name__ == "__main__":
    main()
