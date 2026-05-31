import subprocess
import os
import sys
import time
from dotenv import dotenv_values

def main():
    env_vars = dotenv_values("/home/nicjia/.env")
    
    # Extract credentials
    credentials = []
    
    # Key 1
    if "LOBSTER_API_KEY" in env_vars and "LOBSTER_API_SECRET" in env_vars:
        credentials.append((env_vars["LOBSTER_API_KEY"], env_vars["LOBSTER_API_SECRET"]))
        
    # Key 2
    if "LOBSTER_API_KEY_2" in env_vars and "LOBSTER_API_SECRET_2" in env_vars:
        credentials.append((env_vars["LOBSTER_API_KEY_2"], env_vars["LOBSTER_API_SECRET_2"]))
        
    # Key 3
    if "LOBSTER_API_KEY_3" in env_vars and "LOBSTER_API_SECRET_3" in env_vars:
        credentials.append((env_vars["LOBSTER_API_KEY_3"], env_vars["LOBSTER_API_SECRET_3"]))

    if not credentials:
        print("❌ Error: No LOBSTER API credentials found in .env")
        sys.exit(1)

    print(f"🚀 Starting {len(credentials)} concurrent LOBSTER workers...")
    print("Each worker will autonomously claim tickers using the shared lock system.\n")

    processes = []
    for i, (key, secret) in enumerate(credentials):
        worker_id = i + 1
        env = os.environ.copy()
        env["LOBSTER_API_KEY"] = key
        env["LOBSTER_API_SECRET"] = secret
        
        # Use python -u to prevent output buffering, and sed to prefix each line with the Worker ID
        cmd = f"python3 -u /home/nicjia/async_downloader.py 2>&1 | sed 's/^/[Worker {worker_id}] /'"
        
        p = subprocess.Popen(cmd, shell=True, env=env)
        processes.append(p)

    try:
        # Wait for all workers to finish naturally
        for p in processes:
            p.wait()
        print("\n🎉 All workers have completed successfully!")
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received. Shutting down all workers safely...")
        for p in processes:
            p.terminate()
        time.sleep(2)
        print("Done.")

if __name__ == "__main__":
    main()
