# SETUP
add a .env file in main folder
```bash
LOBSTER_API_KEY=
LOBSTER_API_SECRET=
is_pilot=TRUE
```

set up a venv with all the right installations
Install uv
```bash
python3 -m pip install --user uv
```
create venv
```bash
# Create the environment using Python 3.13
~/.local/bin/uv venv --python 3.13 .venv

# Activate the environment
source .venv/bin/activate

# Install required packages (Must use `uv` because the environment is managed)
~/.local/bin/uv pip install lobsterdata anyio python-dotenv
```

# running it
run with python async_downloader.py - you need to go in and manually set the tickers, you can just copy from the spreadsheet and paste it in there, .split handles the new lines

Start it in a tmux with 
```bash
tmux new -s lobster_dl
source .venv/bin/activate
python async_downloader.py
```
then you can detach with ```ctrl + b``` then ```d```

check with tmux attach -t lobster_dl


downloads chunks of 30 days into .zip files for each stock in /lobster_data
each chunk has a .7z file for every single day

turn into .7z
```bash
python3 combine_lobster.py STOCK_TICKER
```
or 
```bash
python3 combine_lobster.py
```
to process all stocks in the directory that don't have .7z files yet



