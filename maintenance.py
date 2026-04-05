# maintenance.py
"""
Checks for updates to required packages, installs them if needed, and manages log file size.
Works on desktop and Android (Termux/Pydroid 3).
"""
import subprocess
import sys
import logging
import os

# --- CONFIG ---
REQUIRED_PACKAGES = [
    'requests',
    'schedule',
    'mysql-connector-python',
    'fastapi',
    'uvicorn',
    'email-validator',
]
LOG_FILE = 'maintenance.log'
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB

# --- LOGGING SETUP ---
def setup_logger():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > MAX_LOG_SIZE:
        try:
            os.remove(LOG_FILE)
        except Exception as e:
            print(f"Failed to delete log file: {e}")
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

# --- UPDATE PACKAGES ---
def update_package(pkg):
    try:
        logging.info(f"Checking for updates: {pkg}")
        result = subprocess.run([
            sys.executable, '-m', 'pip', 'install', '--upgrade', pkg
        ], capture_output=True, text=True)
        if result.returncode == 0:
            logging.info(f"Updated {pkg}: {result.stdout.strip()}")
        else:
            logging.error(f"Error updating {pkg}: {result.stderr.strip()}")
    except Exception as e:
        logging.error(f"Exception updating {pkg}: {e}")

# --- MAIN ---
def main():
    setup_logger()
    logging.info("Starting maintenance script.")
    for pkg in REQUIRED_PACKAGES:
        update_package(pkg)
    logging.info("Maintenance complete.")

if __name__ == "__main__":
    main()
