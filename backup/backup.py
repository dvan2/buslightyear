# backup.py
import os
import json
import time
import gzip
import shutil
import threading
from google.cloud import pubsub_v1
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ID       = 'plasma-winter-494417-a8'
SUBSCRIPTION_ID  = 'backup_sub'
subscriber       = pubsub_v1.SubscriberClient()
sub_path         = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

LOG_DIR = 'backup/backup_logs'
# make a directory if it doesn't exists for log files
os.makedirs(LOG_DIR, exist_ok=True)

breadcrumb_count = 0
expected_count   = None
first_bc_time    = None
sentinel_time    = None
unique_vehicles  = set()
current_filename = None

file_lock = threading.Lock()

def format_time(raw_timestamp):
    if raw_timestamp is None:
        return "No time"
    formated_time = datetime.fromtimestamp(raw_timestamp, tz=ZoneInfo("America/Los_Angeles"))
    return formated_time.strftime('%Y-%m-%d %H:%M:%S')

def compress_file(filename):
    gz_filename = f"{filename}.gz"
    with open(filename, 'rb') as f_in:
        with gzip.open(gz_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    # remove regular file
    os.remove(filename)
    return gz_filename

#--- Callback Function ----------------------------------------------------
def callback(message):
    global breadcrumb_count, expected_count, first_bc_time, sentinel_time
    global unique_vehicles, current_filename

    message.ack()
    payload = message.data.decode('utf-8')
    breadcrumb = json.loads(payload)

    # Thread lock helps with different threads opening same file
    with file_lock:
        
        if first_bc_time is None:
            # Use the time we recieved first bc to name the file
            first_bc_time = time.time()
            date_str = datetime.fromtimestamp(first_bc_time, tz=ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d')
            current_filename = f"breadcrumbs_{date_str}.log"
            # create file to be in the directory
            current_filename = os.path.join(LOG_DIR, f"breadcrumbs_{date_str}.log")
            print(f"[{format_time(first_bc_time)}] Writing to file: {current_filename}")

        # If we hit Sentinel message, get expected message count
        if breadcrumb.get('VEHICLE_ID') == 0:
            expected_count = breadcrumb.get('METERS')
            sentinel_time = time.time()
        else:
            breadcrumb_count += 1
            unique_vehicles.add(breadcrumb.get('VEHICLE_ID'))
            
            # Append raw JSON string to the log file safely
            with open(current_filename, 'a') as f:
                f.write(payload + '\n')

        # After we reach Sentinel message, check if expected bc count from Sentinel matches bc count
        if expected_count is not None and breadcrumb_count == expected_count:
            
            # size of file before compression
            file_size_bytes = os.path.getsize(current_filename) if os.path.exists(current_filename) else 0
            
            # Run compression
            compressed_file = compress_file(current_filename)
            compression_time = time.time()
            
            elapsed_time = compression_time - first_bc_time
            throughput = breadcrumb_count / elapsed_time if elapsed_time > 0 else 0

            #--- Summary Statistics ---------------------------------------
            print("\n--- Daily Backup Complete & Compressed ---")
            print(f"First breadcrumb received: {format_time(first_bc_time)}")
            print(f"Total breadcrumbs received: {breadcrumb_count}")
            print(f"Raw file size (pre-compression): {file_size_bytes} bytes")
            print(f"Unique vehicles: {len(unique_vehicles)}")
            print(f"File compressed at: {format_time(compression_time)} ({compressed_file})")
            print(f"Elapsed Time: {elapsed_time:.3f}s")
            print(f"Throughput: {throughput:.3f} msg/s")

            #--- Reset Data Structures -----------------------
            breadcrumb_count = 0
            expected_count = None
            first_bc_time = None
            sentinel_time = None
            unique_vehicles.clear()
            current_filename = None

streaming_pull = subscriber.subscribe(sub_path, callback=callback)
print(f"Backup Service is listening quietly on {SUBSCRIPTION_ID} . . .")

with subscriber:
    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        streaming_pull.result()