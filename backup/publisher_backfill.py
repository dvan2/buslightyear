# publisher.py
import requests, json, time
import gzip
import os
from google.cloud import pubsub_v1
from datetime import datetime
from zoneinfo import ZoneInfo
import argparse

parser = argparse.ArgumentParser(description="Publish backup bc data.")
parser.add_argument(
    "date",
    help="The target date to backfill file from (e.g: 2026-05-02)"
)
args = parser.parse_args()

TARGET_DATE = args.date
BACKUP_FILE = f"/home/davvan/bus_light_year/backup/backup_logs/breadcrumbs_{TARGET_DATE}.log.gz"

# configuration and variables
PROJECT_ID = 'plasma-winter-494417-a8'
TOPIC_ID   = 'bc_topic'
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

breadcrumb_count = 0 # running count of breadcrumbs iterated thru
published_count = 0

# function to ensure we sent the actual count
def record_success(future):
  global published_count
  published_count += 1

print(f'Reading from back up {BACKUP_FILE}')
print('(This may take several minutes)\n')

start_time     = time.time()    # start the clock
print(f'Start time: {start_time}')
try:
    if not os.path.exists(BACKUP_FILE):
        print(f"\nError: Could not find file {BACKUP_FILE}")
    else:
        with gzip.open(BACKUP_FILE, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                breadcrumb = json.loads(line)
                payload = json.dumps(breadcrumb).encode('utf-8')
                future = publisher.publish(topic_path, payload)
                future.add_done_callback(record_success)
                breadcrumb_count = breadcrumb_count + 1 # increment breadcrumb count
except Exception as e:
    print(f'Error occured: {e}')
finally:
  if breadcrumb_count > 0:
    # Here we can wait untill actual count is sent by publisher
    print(f"\nWaiting for {breadcrumb_count} background messages to finish publishing...")
    while published_count < breadcrumb_count:
      print(f'published:{published_count}')
      print(breadcrumb_count)
      time.sleep(0.5)

    final_time = time.time() # record end of entire publisher run
    timestamp = datetime.fromtimestamp(final_time, tz=ZoneInfo("America/Los_Angeles"))
    sentinel_timestamp = timestamp.strftime("%d%b%Y: %H:%M%S").upper()
    sentinel_data =  {
        'EVENT_NO_TRIP': 0, 'EVENT_NO_STOP': 0,
        'OPD_DATE': sentinel_timestamp, 'VEHICLE_ID': 0, 'METERS': breadcrumb_count,
        'ACT_TIME': 0.0, 'GPS_LONGITUDE': 0.0, 'GPS_LATITUDE': 0.0,
        'GPS_SATELLITES': 0.0, 'GPS_HDOP': 0.0
      }

    sentinel_payload = json.dumps(sentinel_data).encode('utf-8')
    sentinel_future = publisher.publish(topic_path, sentinel_payload)
    sentinel_future.result()
    print(f'\nSentinel message has been sent: {sentinel_future}')
    print(f'{sentinel_data}')

    # calculate summary statistics
    wall_time = final_time - start_time
    throughput_rate = float(breadcrumb_count) / wall_time

    # print summary statistics
    print(f'\n\nSummary Statistics:\n---------------------------------------')
    print(f'Breadcrumbs published: {breadcrumb_count}')
    print(f'Sentinel message sent at {sentinel_timestamp}')
    print(f'Wall time: {wall_time:.3f}s')
    print(f'Throughput: {throughput_rate:.3f} breadcrumbs per second')


