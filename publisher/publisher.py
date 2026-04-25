# publisher.py
import pandas as pd
import requests, json, time
from google.cloud import pubsub_v1
from datetime import date
from datetime import datetime
from zoneinfo import ZoneInfo


# extract Dazzle vehicles to python list
dazzle_df = pd.read_csv('VehicleGroupsIDs-NEW.csv')
#dazzle_list = dazzle_df['Dazzle'].to_list()
dazzle_list = list([2989,3004,3014,3024])



# configuration and variables
PROJECT_ID = 'de-project-bus-lightyear'
TOPIC_ID   = 'bc_topic'
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
failed_ids        = [] # holds vehicle IDs which failed to fetch
breadcrumb_count = 0 # running count of breadcrumbs iterated thru


print(f'Fetching data for {len(dazzle_list)} vehicles...')
print('(This may take several minutes)\n')

# loop through each vehicle ID in Dazzle column
start_time     = time.time()    # start the clock
print(f'Start time: {start_time}')
for index, vehicle_id in enumerate(dazzle_list):
  print(f'Fetching data for vehicle {vehicle_id} ({index+1}/{len(dazzle_list)})...')
  url = f'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}'
  # attempt fetching JSON for current {vehicle_id}
  try:
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data    = resp.json()
    for index, breadcrumb in enumerate(data):
      payload = json.dumps(breadcrumb).encode('utf-8') # package breadcrumb as bytes-like objects
      future = publisher.publish(topic_path, payload) # publish breadcrumb to cloud
      breadcrumb_count = breadcrumb_count + 1 # increment breadcrumb count
  except Exception as e:
    failed_ids.append(vehicle_id)
  except requests.exceptions.HTTPError as e:
    code = e.response.status_code
    msg  = e.response.json().get('message', 'unknown error')
    print(f'HTTP {code} error for {vehicle_id}: {msg}')
    if code == 429:
          print('API rate limit (too many requests!)\n')
  except requests.exceptions.ConnectionError:
    print('Internet connection issues!')
  except requests.exceptions.Timeout:
    print(f'Request timeout for {vehicle_id}.')

final_time = time.time() # record end of entire publisher run

'''
Send sentinel message
I implemented the sentinel message as a breadcrumb with most attributes set to zero. OPD_DATE contains the full actual timestamp it was sent.
Meters contains the total number of breadcrumbs sent. The listener program can check if the VEHICLE_ID is zero, and if so, knows this breadcrumb is actually the sentinel.
'''
# create timestamp string
timestamp = datetime.fromtimestamp(final_time, tz=ZoneInfo("America/Los_Angeles"))
todays_day_of_month = timestamp.strftime("%d")
todays_month_name   = timestamp.strftime("%b").upper()
todays_year         = timestamp.strftime("%Y")
todays_hour         = timestamp.strftime("%H")
todays_minute       = timestamp.strftime("%M")
todays_second       = timestamp.strftime("%S")
sentinel_timestamp  = todays_day_of_month + todays_month_name + todays_year + ': ' + todays_hour + ': ' + todays_minute + ': ' + todays_second
# create sentinel record
sentinel_data =  {
        'EVENT_NO_TRIP': 0, 'EVENT_NO_STOP': 0,
        'OPD_DATE': sentinel_timestamp, 'VEHICLE_ID': 0, 'METERS': breadcrumb_count,
        'ACT_TIME': 0.0, 'GPS_LONGITUDE': 0.0, 'GPS_LATITUDE': 0.0,
        'GPS_SATELLITES': 0.0, 'GPS_HDOP': 0.0
        }
# package sentinel payload
sentinel_payload = json.dumps(sentinel_data).encode('utf-8')
sentinel_future = publisher.publish(topic_path, sentinel_payload)
sentinel_future.result
print(f'\nSentinel message has been sent: {sentinel_future}')
print(f'{sentinel_data}')


# calculate summary statistics
wall_time = final_time - start_time
received_vehicles = len(dazzle_list) - len(failed_ids)
throughput_rate = float(breadcrumb_count) / wall_time


# print summary statistics
print(f'\n\nSummary Statistics:\n---------------------------------------')
print(f'Vehicles with available records: {received_vehicles}')
print(f'Breadcrumbs published: {breadcrumb_count}')
print(f'Sentinel message sent at {sentinel_timestamp}')
print(f'Wall time: {wall_time:.3f}s')
print(f'Throughput: {throughput_rate:.3f} breadcrumbs per second')