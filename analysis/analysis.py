# analysis.py
# Paste your analysis.py code here.
import requests, json, time
from google.cloud import pubsub_v1
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo


#---Data Structures---------------------------------------------------------
breadcrumb_count = 0
earliest_bc = None
latest_bc = None
wall_clock_time = None
unique_vehicles = set()
unique_trips = set()


#---Helper Functions-------------------------------------------------------
def calc_breadcrumb_timestamp(opd_date, act_time):
        '''
        Each breadcrumb has it's datetime value split between two fields: OPD_DATE (string representing the correct day at midnight) >
        '''
        the_date = datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S')
        the_date = the_date.date()
        time_elapsed = timedelta(seconds=act_time)
        proper_datetime=datetime.combine(the_date, datetime.min.time())+ time_elapsed
        return proper_datetime # return type is datetime object


def format_time(raw_timestamp):
    '''
    Generic function to convert a raw time.time() to a better readable time format
    in Pacific Time Zone (America/LosAngeles).
    '''

    if raw_timestamp is None:
        return "Not time"

    formated_time = datetime.fromtimestamp(raw_timestamp, tz=ZoneInfo("America/Los_Angeles"))
    return formated_time.strftime('%Y-%m-%d %H:%M:%S')



#---Congiguration----------------------------------------------------------
PROJECT_ID       = 'de-project-bus-lightyear'
SUBSCRIPTION_ID  = 'analysis_sub'
TIMEOUT_SECONDS  = 500   ##### delete this later (see instructions above)
subscriber = pubsub_v1.SubscriberClient()
sub_path   = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

#---Callback Function------------------------------------------------------
def callback(message):
        global breadcrumb_count, unique_vehicles, unique_trips, earliest_bc, latest_bc, wall_clock_time
        message.ack()
        breadcrumb = json.loads(message.data.decode('utf-8')) # one breadcrumb

        # analysis happens here
        # Check for Sentinel (VEHICLE_ID = 0)
        if breadcrumb['VEHICLE_ID'] == 0:
          sentinel_time = time.time()

          elapsed_time = sentinel_time - wall_clock_time
          throughput = breadcrumb_count / elapsed_time

        #---Summary Statistics-----------------------------------------------------
          print("\nSentinel Recieved")
          print("Summary Statistics:")
          print(f"First message received: {format_time(wall_clock_time)}")
          print(f"Unique Vehicle IDs: {len(unique_vehicles)}")
          print(f"Earliest Breadcrumb from OPD and ACT: {earliest_bc}")
          print(f"Latest Breadcrumb from OPD and ACT: {latest_bc}")
          print(f"Unique Trip IDs: {len(unique_trips)}")
          print(f"Total Breadcrumbs Received: {breadcrumb_count}")
          print(f"Sentinel Received Time: {format_time(sentinel_time)}")
          print(f"Ellapsed Time: {elapsed_time:.3f}s")
          print(f"Throughput: {throughput:.3f} msg/s")

        #----Reset Data Structure(s)------------------------------------------------
          breadcrumb_count = 0
          unique_vehicles.clear()
          unique_trips.clear()
          earliest_bc = None
          latest_bc = None
          wall_clock_time = None
          sentinel_time = None

        else:
          if wall_clock_time is None: #start timer when first breadcrumb recieved
              wall_clock_time = time.time()
              print(f"First breadcrumb received at {format_time(wall_clock_time)}")

          breadcrumb_count = breadcrumb_count + 1

          if breadcrumb_count % 10000 == 0:
              print(f"Collected {breadcrumb_count} so far")


          unique_vehicles.add(breadcrumb['VEHICLE_ID'])
          unique_trips.add(breadcrumb['EVENT_NO_TRIP'])

          raw_opd = breadcrumb['OPD_DATE']
          raw_act = breadcrumb['ACT_TIME']

          current_bc_time = calc_breadcrumb_timestamp(raw_opd, raw_act)
          if latest_bc is None:
              latest_bc = current_bc_time
          if earliest_bc is None:
            earliest_bc = current_bc_time
          else:
            if current_bc_time < earliest_bc:
              earliest_bc = current_bc_time
            if current_bc_time > latest_bc:
              latest_bc = current_bc_time



#---Listening--------------------------------------------------------------
streaming_pull = subscriber.subscribe(sub_path, callback=callback)

with subscriber:
        try:
                streaming_pull.result()
        except Exception:
                streaming_pull.cancel()
                streaming_pull.result()

