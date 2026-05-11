# analysis.py
# Paste your analysis.py code here.
import requests, json, time
from google.cloud import pubsub_v1
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import logging
import threading
import os

MAX_LATITUDE = 90
MAX_LONGITUDE = 180
PDX_LAT_MIN, PDX_LAT_MAX = 45.0, 46.0
PDX_LON_MIN, PDX_LON_MAX = -123.5, -122.0
MAX_VEHICLE_ID = 9999999

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def validate_batch(batch_df) -> pd.DataFrame:
    """
    Validates a batch of breadcrumb records.
    Creates columns for invalid records with IS_VALID = False
    """

    # Create Columns to flag invalid bc
    batch_df['IS_VALID'] = True
    batch_df['VIOLATION_REASON'] = ""

    trip_counts = batch_df.groupby(['VEHICLE_ID', 'OPD_DATE', 'ACT_TIME'])['EVENT_NO_TRIP'].transform('nunique')

    rules = [
        #---ASSERTION 1---[LIMIT]  GPS_LATITUDE must be non-null and in [-90, 90]-----
        (batch_df['GPS_LATITUDE'].isna() | (batch_df['GPS_LATITUDE'] > MAX_LATITUDE) | (batch_df['GPS_LATITUDE'] < -MAX_LATITUDE),
        'A1: GPS_LATITUDE is null or out of range [-90, 90] | '),

        #---ASSERTION 2---[LIMIT]  GPS_LONGITUDE must be non-null and in [-180, 180]-----
        (batch_df['GPS_LONGITUDE'].isna() | (batch_df['GPS_LONGITUDE'] > MAX_LATITUDE) | (batch_df['GPS_LONGITUDE'] < -MAX_LONGITUDE),
         'A2: GPS_LONGITUDE is null or out of range [-180, 180] | '),

        #---ASSERTION 3---[EXISTENCE]  OPD_DATE must exist-----
        (batch_df['OPD_DATE'].isna() | (batch_df['OPD_DATE'] == ''),
         'A3: OPD_DATE is null | '),
        
        #---ASSERTION 4---[LIMIT]  vehicle_id must non-null, greater than 0, and less than 9999999-----
        (batch_df['VEHICLE_ID'].isna() | (batch_df['VEHICLE_ID'] <= 0) | (batch_df['VEHICLE_ID'] >= MAX_VEHICLE_ID),
         'A4: VEHICLE_ID is null or out of range | '),
        
        #---ASSERTION 5---[INTER-RECORD]  A vehicle can't be on two different trips at once-----
        (trip_counts > 1,
         'A5: A vehicle can\'t be on two different trips at once | '),
         
        #---ASSERTION 6---[INTRA-RECORD / LIMIT]  GPS coordinates must fall within PDX area lat/long limits -----
        ((batch_df['GPS_LATITUDE'] < PDX_LAT_MAX) | (batch_df['GPS_LATITUDE'] > PDX_LAT_MIN) | 
         (batch_df['GPS_LONGITUDE'] < PDX_LON_MAX) | (batch_df['GPS_LONGITUDE'] > PDX_LON_MIN),
         'A6: GPS coordinates are outside of PDX area | '),
         
        #---ASSERTION 7---[EXISTENCE]  Following must be non-null: EVENT_NO_TRIP, EVENT_NO_STOP, METERS, ACT_TIME
        (batch_df['EVENT_NO_TRIP'].isna() | batch_df['EVENT_NO_STOP'].isna() | 
         batch_df['METERS'].isna() | batch_df['ACT_TIME'].isna(),
         'A7: Expecting non-null fields, null value found | ')
    ]

    for mask, reason in rules:
        batch_df.loc[mask, 'IS_VALID'] = False
        batch_df.loc[mask, 'VIOLATION_REASON'] += reason

    violations_df =batch_df[batch_df['IS_VALID'] == False]

    for row in violations_df.itertuples():
        logging.warning("VALIDATION VIOLATION - [%s] | record: %s", row.VIOLATION_REASON, row)

    # Return the data frame with violation flags
    return batch_df


#---Helper Functions-------------------------------------------------------
def calc_breadcrumb_timestamp(opd_date, act_time):
        '''
        Each breadcrumb has it's datetime value split between two fields: OPD_DATE (string representing the correct day at midnight) >
        '''
        the_date = datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S').date()
        time_elapsed = timedelta(seconds=act_time)
        return datetime.combine(the_date, datetime.min.time())+ time_elapsed


def format_time(raw_timestamp):
    if raw_timestamp is None:
        return "Not time"
    formated_time = datetime.fromtimestamp(raw_timestamp, tz=ZoneInfo("America/Los_Angeles"))
    return formated_time.strftime('%Y-%m-%d %H:%M:%S')


#---Configuration----------------------------------------------------------
PROJECT_ID       = 'plasma-winter-494417-a8'
SUBSCRIPTION_ID  = 'analysis_sub'
subscriber = pubsub_v1.SubscriberClient()
sub_path   = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)


def write_invalid_records(invalid_records, run_date=None):
    """
    Write invalid breadcrumb records to a dated JSON file.

    Parameters
    ----------
    invalid_records : list of dict
        Each dict should have a 'record' key (the original data)
        and a 'violations' key (list of assertion violation messages).
    run_date : str, optional
        Date string in YYYY-MM-DD format. Defaults to today.
    """
    if run_date is None:
        run_date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d')

    filename = f"/home/davvan/bus_light_year/analysis/invalid_data/invalid_data_{run_date}.json"

    os.makedirs("/home/davvan/invalid_data", exist_ok=True)
    invalid_records.to_json(filename, orient='records', lines=True, mode='a')

# ----- Bread Crumbs Processor Class
class BreadcrumbProcessor:
    def __init__(self, project_id, subscription_id, batch_limit=1000):
        #Configuration
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.batch_limit = batch_limit

        # Data Structures
        self.message_batch = []
        self.batch_lock = threading.Lock()

        self.breadcrumb_count = 0
        self.expected_count = 0
        self.earliest_bc = None
        self.latest_bc = None
        self.wall_clock_time = None
        self.sentinel_time = None
        self.unique_vehicles = set()
        self.unique_trips = set()

    def reset_datastructure(self):
        self.breadcrumb_count = 0
        self.expected_count = None
        self.unique_vehicles.clear()
        self.unique_trips.clear()
        self.earliest_bc = None
        self.latest_bc = None
        self.wall_clock_time = None
        self.sentinel_time = None
    
    def process_pandas_batch(self):
        """"
        Process current pandas batch of invalid and valid
        """
        if len(self.message_batch) == 0:
            return
        
        df = pd.DataFrame(self.message_batch)
        df_validations = validate_batch(df)

        good_df = df_validations[df_validations['IS_VALID'] == True].copy()
        bad_df = df_validations[df_validations['IS_VALID'] == False].copy()

        if not bad_df.empty:
            write_invalid_records(bad_df)
        
        self.message_batch.clear()


    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates new column timestamp, speed, drops some columns

        Args:
            df (pd.DataFrame): breadcrumbs rows to transform

        """        
        opd_date = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')
        act_time = pd.to_timedelta(df['ACT_TIME'], unit='s')
        df['timestamp'] = opd_date + act_time
        df = df.drop(columns=['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP', 'OPD_DATE', 'ACT_TIME'])

        # --- Calculate Speed ----
        df = df.sort_values(by=['EVENT_NO_TRIP', 'timestamp'])

        dif_meters = df.groupby('EVENT_NO_TRIP')['METERS'].diff()
        dif_secs = df.groupby('EVENT_NO_TRIP')['timestamp'].diff().dt.total_seconds()

        df['speed'] = dif_meters/dif_secs

        df['speed'] = df['speed'].fillna(0.0)

        # -- Rename Columns for db ---
        df = df.rename(columns={
            'EVENT_NO_TRIP': 'trip_id',
            'VEHICLE_ID': 'vehicle_id',
            'GPS_LONGITUDE': 'longitude',
            'GPS_LATITUDE': 'latitude'
        })

        return df



   
    def callback(self, message):
        message.ack()
        breadcrumb = json.loads(message.data.decode('utf-8'))

        # analysis happens here
        # Check for Sentinel (VEHICLE_ID = 0)
        if breadcrumb['VEHICLE_ID'] == 0:
          self.expected_count = breadcrumb['METERS']
          self.sentinel_time = time.time()

          # final batch, use thread locks for safety
          with self.batch_lock:
            self.process_pandas_batch()
        else:
            if self.wall_clock_time is None: #start timer when first breadcrumb recieved
                self.wall_clock_time = time.time()
                print(f"First breadcrumb received at {format_time(self.wall_clock_time)}")

            self.breadcrumb_count += 1
            self.unique_vehicles.add(breadcrumb['VEHICLE_ID'])
            self.unique_trips.add(breadcrumb['EVENT_NO_TRIP'])
            current_bc_time = calc_breadcrumb_timestamp(breadcrumb['OPD_DATE'], breadcrumb['ACT_TIME'])

            # Keep track of latest bc and earliest bc
            if self.latest_bc is None or current_bc_time > self.latest_bc:
                self.latest_bc = current_bc_time
            if self.earliest_bc is None or current_bc_time < self.earliest_bc:
                self.earliest_bc = current_bc_time

            with self.batch_lock:
                self.message_batch.append(breadcrumb)
                if len(self.message_batch) >= self.batch_limit:
                    self.process_pandas_batch()

        # --- SUMMARY STATISTICS ---
        with self.batch_lock:
            # After recieving Sentinel, check for expected count and actual count 
            if self.expected_count is not None and self.breadcrumb_count == self.expected_count:
                elapsed_time = self.sentinel_time - self.wall_clock_time
                throughput = self.breadcrumb_count / elapsed_time

                print("\nSentinel Received")
                print("Summary Statistics:")
                print(f"First message received: {format_time(self.wall_clock_time)}")
                print(f"Unique Vehicle IDs: {len(self.unique_vehicles)}")
                print(f"Earliest Breadcrumb from OPD and ACT: {self.earliest_bc}")
                print(f"Latest Breadcrumb from OPD and ACT: {self.latest_bc}")
                print(f"Unique Trip IDs: {len(self.unique_trips)}")
                print(f"Total Breadcrumbs Received: {self.breadcrumb_count}")
                print(f"Sentinel Received Time: {format_time(self.sentinel_time)}")
                print(f"Elapsed Time: {elapsed_time:.3f}s")
                print(f"Throughput: {throughput:.3f} msg/s")

                self.reset_datastructure()
    
    def start_listening(self):
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(self.project_id, self.subscription_id)

        streaming_pull = subscriber.subscribe(sub_path, callback=self.callback)
        print(f"Listening for messages on {self.subscription_id} at {format_time(time.time())}...")

        with subscriber:
            try:
                streaming_pull.result()
            except Exception as e:
                streaming_pull.cancel()
                streaming_pull.result()


if __name__ == "__main__":
    PROJECT_ID = 'plasma-winter-494417-a8'
    SUBSCRIPTION_ID = 'analysis_sub'

    processor = BreadcrumbProcessor(
        project_id=PROJECT_ID, 
        subscription_id=SUBSCRIPTION_ID, 
        batch_limit=1000
    )
    
    processor.start_listening()