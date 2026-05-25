import json
import time
import os
import argparse
import logging
import threading
import pandas as pd
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine
from pyproj import Transformer
import logging

MAX_LATITUDE = 90
MAX_LONGITUDE = 180
PDX_LAT_MIN, PDX_LAT_MAX = 45.0, 46.0
PDX_LON_MIN, PDX_LON_MAX = -123.5, -122.0
MAX_VEHICLE_ID = 9999999

_transformer = Transformer.from_crs("EPSG:2913", "EPSG:4326", always_xy=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def validate_batch(batch_df) -> pd.DataFrame:
    """
    Validates a batch of  payloadrecords.
    Creates columns for invalid records with IS_VALID = False
    """

    # Create Columns to flag invalid bc
    batch_df['IS_VALID'] = True
    batch_df['VIOLATION_REASON'] = ""

    # arrive_time is a datetime object already so convert it back to seconds past midnight after service
    seconds_past_midnight = (batch_df['arrive_time'] - pd.to_datetime(batch_df['service_date'])).dt.total_seconds()
    rules = [
        # Assertion 1 [Limit] - Arrive time must not be null and in range 0 to 108000
        (batch_df['arrive_time'].isna() | (seconds_past_midnight < 0) | (seconds_past_midnight > 108000),
         'A9: arrive_time is negative or exceeds 30 hours (108000s) past midnight | '),

        # Assertion 2 [Intra-record] - Leave time is non null and leave time >= arrive time
        (batch_df['leave_time'].isna() | (batch_df['leave_time'] < batch_df['arrive_time']),
         'A2: leave_time is null or it occurs before arrive time |'),

        # ASSERTION 3 [Existence] Primary Key must not be null (vehicle_number, trip_number, arrive_time )
        (batch_df['vehicle_number'].isna() | (batch_df['trip_number'].isna()) | (batch_df['arrive_time'].isna()),
         'A1: Primary Key (vehicle, trip, arrive_time) contains null | '),

        # Assertion 4- [Limit] Speed must be in the range [0,80]
        (batch_df['maximum_speed'].isna() | (batch_df['maximum_speed'] < 0) | (batch_df['maximum_speed'] > 80),
        'A4: Speed is not in range 0-80mph'),

        #Assertion 5 - [Limit] Passenger on board should not be negative
        (batch_df['ons']<0 | (batch_df['offs'] < 0),
         'A5: Negative passenger getting on or getting off'),

        #---ASSERTION 6---[LIMIT]  GPS_LATITUDE must be non-null and in [-90, 90]-----
        (batch_df['gps_latitude'].isna() | (batch_df['gps_latitude'] > MAX_LATITUDE) | (batch_df['gps_latitude'] < -MAX_LATITUDE),
        'A1: gps_latitude is null or out of range [-90, 90] | '),

        #---ASSERTION 7---[LIMIT]  GPS_LONGITUDE must be non-null and in [-180, 180]-----
        (batch_df['gps_longitude'].isna() | (batch_df['gps_longitude'] > MAX_LONGITUDE) | (batch_df['gps_longitude'] < -MAX_LONGITUDE),
         'A2: gps_longitude is null or out of range [-180, 180] | '),

        #Assertion 8 - [LIMIT] GPS coordinates within PDX range
        ((batch_df['gps_latitude'] < PDX_LAT_MIN) | (batch_df['gps_latitude'] > PDX_LAT_MAX) | 
         (batch_df['gps_longitude'] < PDX_LON_MIN) | (batch_df['gps_longitude'] > PDX_LON_MAX),
         'A7: GPS coordinates are outside of Portland area bounds | '),

    ]

    for mask, reason in rules:
        batch_df.loc[mask, 'IS_VALID'] = False
        batch_df.loc[mask, 'VIOLATION_REASON'] += reason

    violations_df =batch_df[batch_df['IS_VALID'] == False]

    # Return the data frame with violation flags
    return batch_df
    


def format_time(raw_timestamp):
    if raw_timestamp is None:
        return "Not time"
    formated_time = datetime.fromtimestamp(raw_timestamp, tz=ZoneInfo("America/Los_Angeles"))
    return formated_time.strftime('%Y-%m-%d %H:%M:%S')

def calc_breadcrumb_timestamp(bus_date, bus_time):
        the_date = datetime.strptime(bus_date, '%Y-%m-%d').date()
        time_elapsed = timedelta(seconds=int(bus_time))
        return datetime.combine(the_date, datetime.min.time())+ time_elapsed


def write_invalid_records(invalid_records, run_date=None):
    """
    Write invalid  payloadrecords to json file with date
    """
    if run_date is None:
        run_date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d')
    
    base_dir = "/home/davvan/bus_light_year/analysis/invalid_se"
    filename = f"{base_dir}/invalid_se_{run_date}.json"

    os.makedirs(base_dir, exist_ok=True)
    invalid_records.to_json(filename, orient='records', lines=True, mode='a', date_format='iso')

# ----- Bread Crumbs Processor Class
class StopEventProcessor:
    def __init__(self, project_id, subscription_id, batch_limit=1000):
        #Configuration
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.batch_limit = batch_limit

        # Data Structures
        self.message_batch = []
        self.batch_lock = threading.Lock()
        self.db_engine = create_engine('postgresql://bus:lightyear@localhost:5432/breadcrumbs')

        # Start fresh
        self.reset_datastructure()

    def reset_datastructure(self):
        self.breadcrumb_count = 0
        self.expected_count = None
        self.unique_vehicles = set()
        self.unique_trips = set()
        self.earliest_bc = None
        self.latest_bc = None
        self.wall_clock_time = None
        self.sentinel_time = None
        self.curret_service_date = "Unknown"

        self.total_valid_records = 0
        self.total_invalid_records = 0

    def process_pandas_batch(self):
        """"
        Process current pandas batch of invalid and valid
        """
        if len(self.message_batch) == 0:
            return
        
        df = pd.DataFrame(self.message_batch)
        df = self._transform_data(df)
        df_validations = validate_batch(df)

        good_df = df_validations[df_validations['IS_VALID'] == True].copy()
        bad_df = df_validations[df_validations['IS_VALID'] == False].copy()

        if not bad_df.empty:
            write_invalid_records(bad_df)
            self.total_invalid_records += len(bad_df)
        
        if not good_df.empty:
            good_df = good_df.drop(columns=['IS_VALID', 'VIOLATION_REASON'])
            try:
                good_df.to_sql('stopevent', con=self.db_engine, if_exists='append', index=False)
                self.total_valid_records += len(good_df)
            except Exception as e:
                logging.error(f"UNEXPECTED DB ERROR: {type(e).__name__} - {e}")
                logging.error(f"Problematic Batch Sample:\n{good_df.head(3)}\n")
                


        self.message_batch.clear()
    
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(r'^\s*$', None, regex=True)
        
        base_date = pd.to_datetime(df['service_date'])

        for time_col in ['leave_time', 'stop_time', 'arrive_time']:
            seconds = pd.to_numeric(df[time_col], errors='coerce')
            df[time_col] = base_date + pd.to_timedelta(seconds, unit='s')
        
        numeric_columns = ['maximum_speed', 'ons', 'offs', 'estimated_load', 'dwell']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df['GPS_latitude'] = pd.to_numeric(df['GPS_latitude'], errors='coerce')
        df['GPS_longitude'] = pd.to_numeric(df['GPS_longitude'], errors='coerce')

        new_long, new_lat = _transformer.transform(
            df['GPS_latitude'].values,
            df['GPS_longitude'].values
        )

        df['GPS_latitude'] = new_lat
        df['GPS_longitude'] = new_long


        df.columns = df.columns.str.lower()

        return df
    
    def state_plane_to_latlon(x_coordinate, y_coordinate):
        """
        Convert Oregon State Plane Coordinate System coordinates (EPSG:2913)
        to WGS 84 GPS latitude and longitude.

        Parameters
        ----------
        x_coordinate : float
            Easting in international feet (SPCS83 Oregon North Zone).
        y_coordinate : float
            Northing in international feet (SPCS83 Oregon North Zone).

        Returns
        -------
        tuple[float, float]
            (GPS_latitude, GPS_longitude) in decimal degrees (WGS 84).
        """
        lon, lat = _transformer.transform(x_coordinate, y_coordinate)
        return lat, lon


    def callback(self, message):
        message.ack()
        payload = json.loads(message.data.decode('utf-8'))

        # analysis happens here
        # Check for Sentinel (VEHICLE_ID = 0)
        if payload.get('sentinel') == True:
          self.expected_count = payload.get('total_count')
          self.sentinel_time = time.time()
        else:
            if self.wall_clock_time is None: #start timer when first  payloadrecieved
                self.wall_clock_time = time.time()
                print(f"First  payloadreceived at {format_time(self.wall_clock_time)}")

            try:
                current_bc_time = calc_breadcrumb_timestamp(payload['service_date'],payload['arrive_time'])

                # Keep track of latest bc and earliest bc
                if self.latest_bc is None or current_bc_time > self.latest_bc:
                    self.latest_bc = current_bc_time
                if self.earliest_bc is None or current_bc_time < self.earliest_bc:
                    self.earliest_bc = current_bc_time
            except (ValueError, TypeError):
                pass

            with self.batch_lock:
                self.breadcrumb_count += 1
                self.unique_vehicles.add(payload['vehicle_number'])
                self.unique_trips.add(payload['trip_number'])


                self.message_batch.append( payload)
                if len(self.message_batch) >= self.batch_limit:
                    self.process_pandas_batch()

        # --- SUMMARY STATISTICS ---
        with self.batch_lock:
            # After recieving Sentinel, check for expected count and actual count 
            if self.expected_count is not None and self.breadcrumb_count == self.expected_count:
                # flush messages to db again incase it is missed
                if len(self.message_batch) > 0:
                    self.process_pandas_batch()
                elapsed_time = self.sentinel_time - self.wall_clock_time
                throughput = self.breadcrumb_count / elapsed_time

                print("\nSentinel Received")
                print("Summary Statistics:")
                print(f"First message received: {format_time(self.wall_clock_time)}")
                print(f"Unique Vehicle IDs: {len(self.unique_vehicles)}")
                print(f"Earliest recorded bus: {self.earliest_bc}")
                print(f"Latest recorded bus: {self.latest_bc}")
                print(f"Unique Trip IDs: {len(self.unique_trips)}")
                print(f"Total Breadcrumbs Received: {self.breadcrumb_count}")
                print(f"Sentinel Received Time: {format_time(self.sentinel_time)}")
                print(f"Elapsed Time: {elapsed_time:.3f}s")
                print(f"Throughput: {throughput:.3f} msg/s")
                print(f"Total valid Breadcrumbs stored in db: {self.total_valid_records}")
                print(f"Total invalid written to json: {self.total_invalid_records}")

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
    SUBSCRIPTION_ID = 'se_analysis_sub'

    processor = StopEventProcessor(
        project_id=PROJECT_ID, 
        subscription_id=SUBSCRIPTION_ID, 
        batch_limit=1000
    )

    processor.start_listening()