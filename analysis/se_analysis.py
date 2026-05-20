import json
import time
import os
import argparse
import logging
import threading
import pandas as pd
from google.cloud import pubsub_v1
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine

MAX_LATITUDE = 90
MAX_LONGITUDE = 180
PDX_LAT_MIN, PDX_LAT_MAX = 45.0, 46.0
PDX_LON_MIN, PDX_LON_MAX = -123.5, -122.0
MAX_VEHICLE_ID = 9999999


def validate_batch(batch_df) -> pd.DataFrame:
    """
    Validates a batch of  payloadrecords.
    Creates columns for invalid records with IS_VALID = False
    """

    # Create Columns to flag invalid bc
    batch_df['IS_VALID'] = True
    batch_df['VIOLATION_REASON'] = ""

    rules = [
        #---ASSERTION 1---[LIMIT] Primary Key must not be null (vehicle_number, trip_number, arrive_time )
        (batch_df['vehicle_number'].isna() | batch_df['trip_number'].isna() | batch_df['arrive_time'].isna(),
         'A1: Primary Key (vehicle, trip, arrive_time) contains null | '),
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


def write_invalid_records(invalid_records, run_date=None):
    """
    Write invalid  payloadrecords to json file with date
    """
    if run_date is None:
        run_date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y-%m-%d')

    filename = f"/home/davvan/bus_light_year/analysis/invalid_se/invalid_se_{run_date}.json"

    os.makedirs("/home/davvan/invalid_se", exist_ok=True)
    invalid_records.to_json(filename, orient='records', lines=True, mode='a')

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
        
        good_df = good_df.drop_duplicates(subset=['vehicle_number', 'trip_number', 'arrive_time'])
        if not good_df.empty:
            good_df = good_df.drop(columns=['IS_VALID', 'VIOLATION_REASON'])
            good_df.to_sql('stopevent', con=self.db_engine, if_exists='append', index=False)
            self.total_valid_records += len(good_df)

        self.message_batch.clear()
    
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(r'^\s*$', None, regex=True)
        
        df['GPS_latitude'] = pd.to_numeric(df['GPS_latitude'], errors='coerce')
        df['GPS_longitude'] = pd.to_numeric(df['GPS_longitude'], errors='coerce')

        base_date = pd.to_datetime(df['service_date'])

        for time_col in ['leave_time', 'stop_time', 'arrive_time']:
            if time_col in df.columns:
                seconds = pd.to_numeric(df[time_col], errors='coerce')
                df[time_col] = base_date + pd.to_timedelta(seconds, unit='s')

        df.columns = df.columns.str.lower()

        return df


    def callback(self, message):
        message.ack()
        payload = json.loads(message.data.decode('utf-8'))

        # analysis happens here
        # Check for Sentinel (VEHICLE_ID = 0)
        if payload.get('sentinel') == True:
          self.expected_count = payload.get('total_count')
          self.sentinel_time = time.time()

          # final batch, use thread locks for safety
          with self.batch_lock:
            self.process_pandas_batch()
        else:
            if self.wall_clock_time is None: #start timer when first  payloadrecieved
                self.wall_clock_time = time.time()
                print(f"First  payloadreceived at {format_time(self.wall_clock_time)}")

            self.breadcrumb_count += 1
            self.unique_vehicles.add( payload['vehicle_number'])
            self.unique_trips.add( payload['trip_number'])

            with self.batch_lock:
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
                # print(f"Earliest Breadcrumb from OPD and ACT: {self.earliest_bc}")
                # print(f"Latest Breadcrumb from OPD and ACT: {self.latest_bc}")
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