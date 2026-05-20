# publisher.py
import pandas as pd
import requests, json, time
from google.cloud import pubsub_v1
from datetime import date
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import re


class StopEventPublisher:
    def __init__(self, project_id, topic_id, api_url):
        self.project_id = project_id
        self.topic_id = topic_id
        self.api_url = api_url

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        self.event_count = 0 # running count of breadcrumbs iterated thru
        self.published_count = 0
        self.failed_ids = [] # holds vehicle IDs which failed to fetch
        self.start_time = None
        self.final_time = None
    
    def _record_success(self, future):
        """Keep track of how many message succesfully sent"""
        self.published_count += 1
    
    def process_vehicles(self, vehicle_list):
        self.start_time = time.time()
        print(f'Start time: {self.start_time}')
        print(f'Fetching data for {len(vehicle_list)} vehicles...')
        print('(This may take several minutes)\n')
        
        for index, vehicle_id in enumerate(vehicle_list):
            url = f'{self.api_url}?vehicle_num={vehicle_id}'

            try:
                if (index + 1) % 10 == 0:
                    print(f'Fetching vehicle #{vehicle_id} ... ({index + 1}/{len(vehicle_list)})')
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, 'html.parser')
                h2_tags = soup.find_all('h2')

                for h2 in h2_tags:
                    attempted_match = re.search(r'TRIP (-?\d+)', h2.text)
                    if attempted_match:
                        table = h2.find_next_sibling('table')
                        if not table: 
                            continue
                        
                        rows = table.find_all('tr')
                        for row in rows:
                            cells = row.find_all('td')
                            if len(cells) == 24:
                                record = {
                                    "vehicle_number": cells[0].get_text().strip(),
                                    "leave_time": cells[1].get_text().strip(),
                                    "train": cells[2].get_text().strip(),
                                    "route_number": cells[3].get_text().strip(),
                                    "direction": cells[4].get_text().strip(),
                                    "service_key": cells[5].get_text().strip(),
                                    "trip_number": int(attempted_match.group(1)),
                                    "trip_number_2": cells[6].get_text().strip(),
                                    "stop_time": cells[7].get_text().strip(),
                                    "arrive_time": cells[8].get_text().strip(),
                                    "dwell": cells[9].get_text().strip(),
                                    "location_id": cells[10].get_text().strip(),
                                    "door": cells[11].get_text().strip(),
                                    "lift": cells[12].get_text().strip(),
                                    "ons": cells[13].get_text().strip(),
                                    "offs": cells[14].get_text().strip(),
                                    "estimated_load": cells[15].get_text().strip(),
                                    "maximum_speed": cells[16].get_text().strip(),
                                    "train_mileage": cells[17].get_text().strip(),
                                    "pattern_distance": cells[18].get_text().strip(),
                                    "location_distance": cells[19].get_text().strip(),
                                    "GPS_latitude": cells[20].get_text().strip(),
                                    "GPS_longitude": cells[21].get_text().strip(),
                                    "data_source": cells[22].get_text().strip(),
                                    "schedule_status": cells[23].get_text().strip()
                                }
                                payload = json.dumps(record).encode('utf-8')
                                future = self.publisher.publish(self.topic_path, payload)
                                future.add_done_callback(self._record_success)
                                self.event_count += 1
            
            except requests.exceptions.HTTPError as e:
                self.failed_ids.append(vehicle_id)
                print(f'HTTP Error for {vehicle_id}: {e}')
            except requests.exceptions.ConnectionError:
                self.failed_ids.append(vehicle_id)
                print(f'Internet connection issues for {vehicle_id}!')
            except requests.exceptions.Timeout:
                self.failed_ids.append(vehicle_id)
                print(f'Request timeout for {vehicle_id}.')
            except Exception as e:
                self.failed_ids.append(vehicle_id)
                print(f'Unexpected error for {vehicle_id}: {e}')

    def _wait_for_publishing(self):
        """Pause execution if target count not reached"""
        print(f"\nWaiting for {self.event_count} background messages to finish publishing...")
        last_printed_count = 0
        
        while self.published_count < self.event_count:
            if self.published_count - last_printed_count >= 1000:
                print(f'Published: {self.published_count} / {self.event_count}')
                last_printed_count = self.published_count
            time.sleep(0.3)
        
        print(f'Published: {self.published_count} / {self.event_count} (Finished)')
    
    def _send_sentinel(self):
        """Publishes the final Sentinel message to signal the end of the stream."""
        sentinel_data = {
            "sentinel": True, 
            "total_count": self.event_count
        }

        sentinel_payload = json.dumps(sentinel_data).encode('utf-8')
        sentinel_future = self.publisher.publish(self.topic_path, sentinel_payload)
        sentinel_future.result()  
        
        print(f'\nSentinel message has been sent: {sentinel_data}')
    
    def _print_summary(self, total_vehicles):
        """Calculates and outputs the final metrics for the run."""
        self.final_time = time.time()
        wall_time = self.final_time - self.start_time
        received_vehicles = total_vehicles - len(self.failed_ids)
        throughput_rate = float(self.event_count) / wall_time if wall_time > 0 else 0

        print(f'\n\nSummary Statistics:\n---------------------------------------')
        print(f'Records published: {self.event_count}')
        print(f'Vehicles with available records: {received_vehicles}')
        print(f'Begin Timestamp: {time.ctime(self.start_time)}')
        print(f'End Timestamp: {time.ctime(self.final_time)}')
        print(f'Elapsed time: {wall_time:.3f}s')
        print(f'Throughput: {throughput_rate:.3f} records/sec')
       
    
    def run(self, vehicle_list):
        """The main orchestrator that executes the pipeline."""
        self.process_vehicles(vehicle_list)
        
        if self.event_count > 0:
            self._wait_for_publishing()
            self._send_sentinel()
            self._print_summary(len(vehicle_list))
        else:
            print("\nNo Stop Events were fetched or published. Pipeline aborted.")
    

if __name__=="__main__":
    PROJECT_ID = 'plasma-winter-494417-a8'
    TOPIC_ID   = 'se_topic'
    API_URL    = 'https://busdata.cs.pdx.edu/api/getStopEvents'

    try:
        dazzle_df = pd.read_csv('/home/davvan/VehicleGroupsIDs-NEW.csv')
        dazzle_list = dazzle_df['Dazzle'].dropna().astype(int).to_list()
        
        dazzle_list = dazzle_list[:3] 
        
    except FileNotFoundError:
        print("Error: Could not find the vehicle CSV file.")
        exit(1)

    pipeline = StopEventPublisher(
        project_id=PROJECT_ID, 
        topic_id=TOPIC_ID, 
        api_url=API_URL
    )
    
    pipeline.run(dazzle_list)

