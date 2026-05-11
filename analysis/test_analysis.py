import unittest
import pandas as pd

from analysis import validate_batch, BreadcrumbProcessor


class BaseAnalysisTest(unittest.TestCase):
    def setUp(self):
        self.perfect_bus = {
            'EVENT_NO_TRIP': 1001,
            'EVENT_NO_STOP': 2001,
            'OPD_DATE': '07JAN2023:00:00:00',
            'VEHICLE_ID': 3011,
            'METERS': 15000,
            'ACT_TIME': 50000,
            'GPS_LONGITUDE': -122.5,
            'GPS_LATITUDE': 45.5,
            'GPS_SATELLITES': 12.0,
            'GPS_HDOP': 5.0
        }
    
    def _build_bus(self, **kwargs):
        """
        Use kwargs to modify fields from perfect bus
        """
        bus = self.perfect_bus.copy()
        bus.update(kwargs)
        return bus
    
  
class TestValidationRules(BaseAnalysisTest):
    
    def test_assertion_1_latitude_out_of_bounds(self):
        """ A1: test latitude > 90 is invalid """
        bad_bus = self._build_bus(GPS_LATITUDE=95.0)
        test_df = pd.DataFrame([bad_bus])
        
        result_df = validate_batch(test_df)
        
        is_valid = result_df.loc[0, 'IS_VALID']
        reason = result_df.loc[0, 'VIOLATION_REASON']
        
        self.assertFalse(is_valid, "Bouncer failed to catch bad latitude!")
        self.assertIn("A1:", reason, "Bouncer caught it, but gave the wrong reason code!")

class TestDataTransformation(BaseAnalysisTest):

    def setUp(self):
        super().setUp()
        self.processor = BreadcrumbProcessor(project_id="dummy", subscription_id="dummy")
    
    def test_transform_data_creates_timestamp_renames_columns(self):
        test_df = pd.DataFrame([self.perfect_bus])
        
        # Call function
        result_df = self.processor._transform_data(test_df)
        
        # Get columns for assertion
        remaining_columns = result_df.columns.tolist()
        
        # Check columns dropped
        self.assertNotIn('OPD_DATE', remaining_columns, "OPD_DATE was not dropped!")
        self.assertNotIn('EVENT_NO_STOP', remaining_columns, "EVENT_NO_STOP was not dropped!")
        self.assertIn('timestamp', remaining_columns, "timestamp column was not created!")
        
        # Check new columns
        expected_timestamp = pd.Timestamp('2023-01-07 13:53:20')
        actual_timestamp = result_df.loc[0, 'timestamp']
        self.assertEqual(actual_timestamp, expected_timestamp, "Timestamp math is incorrect!")

        # Check rename
        self.assertIn('trip_id', remaining_columns, "EVENT_NO_TRIP was not renamed to trip_id!")
        self.assertIn('vehicle_id', remaining_columns, "VEHICLE_ID was not renamed to vehicle_id!")
        self.assertIn('latitude', remaining_columns, "GPS_LATITUDE was not renamed to latitude!")
        self.assertIn('longitude', remaining_columns, "GPS_LONGITUDE was not renamed to longitude!")
        self.assertNotIn('EVENT_NO_TRIP', remaining_columns, "Old EVENT_NO_TRIP column still exists!")
    
    def test_transform_data_calculates_speed_correctly(self):
        bus_1 = self._build_bus(EVENT_NO_TRIP=1001, METERS=15000, ACT_TIME=50000)
        bus_2 = self._build_bus(EVENT_NO_TRIP=1001, METERS=15150, ACT_TIME=50010)
        bus_3 = self._build_bus(EVENT_NO_TRIP=9999, METERS=20000, ACT_TIME=50020)

        test_df = pd.DataFrame([bus_1, bus_2, bus_3])
        result_df = self.processor._transform_data(test_df)

        self.assertIn('speed', result_df.columns, "speed column was not created")

        self.assertEqual(result_df.loc[0, 'speed'], 0.0, "First breadcrumb not 0.0")
        self.assertEqual(result_df.loc[1, 'speed'], 15.0, "speed math not correct")
        self.assertEqual(result_df.loc[2, 'speed'], 0.0, "Speed did not reset for a new EVENT_NO_TRIP")



if __name__ == '__main__':
    unittest.main()