"""Tests for the utils module."""

import unittest
import pandas as pd
from pyICU.utils import (
    is_timestamp,
    convert_timestamp_columns,
    check_or_convert_timestamp_columns,
    intervention_after_event,
    continous_intervention_after_event,
    calculate_time_overlap,
    dosage_during_event,
    time_based_violation,
    count_events_during_observation,
    count_events_per_day_during_observation
)
from datetime import datetime


class TestTimestampFunctions(unittest.TestCase):
    def test_is_timestamp(self):
        # Test valid timestamps
        self.assertTrue(is_timestamp("2023-10-13"))
        self.assertTrue(is_timestamp("2023-10-13 12:34:56"))
        self.assertTrue(is_timestamp("2023-10-13 12:34:56 PM"))
        self.assertTrue(is_timestamp("2023-10-13T12:34:56"))

    def test_convert_timestamp_columns(self):
        # Create a sample DataFrame
        data = {'Timestamp1': ['2023-10-13', '2023-10-14', '2023-10-15'],
                'Timestamp2': ['2023-10-13 12:34:56', '2023-10-14 10:00:00', '2023-10-15 15:45:30'],
                'NotTimestamp': ['hello', 'world', 'how are you?']}
        df = pd.DataFrame(data)

        # Convert timestamp columns
        convert_timestamp_columns(df)

        # Check if Timestamp1 and Timestamp2 were converted to datetime objects
        self.assertIn(str(df['Timestamp1'].dtype), [
                      'datetime64[ns]', 'datetime64[ns, UTC]', '<M8[ns]'])
        self.assertIn(str(df['Timestamp2'].dtype), [
                      'datetime64[ns]', 'datetime64[ns, UTC]', '<M8[ns]'])
        self.assertEqual(df['NotTimestamp'].dtype, 'O')

    def test_check_or_convert_timestamp_columns(self):
        # Create a sample DataFrame
        data = {'Timestamp1': ['2023-10-13', '2023-10-14', '2023-10-15'],
                'Timestamp2': ['2023-10-13 12:34:56', '2023-10-14 10:00:00', '2023-10-15 15:45:30'],
                'NotTimestamp': ['hello', 'world', 'how are you?']}
        df = pd.DataFrame(data)

        # Define a mapper
        mapper = [(df, ['Timestamp1', 'Timestamp2'])]

        # Check and convert timestamp columns
        check_or_convert_timestamp_columns(mapper, convert_datetimes=True)

        # Check if Timestamp1 and Timestamp2 were converted to datetime objects
        self.assertIn(str(df['Timestamp1'].dtype), [
                      'datetime64[ns]', 'datetime64[ns, UTC]', '<M8[ns]'])
        self.assertIn(str(df['Timestamp2'].dtype), [
                      'datetime64[ns]', 'datetime64[ns, UTC]', '<M8[ns]'])
        self.assertEqual(df['NotTimestamp'].dtype, 'O')


class TestEventFunctions(unittest.TestCase):

    def test_intervention_after_event(self):
        # Create a sample event DataFrame
        event_data = {'subject_id': [1, 2, 3, 4, 5],
                      'event_time': ['2023-10-13 08:00:00',  # intervention within time window
                                     '2023-10-14 10:30:00',  # intervention within time window
                                     '2023-10-15 14:45:00',  # intervention but before event
                                     '2023-10-15 18:30:00',  # intervention but after time window
                                     '2023-10-16 09:30:00'  # no intervention
                                     ]}
        event_df = pd.DataFrame(event_data)

        # Create a sample intervention DataFrame
        intervention_data = {'subject_id': [1, 2, 3, 4],
                             'charttime': ['2023-10-13 08:30:00', '2023-10-14 11:00:00', '2023-10-15 14:30:00', '2023-10-15 20:00:00']}
        intervention_df = pd.DataFrame(intervention_data)

        # Call the intervention_after_event function
        result = intervention_after_event(
            event_df, intervention_df, event_time_indicator='event_time', observation_time=1, convert_datetimes=True)

        # Assert that the result contains sample IDs for which an intervention occurred within the time window
        expected_result = [1, 2]
        self.assertEqual(result, expected_result)

    def test_continous_intervention_after_event(self):
        # Create a sample event DatFrame
        event_data = {'subject_id': [1, 2, 3, 4, 5, 6],
                      'event_time': [
                          '2023-10-13 8:00:00',  # started after event, ended before threshold
                          '2023-10-13 8:00:00',  # started after event, ended after threshold
                          '2023-10-13 8:00:00',  # started before event, ended before threshold
                          '2023-10-13 8:00:00',  # started before event, ended before event
                          '2023-10-13 8:00:00',  # started after threshold, ended after threshold
                          '2023-10-13 8:00:00'  # no data
        ]}
        event_df = pd.DataFrame(event_data)
        # Create a sample intervention dataframe
        intervention_data = {
            'subject_id': [1, 2, 3, 4, 5],
            'starttime': [
                '2023-10-13 8:15:00',
                '2023-10-13 8:15:00',
                '2023-10-13 7:00:00',
                '2023-10-13 7:00:00',
                '2023-10-13 9:15:00',
            ],
            'endtime': [
                '2023-10-13 8:30:00',
                '2023-10-13 9:15:00',
                '2023-10-13 8:30:00',
                '2023-10-13 7:30:00',
                '2023-10-13 9:30:00'
            ]
        }
        intervention_df = pd.DataFrame(intervention_data)

        # Call continous_intervention_after_event
        result = continous_intervention_after_event(
            event_df, intervention_df, event_time_indicator='event_time', observation_time=1, convert_datetimes=True)
        expected_result = [1, 2, 3]
        self.assertEqual(result, expected_result)

    def test_calculate_time_overlap_with_overlap(self):
        # Sample timestamps and observation window duration in hours
        event_start_timestamp = datetime(2023, 10, 15, 12, 0)
        intervention_start_timestamp = datetime(2023, 10, 15, 11, 30)
        intervention_end_timestamp = datetime(2023, 10, 15, 13, 0)
        observation_window_hours = 2  # Replace with your actual observation window duration

        overlap = calculate_time_overlap(event_start_timestamp,
                                         observation_window_hours,
                                         intervention_start_timestamp,
                                         intervention_end_timestamp)
        expected_overlap = 1
        self.assertEqual(overlap, expected_overlap)

    def test_calculate_time_overlap_without_overlap(self):
        # Sample timestamps and observation window duration in hours
        event_start_timestamp = datetime(2023, 10, 15, 12, 0)
        intervention_start_timestamp = datetime(2023, 10, 15, 15, 30)
        intervention_end_timestamp = datetime(2023, 10, 15, 16, 0)
        observation_window_hours = 2

        overlap = calculate_time_overlap(event_start_timestamp,
                                         observation_window_hours,
                                         intervention_start_timestamp,
                                         intervention_end_timestamp)

        expected_overlap = 0
        self.assertEqual(overlap, expected_overlap)

    def test_dosage_for_event(self):
        # Create a sample event DataFrame
        event_data = {'subject_id': [1, 1, 2, 2, 3, 3, 4, 5],
                      'event_time': [
                          '2023-10-13 8:00:00',  # 1 dosage only within observation window 1
                          # 2 dosage begins before observation window, ends within observation window 1
                          '2023-10-13 8:00:00',
                          # 3 dosage begins before observation window, ends after observation window 2
                          '2023-10-13 8:00:00',
                          '2023-10-13 8:00:00',  # 4 dosage begins and ends before observation window 2
                          '2023-10-13 8:00:00',  # 5 dosage begins and ends after observation window 3
                          '2023-10-13 8:00:00',  # 6 dosage begins after event, ends before observation window 3
                          '2023-10-13 8:00:00',  # 7 dosage begins after event, ends after observation window 4
                          '2023-10-13 8:00:00'  # 8 no data 5
        ]}

        intervention_data = {
            'subject_id': [1, 1, 2, 2, 3, 3, 4],
            'starttime': [
                '2023-10-13 8:15:00',
                '2023-10-13 7:00:00',
                '2023-10-13 7:00:00',
                '2023-10-13 7:00:00',
                '2023-10-13 9:15:00',
                '2023-10-13 8:15:00',
                '2023-10-13 8:15:00'
            ],
            'endtime': [
                '2023-10-13 8:30:00',
                '2023-10-13 8:30:00',
                '2023-10-13 9:30:00',
                '2023-10-13 7:30:00',
                '2023-10-13 9:30:00',
                '2023-10-13 8:30:00',
                '2023-10-13 9:30:00'
            ],
            'amount': [
                150, 150, 150, 150, 150, 150, 150
            ]
        }
        intervention_df = pd.DataFrame(intervention_data)
        event_df = pd.DataFrame(event_data)
        result = dosage_during_event(
            event_df, intervention_df, event_time_indicator='event_time', observation_time=1, convert_datetimes=True)
        expected_result = {
            'subject_id': [1, 2, 3, 4, 5],
            'total_amount': [
                (150.0 + 50.0),  # 1 + 2
                (60.0 + 0),  # 3 + 4
                (0 + 150.0),  # 5 + 6
                (90.0),  # 7
                0  # 8
            ]
        }
        expected_result_df = pd.DataFrame(expected_result)
        # convert to float
        expected_result['total_amount'] = expected_result['total_amount']

        pd.testing.assert_frame_equal(result, expected_result_df)


class TestTimeBasedViolation(unittest.TestCase):

    def test_high_violation(self):
        # Create a test value_dict for high violation
        value_dict = [
            {"stay_id_value": 1, "valuenum": 180,
                "charttime": datetime(2023, 10, 15, 7, 0, 0)},
            {"stay_id_value": 1, "valuenum": 190,
                "charttime": datetime(2023, 10, 15, 10, 0, 0)},
            {"stay_id_value": 2, "valuenum": 140,
                "charttime": datetime(2023, 10, 15, 10, 0, 0)}
        ]

        threshold = 150
        delta_value = 3
        violation_ids = time_based_violation(
            threshold, delta_value, value_dict, violation="high")

        # Check if stay_id_value 1 is in the violation_ids list
        self.assertIn(1, violation_ids)
        # Check if stay_id_value 2 is not in the violation_ids list
        self.assertNotIn(2, violation_ids)

    def test_low_violation(self):
        # Create a test value_dict for low violation
        value_dict = [
            {"stay_id_value": 1, "valuenum": 50,
                "charttime": datetime(2023, 10, 15, 10, 0, 0)},
            {"stay_id_value": 1, "valuenum": 50,
                "charttime": datetime(2023, 10, 15, 11, 0, 0)},
            {"stay_id_value": 2, "valuenum": 85,
                "charttime": datetime(2023, 10, 15, 10, 0, 0)}
        ]

        threshold = 65
        delta_value = 1
        violation_ids = time_based_violation(
            threshold, delta_value, value_dict, violation="low")

        # Check if stay_id_value 1 is in the violation_ids list
        self.assertIn(1, violation_ids)
        # Check if stay_id_value 2 is not in the violation_ids list
        self.assertNotIn(2, violation_ids)

    def test_no_violation(self):
        # Create a test value_dict with no violation
        value_dict = [
            {"stay_id_value": 1, "valuenum": 90,
                "charttime": datetime(2023, 10, 15, 10, 0, 0)},
            {"stay_id_value": 1, "valuenum": 80,
                "charttime": datetime(2023, 10, 15, 11, 0, 0)},
            {"stay_id_value": 2, "valuenum": 100,
                "charttime": datetime(2023, 10, 15, 10, 0, 0)}
        ]

        threshold = 85
        delta_value = 1
        violation_ids = time_based_violation(
            threshold, delta_value, value_dict, violation="high")

        # Check if the violation_ids list is empty
        self.assertEqual(len(violation_ids), 0)


class TestCountEventsDuringObservation(unittest.TestCase):
    def setUp(self):
        # Create sample DataFrames for testing
        self.event_data = pd.DataFrame({
            'subject_id': [1, 2, 3, 4, 5],
            'event_time': [datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0)]
        })

        self.intervention_data = pd.DataFrame({
            'subject_id': [1, 2, 2, 3, 4, 4],
            'charttime': [
                datetime(2023, 1, 1, 8, 15),
                datetime(2023, 1, 1, 9, 0),
                datetime(2023, 1, 1, 15, 0),
                datetime(2023, 1, 1, 9, 30),
                datetime(2023, 1, 1, 10, 0),
                datetime(2023, 1, 1, 10, 15)
            ],
            'amount': [5, 10, 20, 15, 30, 5]
        })

    def test_count_events_during_observation(self):
        result = count_events_during_observation(
            self.event_data, self.intervention_data, 'event_time', 3, 'subject_id', 'charttime')

        # Expected result for the given data and parameters
        expected_result = pd.DataFrame({
            'subject_id': [1, 2, 3, 4],
            'total_interventions': [1, 1, 1, 2]
        })

        pd.testing.assert_frame_equal(result, expected_result)


class TestCountEventsPerDayDuringObservation(unittest.TestCase):
    def setUp(self):
        # Create sample DataFrames for testing
        self.event_data = pd.DataFrame({
            'subject_id': [1, 2, 3, 4, 5],
            'event_time': [datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0),
                           datetime(2023, 1, 1, 8, 0)]
        })
        self.intervention_data: pd.DataFrame = pd.DataFrame({
            'subject_id': [1, 1, 2, 2, 2, 3, 4, 4],
            'charttime': [
                datetime(2023, 1, 1, 7, 15),  # 1
                datetime(2023, 1, 1, 9, 0),  # 1
                datetime(2023, 1, 1, 15, 0),  # 2
                datetime(2023, 1, 2, 9, 30),  # 2
                datetime(2023, 1, 2, 10, 0),  # 2
                datetime(2023, 1, 1, 10, 15),  # 3
                datetime(2023, 1, 1, 10, 15),  # 4
                datetime(2023, 1, 10, 10, 15)  # 4
            ],
            'amount': [5, 10, 20, 15, 30, 5, 10, 10]
        })

    def test_count_events_per_day_during_observation(self):
        result = count_events_per_day_during_observation(
            self.event_data, self.intervention_data, 'event_time', 72, 'subject_id', 'charttime')

        # Expected result for the given data and parameters
        expected_result = pd.DataFrame({
            'subject_id': [1, 2, 2, 3, 4],
            'charttime_day': [
                datetime(2023, 1, 1),
                datetime(2023, 1, 1),
                datetime(2023, 1, 2),
                datetime(2023, 1, 1),
                datetime(2023, 1, 1)
            ],
            'count_per_day': [1, 1, 2, 1, 1]
        })

        pd.testing.assert_frame_equal(result, expected_result)


if __name__ == '__main__':
    unittest.main()
